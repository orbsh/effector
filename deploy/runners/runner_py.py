#!/usr/bin/env python3
"""The python runner — a FIXED deployment artifact installed with a node
(gravity ADR-0001 §4; the runner is infrastructure, not a graph citizen).

It is the body of a node mount: the mount's source is this file's spawn spec and never changes;
everything that varies per operation arrives as call arguments. The delivered operation is a python
script in `args.code`; its payload is `args.args`, a NAMED MAP and never expanded argv — a script is
a body delivered as data, not a command line, and argv would strip the types a caller's JSON Schema
check just established.

ONE FILE, TWO CARRIERS. Which shape this process serves is decided by the document that arrives
first, because the two carriers announce themselves differently and neither is a script's business:

  exec        {"handler": <entry>, "args": {"code": <script>, "args": {...}}}      then EOF
              One process per call — the bare cgi shape. The request is read, `code` is taken out
              (it is the PROGRAM, not data), and the remainder — {"handler": …, "args": {…}} — is
              forwarded VERBATIM on the delivered script's stdin. The script prints one JSON
              document; that document is the result. No environment variable carries the payload:
              it is already a document, and re-spelling two of its fields was the CGI residue that
              argv-shaped invocation left behind.

  induction   {"type": "initialize"} …  then {"id": N, "kind": …, "event": …, "args": {...}}
              A resident guest on the line protocol below. The delivered script runs in THIS
              process, so it keeps STATE across calls and can reach the host (ctx_invoke/ctx_emit).
              Its stdin IS the wire — hence ARGS in scope instead of a request document, and hence
              `sys.stdin` is emptied before delivered code runs.

Frame protocol (induction only):

  parent -> guest   {"type": "initialize", ...}                -> {"type":"hello",...}
                    {"id":N,"kind":...,"event":...,"args":{}}   -> {"id":N,"result":...}
                    {"host_reply": {"id":M,"ok"|"error": ...}}  (answers a ctx call)
  guest  -> parent  {"host": {"type": "<arm>", "args": {...},
                              "id": M, "call": N}}              (a ctx call out)

Failures are VALUES (ADR-0012): a delivered script that raises, prints nothing, or prints a non-JSON
document comes back as {"error": ...} — the runner's own text reaches the next inference round
instead of being masked by the carrier.

The wire fds are PINNED at import (`WIRE_IN` / `WIRE_OUT`) and every protocol frame goes through them
under one lock (a line is the unit on the wire: two writers would interleave bytes, not requests).
"""

import io
import itertools
import json
import os
import subprocess
import sys
import tempfile
import threading

# Pinned before anything can redirect them (see the module docstring): frame lookups must never go
# through `sys.stdout`/`sys.stdin` at frame time.
WIRE_IN = sys.stdin
WIRE_OUT = sys.stdout

SCHEMA_EVENT = "interface_schema"
ENCODING_ENV = "INDUCTION_ENCODING"

# Guest-scope state: the residency the resident form buys. Scripts may read and mutate it (they run
# in the namespace below); nothing here is persisted, and two requests running at once SEE each
# other's writes — the author's business.
STATE = {}

SCHEMA = {
    "receives": {"execute": {}},
    "wildcard_receives": [],
    "hooks": [],
    "auth": None,
    # Consulted by the host at registration (aura ADR-0046): ONE resident process serving every call
    # at once — no queue, no second process. A serial runner would say `"concurrency": 1` here and
    # get the queue its body needs.
    "lifecycle": {"concurrency": "unbounded"},
}

# The write side, one lock: a frame is one line, and two lines must not merge.
_WRITE_LOCK = threading.Lock()
# Ctx calls waiting for the host, by the ctx id they were written with.
_PENDING = {}
_PENDING_LOCK = threading.Lock()
# Which request the CURRENT thread is serving (stamped onto its ctx frames).
_CURRENT = threading.local()
_NEXT_CTX = itertools.count(1)
_STOP = threading.Event()


def write_frame(value):
    """One protocol line on the wire — the only writer of this channel."""
    line = json.dumps(value) + "\n"
    with _WRITE_LOCK:
        WIRE_OUT.write(line)
        WIRE_OUT.flush()


def read_frame():
    """One protocol line from stdin, or None at EOF (the parent closed it)."""
    line = WIRE_IN.readline()
    if not line:
        return None
    line = line.strip()
    if not line:
        return {}
    try:
        return json.loads(line)
    except ValueError as e:
        print(f"[runner_py] undecodable line: {e}", file=sys.stderr)
        return {}


def _fail_result(reason):
    """A failure as a VALUE (ADR-0012) — the same shape from either carrier."""
    return {"error": reason}


def _result_document(text):
    """The delivered script's one JSON document, or an error value naming what came instead."""
    out = text.strip()
    if not out:
        return _fail_result(
            "runner_py: delivered code printed nothing (a result JSON document is required)"
        )
    try:
        return json.loads(out)
    except ValueError as e:
        return _fail_result(f"runner_py: stdout is not one JSON document: {e}")


# ---------------------------------------------------------------------------------------------
# The one-shot shape (exec carrier): forward the request, carry the result back
# ---------------------------------------------------------------------------------------------


def serve_one_shot(request_line):
    """One request, one child interpreter, one answer — the cgi shape.

    The runner reads the carrier's document, removes `code` (the program), and forwards the REST
    verbatim on the child's stdin. The delivered script therefore sees the document shape the
    carrier already speaks — {"handler": …, "args": {…}} — with no environment variable, no argv and
    no second dialect to learn.
    """
    rest = WIRE_IN.read()
    text = request_line + rest
    try:
        req = json.loads(text)
    except ValueError as e:
        print(json.dumps(_fail_result(f"runner_py: the request is not one JSON document: {e}")))
        return 0
    payload = req.get("args") or {}
    code = payload.get("code")
    if not code:
        print(json.dumps(_fail_result("runner_py: args.code missing (the operation's script)")))
        return 0
    forward = json.dumps(
        {"handler": req.get("handler") or "execute", "args": payload.get("args") or {}}
    )
    # The delivered script runs in a CHILD interpreter: the script's own scope is never the
    # runner's, and the process boundary is preserved (the same reason the nu form shells out
    # instead of sourcing in place).
    f = tempfile.NamedTemporaryFile("w", suffix=".py", delete=False)
    try:
        f.write(code)
        f.close()
        proc = subprocess.run(
            [sys.executable, f.name], input=forward, capture_output=True, text=True
        )
    finally:
        os.unlink(f.name)
    if proc.returncode != 0:
        print(
            json.dumps(
                _fail_result(f"runner_py: delivered code failed: {proc.stderr.strip()}")
            )
        )
        return 0
    print(json.dumps(_result_document(proc.stdout)))
    return 0


# ---------------------------------------------------------------------------------------------
# The resident shape (induction carrier): frames in, frames out, state kept
# ---------------------------------------------------------------------------------------------


def _fail_pending(reason):
    """The host is gone: every ctx call waiting on it fails instead of hanging."""
    with _PENDING_LOCK:
        waiting = list(_PENDING.items())
        _PENDING.clear()
    for _, slot in waiting:
        slot["error"] = reason
        slot["event"].set()


def ctx_call(arm, args):
    """One host round trip, paired by id: the frame out, ITS reply back in.

    Several calls of several requests may be in flight at once, so the answer is matched on the id
    this call wrote — never "the next frame", which is another request's answer the moment two calls
    are live.
    """
    ctx_id = next(_NEXT_CTX)
    slot = {"event": threading.Event(), "reply": None, "error": None}
    with _PENDING_LOCK:
        _PENDING[ctx_id] = slot
    host = {"type": arm, "args": args, "id": ctx_id}
    call = getattr(_CURRENT, "call", None)
    if call is not None:
        # The request this ctx call belongs to (induction ADR-0002): without it the host cannot
        # tell whose bridge should answer.
        host["call"] = call
    try:
        write_frame({"host": host})
        slot["event"].wait()
    finally:
        with _PENDING_LOCK:
            _PENDING.pop(ctx_id, None)
    if slot["error"] is not None:
        raise RuntimeError(slot["error"])
    reply = slot["reply"]
    if reply is None:
        raise RuntimeError("runner_py: host closed during a ctx call")
    if "ok" in reply:
        return reply["ok"]
    raise RuntimeError(reply.get("error") or "ctx call failed")


def ctx_invoke(target_type, key, handler, args):
    """Call a sibling booth through the host realm (the ctx bridge)."""
    return ctx_call(
        "invoke",
        {"type": target_type, "key": key, "handler": handler, "args": args},
    )


def ctx_emit(event, data):
    """Publish into the realm's MQ plane — fire-and-forget by contract: the host answers NOTHING,
    so waiting here would cost a round trip that has no reply to come back."""
    host = {"type": "emit", "args": {"event": event, "data": data}}
    call = getattr(_CURRENT, "call", None)
    if call is not None:
        host["call"] = call
    write_frame({"host": host})


def _printer(buf):
    """The delivered script's `print`: ITS buffer, never the shared stdout."""

    def emit(*parts, **kwargs):
        kwargs.pop("file", None)
        print(*parts, file=buf, **kwargs)

    return emit


def run_delivered(payload):
    """Execute the operation's script in this process and return its result.

    The script's result is the one JSON document it prints, captured in a buffer of its own: this
    channel belongs to the protocol, and with several requests running at once a process-wide stdout
    swap would hand one script's output to another.
    """
    code = payload.get("code")
    if not code:
        return _fail_result("runner_py: args.code missing (the operation's script)")
    buf = io.StringIO()
    scope = {
        "ARGS": payload.get("args") or {},
        "STATE": STATE,
        "ctx_invoke": ctx_invoke,
        "ctx_emit": ctx_emit,
        "json": json,
        "print": _printer(buf),
        "__name__": "__delivered__",
    }
    try:
        exec(compile(code, "<delivered>", "exec"), scope)
    except BaseException as e:  # the script's failure is a value, not a crash
        return _fail_result(f"runner_py: delivered code failed: {e}")
    return _result_document(buf.getvalue())


def serve_frame(frame):
    """One request, on its own thread, answered with its own id."""
    _CURRENT.call = frame["id"]
    try:
        event = frame["event"]
        kind = frame.get("kind") or "call"
        if event == SCHEMA_EVENT and kind == "call":
            result = SCHEMA
        elif kind in ("call", "iterate_start"):
            # One entry, any name: the caller's entry name is a convention the carrier does not
            # check, and a runner with one job should not turn a naming drift into an error
            # (ADR-0001's `execute` fallback).
            result = run_delivered(frame.get("args") or {})
        else:
            result = {
                "error": f"runner_py: {kind} is not served by this runner "
                "(delivered scripts answer in one round — no stream state)"
            }
    finally:
        _CURRENT.call = None
    write_frame({"id": frame["id"], "result": result})


def serve_frames(first_frame):
    """The resident loop: replies to their ctx calls, requests to their own threads."""
    if first_frame.get("type") == "initialize":
        write_frame({"type": "hello", "name": "runner_py", "schema": SCHEMA})
    elif "id" in first_frame and "event" in first_frame:
        threading.Thread(target=serve_frame, args=(first_frame,), daemon=True).start()
    while not _STOP.is_set():
        frame = read_frame()
        if frame is None:
            _STOP.set()
            _fail_pending("runner_py: host closed the channel")
            return 0
        if "host_reply" in frame:
            reply = frame["host_reply"]
            ctx_id = reply.get("id")
            with _PENDING_LOCK:
                slot = _PENDING.get(ctx_id) if ctx_id is not None else None
            if slot is None:
                print(f"[runner_py] reply for unknown ctx id {ctx_id}", file=sys.stderr)
                continue
            slot["reply"] = reply
            slot["event"].set()
            continue
        if frame.get("type") == "initialize":
            write_frame({"type": "hello", "name": "runner_py", "schema": SCHEMA})
            continue
        if "id" not in frame or "event" not in frame:
            print(f"[runner_py] undecodable frame: {frame}", file=sys.stderr)
            continue
        threading.Thread(target=serve_frame, args=(frame,), daemon=True).start()
    return 0


def declared_codec_is_json():
    """The codec is a property of the FRAMED wire (INDUCTION_ENCODING), so that is where it is
    checked: the one-shot shape has no frames to encode — the carrier writes one JSON document and
    reads one back regardless of what the mount declared — and refusing there would break a mount
    over a field that shape never consults."""
    declared = os.environ.get(ENCODING_ENV, "json").lower()
    if declared == "json":
        return True
    print(
        f"[runner_py] declared codec '{declared}' is not json — this runner ships the "
        f"JSON-lines codec only",
        file=sys.stderr,
    )
    return False


def main():
    first = WIRE_IN.readline()
    if not first:
        # A parent that says nothing at all: nothing to serve, and nobody to report it to.
        return 0
    try:
        doc = json.loads(first)
    except ValueError:
        return serve_one_shot(first)
    if not isinstance(doc, dict):
        return serve_one_shot(first)
    # The carriers announce themselves: a frame carries `id`/`event` (or the handshake's `type`),
    # a one-shot request carries `handler`/`args`. Nothing else distinguishes them, and nothing
    # else needs to.
    if doc.get("type") == "initialize" or "id" in doc or "event" in doc:
        if not declared_codec_is_json():
            return 1
        # The wire is the protocol, so delivered code must not read it: stdin is emptied for it
        # (WIRE_IN is already pinned and stays the reader).
        sys.stdin = io.StringIO("")
        return serve_frames(doc)
    return serve_one_shot(first)


if __name__ == "__main__":
    sys.exit(main())