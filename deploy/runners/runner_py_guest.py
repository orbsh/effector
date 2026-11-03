#!/usr/bin/env python3
"""The python RESIDENT runner — the induction-guest form of a node mount
(gravity ADR-0001 §8), a FIXED deployment artifact.

Same contract as the one-shot runners, one process longer-lived: the mount's
source is this file's spawn spec and never changes, the operation's script
arrives in `args.code` with its payload in `args.args`, and the script must
print one JSON document as its result.

Why this form exists (verified, not assumed): nu cannot evaluate a runtime
string in-process — `source` demands a parse-time-constant path and nu 0.116 has
no `eval` — so a nu guest could only shell out, and a child never sees the
guest's scope. python can: `exec` runs the delivered script IN this process, in
a namespace this runner builds, so

  * the script sees the guest's ctx helpers (`ctx_invoke`, `ctx_emit`), which is
    the reason to be resident at all, and
  * the script sees `STATE` — plain guest-scope state that survives between
    calls (the one-shot form cannot keep anything; its contract is one process
    per call).

Concurrency (induction ADR-0002, aura ADR-0046). This guest serves SEVERAL
requests at once, in one process, so a booth may declare `unbounded`:

  * a reader THREAD owns stdin and demultiplexes it — `host_reply` frames wake the
    ctx call that asked for them by id, request frames are dispatched to their own
    thread, EOF ends the process;
  * every request runs on its own thread, and the thread remembers which request
    it is serving (`_CURRENT.call`);
  * every ctx frame carries `call` (the request it belongs to) beside its own
    `id`, so the host answers it from THAT call's bridge and never from a
    sibling's — the pairing the host needs to keep scopes apart;
  * the delivered script's `print` is its own capture buffer (a per-call
    function in its scope, not a global `sys.stdout` swap): two scripts running
    at once must not write into each other's result, and nothing a script does
    can reach the protocol channel.

Wire (induction protocol, JSON-lines codec — declared by the parent through
INDUCTION_ENCODING; this guest ships JSON only and refuses a CBOR declaration
loudly rather than half-speaking it):

  parent -> guest   {"type": "initialize", ...}                -> {"type":"hello",...}
                    {"id":N,"kind":...,"event":...,"args":{}}   -> {"id":N,"result":...}
                    {"host_reply": {"id":M,"ok"|"error": ...}}  (answers a ctx call)
  guest  -> parent  {"host": {"type": "<arm>", "args": {...},
                              "id": M, "call": N}}              (a ctx call out)

Failures are VALUES (ADR-0012): a delivered script that raises, prints nothing,
or prints a non-JSON document comes back as {"error": ...} — the runner's own
text reaches the next inference round instead of being masked by the carrier.

The wire fds are PINNED at import (`WIRE_IN` / `WIRE_OUT`) and every protocol
frame goes through them under one lock (a line is the unit on the wire: two
writers would interleave bytes, not requests).
"""

import io
import itertools
import json
import os
import sys
import threading

# Pinned before anything can redirect them (see the module docstring): frame
# lookups must never go through `sys.stdout`/`sys.stdin` at frame time.
WIRE_IN = sys.stdin
WIRE_OUT = sys.stdout

SCHEMA_EVENT = "interface_schema"
ENCODING_ENV = "INDUCTION_ENCODING"

# Guest-scope state: the residency this form buys. Scripts may read and mutate
# it (they run in the namespace below); nothing here is persisted, and two
# requests running at once SEE each other's writes — the author's business.
STATE = {}

SCHEMA = {
    "receives": {"execute": {}},
    "wildcard_receives": [],
    "hooks": [],
    "auth": None,
    # Consulted by the host at registration (aura ADR-0046): ONE resident process
    # serving every call at once — no queue, no second process. A serial runner
    # would say `"concurrency": 1` here and get the queue its body needs.
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
        print(f"[runner_py_guest] undecodable line: {e}", file=sys.stderr)
        return {}


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

    Several calls of several requests may be in flight at once, so the answer is
    matched on the id this call wrote — never "the next frame", which is another
    request's answer the moment two calls are live.
    """
    ctx_id = next(_NEXT_CTX)
    slot = {"event": threading.Event(), "reply": None, "error": None}
    with _PENDING_LOCK:
        _PENDING[ctx_id] = slot
    host = {"type": arm, "args": args, "id": ctx_id}
    call = getattr(_CURRENT, "call", None)
    if call is not None:
        # The request this ctx call belongs to (induction ADR-0002): without it
        # the host cannot tell whose bridge should answer.
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
        raise RuntimeError("runner_py_guest: host closed during a ctx call")
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
    """Publish into the realm's MQ plane — fire-and-forget by contract: the
    host answers NOTHING, so waiting here would cost a round trip that has no
    reply to come back."""
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

    The script's result is the one JSON document it prints, captured in a buffer
    of its own: this channel belongs to the protocol, and with several requests
    running at once a process-wide stdout swap would hand one script's output to
    another.
    """
    code = payload.get("code")
    if not code:
        return {"error": "runner_py_guest: args.code missing (the operation's script)"}
    payload_args = payload.get("args") or {}
    buf = io.StringIO()
    scope = {
        "ARGS": payload_args,
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
        return {"error": f"runner_py_guest: delivered code failed: {e}"}
    out = buf.getvalue().strip()
    if not out:
        return {
            "error": "runner_py_guest: delivered code printed nothing "
            "(a result JSON document is required)"
        }
    try:
        return json.loads(out)
    except ValueError as e:
        return {"error": f"runner_py_guest: stdout is not one JSON document: {e}"}


def serve(frame):
    """One request, on its own thread, answered with its own id."""
    _CURRENT.call = frame["id"]
    try:
        event = frame["event"]
        kind = frame.get("kind") or "call"
        if event == SCHEMA_EVENT and kind == "call":
            result = SCHEMA
        elif kind in ("call", "iterate_start"):
            # One entry, any name: the caller's entry name is a convention the
            # carrier does not check, and a runner with one job should not turn
            # a naming drift into an error (ADR-0001's `execute` fallback).
            result = run_delivered(frame.get("args") or {})
        else:
            result = {
                "error": f"runner_py_guest: {kind} is not served by this runner "
                "(delivered scripts answer in one round — no stream state)"
            }
    finally:
        _CURRENT.call = None
    write_frame({"id": frame["id"], "result": result})


def read_loop():
    """The reader: replies to their ctx calls, requests to their own threads."""
    while not _STOP.is_set():
        frame = read_frame()
        if frame is None:
            _STOP.set()
            _fail_pending("runner_py_guest: host closed the channel")
            return
        if "host_reply" in frame:
            reply = frame["host_reply"]
            ctx_id = reply.get("id")
            with _PENDING_LOCK:
                slot = _PENDING.get(ctx_id) if ctx_id is not None else None
            if slot is None:
                print(
                    f"[runner_py_guest] reply for unknown ctx id {ctx_id}",
                    file=sys.stderr,
                )
                continue
            slot["reply"] = reply
            slot["event"].set()
            continue
        if frame.get("type") == "initialize":
            write_frame({"type": "hello", "name": "runner_py_guest", "schema": SCHEMA})
            continue
        if "id" not in frame or "event" not in frame:
            print(f"[runner_py_guest] undecodable frame: {frame}", file=sys.stderr)
            continue
        threading.Thread(target=serve, args=(frame,), daemon=True).start()


def main():
    declared = os.environ.get(ENCODING_ENV, "json").lower()
    if declared != "json":
        print(
            f"[runner_py_guest] declared codec '{declared}' is not json — this "
            f"runner ships the JSON-lines codec only",
            file=sys.stderr,
        )
        return 1
    read_loop()
    return 0


if __name__ == "__main__":
    sys.exit(main())