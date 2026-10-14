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

Wire (induction protocol, JSON-lines codec — declared by the parent through
INDUCTION_ENCODING; this guest ships JSON only and refuses a CBOR declaration
loudly rather than half-speaking it):

  parent -> guest   {"type": "initialize", ...}                -> {"type":"hello",...}
                    {"id":N,"kind":...,"event":...,"args":{}}   -> {"id":N,"result":...}
                    {"host_reply": {"ok"|"error": ...}}         (answers a ctx call)
  guest  -> parent  {"host": {"type": "<arm>", "args": {...}}}  (a ctx call out)

Failures are VALUES (ADR-0012): a delivered script that raises, prints nothing,
or prints a non-JSON document comes back as {"error": ...} — the runner's own
text reaches the next inference round instead of being masked by the carrier.

The wire fds are PINNED at import (`WIRE_IN` / `WIRE_OUT`) and every protocol
frame goes through them: the delivered script runs with `sys.stdout` redirected
into a capture buffer (its printed document IS the result), so a ctx call made
from inside a script would otherwise write its frame into that buffer — the
parent would wait for a line that never arrives while the script waits for a
reply that was never sent, and the call would end at the deadline.
"""

import contextlib
import io
import json
import os
import sys

# Pinned before anything can redirect them (see the module docstring): frame
# lookups must never go through `sys.stdout`/`sys.stdin` at frame time.
WIRE_IN = sys.stdin
WIRE_OUT = sys.stdout

SCHEMA_EVENT = "interface_schema"
ENCODING_ENV = "INDUCTION_ENCODING"

# Guest-scope state: the residency this form buys. Scripts may read and mutate
# it (they run in the namespace below); nothing here is persisted.
STATE = {}

SCHEMA = {
    "receives": {"execute": {}},
    "wildcard_receives": [],
    "hooks": [],
    "auth": None,
}


def write_frame(value):
    """One protocol line on the captured wire — the ONLY writer of this channel."""
    WIRE_OUT.write(json.dumps(value) + "\n")
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


def ctx_call(arm, args):
    """One host round trip: the arm's frame out, the host_reply back in."""
    write_frame({"host": {"type": arm, "args": args}})
    while True:
        reply = read_frame()
        if reply is None:
            raise RuntimeError("runner_py_guest: host closed during a ctx call")
        if "host_reply" in reply:
            hr = reply["host_reply"]
            if "ok" in hr:
                return hr["ok"]
            raise RuntimeError(hr.get("error") or hr.get("err") or "ctx call failed")
        # A request frame arriving mid-call is a host bug; fail loudly rather
        # than silently treating it as the reply.
        raise RuntimeError(f"runner_py_guest: expected host_reply, got {reply}")


def ctx_invoke(target_type, key, handler, args):
    """Call a sibling booth through the host realm (the ctx bridge)."""
    return ctx_call(
        "invoke",
        {"type": target_type, "key": key, "handler": handler, "args": args},
    )


def ctx_emit(event, data):
    """Publish into the realm's MQ plane — fire-and-forget by contract: the
    host answers NOTHING, so reading here would block the loop."""
    write_frame({"host": {"type": "emit", "args": {"event": event, "data": data}}})


def run_delivered(payload):
    """Execute the operation's script in this process and return its result.

    The script's stdout is captured — this channel belongs to the protocol —
    and its result is the one JSON document it prints.
    """
    code = payload.get("code")
    if not code:
        return {"error": "runner_py_guest: args.code missing (the operation's script)"}
    payload_args = payload.get("args") or {}
    scope = {
        "ARGS": payload_args,
        "STATE": STATE,
        "ctx_invoke": ctx_invoke,
        "ctx_emit": ctx_emit,
        "json": json,
        "__name__": "__delivered__",
    }
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf):
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


def main():
    declared = os.environ.get(ENCODING_ENV, "json").lower()
    if declared != "json":
        print(
            f"[runner_py_guest] declared codec '{declared}' is not json — this "
            f"runner ships the JSON-lines codec only",
            file=sys.stderr,
        )
        return 1

    while True:
        frame = read_frame()
        if frame is None:
            return 0  # EOF: the parent closed the session
        if frame.get("type") == "initialize":
            write_frame({"type": "hello", "name": "runner_py_guest", "schema": SCHEMA})
            continue
        if "id" not in frame or "event" not in frame:
            print(f"[runner_py_guest] undecodable frame: {frame}", file=sys.stderr)
            continue
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
        write_frame({"id": frame["id"], "result": result})


if __name__ == "__main__":
    sys.exit(main())