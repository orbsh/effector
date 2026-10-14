#!/usr/bin/env python3
"""The python ToolRunner — a FIXED deployment artifact installed with a node
(gravity ADR-0001 §4; the runner is infrastructure, not a graph citizen).

It is the body of a node mount: the mount's source is this file's spawn spec
and never changes; everything that varies per operation arrives as call
arguments. The delivered operation is a python script in `args.code`, its
payload in `args.args`.

Carrier contract (effector `exec`, one process per call): one JSON document on
stdin — {"handler": <entry>, "args": {...}}, then EOF — and one JSON document
on stdout as the result.

Failure is a VALUE (ADR-0012): a non-zero child exit returns {"error": ...}
instead of dying, so the error reaches the next inference round as data rather
than as a masked carrier failure.
"""

import json
import os
import subprocess
import sys
import tempfile

req = json.load(sys.stdin)
handler = req.get("handler") or "execute"
payload = req.get("args") or {}
code = payload.get("code")

if not code:
    print(json.dumps({"error": "runner_py: args.code missing (the operation's script)"}))
    sys.exit(0)

env = dict(
    os.environ,
    TOOLRUNNER_ENTRY=handler,
    TOOLRUNNER_ARGS=json.dumps(payload.get("args") or {}),
)

# The delivered script runs in a CHILD interpreter: the script's own scope is
# never the runner's, and the process boundary is preserved (the same reason
# the nu form shells out instead of sourcing in place).
f = tempfile.NamedTemporaryFile("w", suffix=".py", delete=False)
try:
    f.write(code)
    f.close()
    proc = subprocess.run(
        [sys.executable, f.name], capture_output=True, text=True, env=env
    )
finally:
    os.unlink(f.name)

if proc.returncode != 0:
    print(
        json.dumps(
            {"error": f"runner_py: delivered code failed: {proc.stderr.strip()}"}
        )
    )
elif not proc.stdout.strip():
    print(
        json.dumps(
            {
                "error": "runner_py: delivered code printed nothing "
                "(a result JSON document is required)"
            }
        )
    )
else:
    try:
        value = json.loads(proc.stdout)
        print(json.dumps(value))
    except ValueError as e:
        print(
            json.dumps({"error": f"runner_py: stdout is not one JSON document: {e}"})
        )