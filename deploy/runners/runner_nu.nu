#!/usr/bin/env nu
# The nu ToolRunner — a FIXED deployment artifact installed with a node
# (gravity ADR-0001 §4; the runner is infrastructure, not a graph citizen).
#
# It is the body of a node mount: the mount's source is this file's spawn
# spec and never changes; everything that varies per operation arrives as
# call arguments. The delivered operation is a nu script in `args.code`,
# its payload in `args.args` — a NAMED MAP, never expanded argv: a script is a
# body delivered as data, not a command line, and argv would strip the types a
# caller's JSON Schema check just established.
#
# Carrier contract (effector `exec`, one process per call): one JSON
# document on stdin — `{"handler": <entry>, "args": {...}}`, then EOF —
# and one JSON document on stdout as the result.
#
# Failure is a VALUE (ADR-0012): a non-zero child exit returns
# `{"error": ...}` instead of dying, so the error reaches the LLM's next
# inference round as data rather than as a masked carrier failure.

let req = (open --raw /dev/stdin | from json)
let handler = ($req.handler? | default "execute")
let payload = ($req.args | default {})

if not ($payload.code? | default "" | is-not-empty) {
    { error: "runner_nu: args.code missing (the operation's script)" } | to json
} else {
    let code = ($payload.code | into string)
    # The delivered script reads its own arguments from this env var and
    # prints one JSON document as its result.
    let child = (with-env {
        TOOLRUNNER_ENTRY: $handler
        TOOLRUNNER_ARGS: (($payload.args? | default {}) | to json)
    } {
        # `--no-config-file`: the runner's behavior is the deployment's, not
        # the machine's nu config.
        ^nu --no-config-file -c $code | complete
    })

    if $child.exit_code != 0 {
        { error: $"runner_nu: delivered code failed: ($child.stderr | str trim)" } | to json
    } else if ($child.stdout | str trim | is-empty) {
        { error: "runner_nu: delivered code printed nothing (a result JSON document is required)" } | to json
    } else {
        # Parse-and-reprint so the output is a JSON document whatever the
        # delivered script produced (nu renders records as tables).
        $child.stdout | from json | to json
    }
}