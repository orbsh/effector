#!/usr/bin/env nu
# The nu runner — a FIXED deployment artifact installed with a node
# (gravity ADR-0001 §4; the runner is infrastructure, not a graph citizen).
#
# It is the body of a node mount: the mount's source is this file's spawn spec and never changes;
# everything that varies per operation arrives as call arguments. The delivered operation is a nu
# script in `args.code`; its payload is `args.args`, a NAMED MAP and never expanded argv — a script
# is a body delivered as data, not a command line, and argv would strip the types a caller's JSON
# Schema check just established.
#
# Carrier contract (effector `exec`, one process per call): one JSON document on stdin —
# `{"handler": <entry>, "args": {"code": <script>, "args": {...}}}`, then EOF — and one JSON document
# on stdout as the result.
#
# WHAT THE DELIVERED SCRIPT RECEIVES: the request document minus `code`, read from ITS OWN stdin —
#   {"handler": <entry>, "args": <the operation's arguments>}
# No environment variable carries the payload: it is already a document, and re-spelling two of its
# fields was the CGI residue that argv-shaped invocation left behind.
#
# Failure is a VALUE (ADR-0012): a non-zero child exit returns `{"error": ...}` instead of dying, so
# the error reaches the LLM's next inference round as data rather than as a masked carrier failure.

let raw = (open --raw /dev/stdin)
let req = ($raw | from json)
let payload = ($req.args? | default {})

if not ($payload.code? | default "" | is-not-empty) {
    { error: "runner_nu: args.code missing (the operation's script)" } | to json
} else {
    let code = ($payload.code | into string)
    let forward = ({
        handler: ($req.handler? | default "execute")
        args: ($payload.args? | default {})
    } | to json)
    let dir = ($env.TMPDIR? | default "/tmp")
    let tmp = ($"($dir)/runner_nu_((random uuid)).nu")
    $code | save --force $tmp
    # The delivered script is a FILE run by a child `nu`: its scope is never this runner's, and
    # `--no-config-file` makes the runner's behavior the deployment's, not the machine's nu config.
    let child = (try {
        $forward | ^nu --no-config-file $tmp | complete
    } catch {|e|
        { exit_code: 1, stdout: "", stderr: ($e.msg? | default "spawn failed") }
    })
    rm --force $tmp

    if $child.exit_code != 0 {
        { error: $"runner_nu: delivered code failed: ($child.stderr | str trim)" } | to json
    } else if ($child.stdout | str trim | is-empty) {
        { error: "runner_nu: delivered code printed nothing (a result JSON document is required)" } | to json
    } else {
        # Parse-and-reprint so the output is a JSON document whatever the delivered script produced
        # (nu renders records as tables).
        try { $child.stdout | from json | to json } catch {|e|
            { error: $"runner_nu: stdout is not one JSON document: ($e.msg?)" } | to json
        }
    }
}