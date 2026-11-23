#!/usr/bin/env nu
# The container launcher (effector ADR-0003 §10): the ONE privileged component.
#
# It mints nothing and decides nothing. It takes four facts — the alias, the image, the control
# plane's base URL, and the token — and starts
#
#     docker run --rm --init -i <image> effector --control-plane <base> --token-stdin
#
# then writes the token to that process's stdin and drops its own copy. The node becomes PID 1: the
# delivered scripts are its children, and when it exits the container exits. That is why the node
# holds no container-control surface at all, and why the only thing an operator reads on the way out
# is this process's exit code.
#
# What this component must NOT do: create, attach, exec or destroy anything; keep a container
# between tasks by itself; decide whether a node is acceptable (the plane does that at the dial).
# It is deliberately too small to be interesting.
#
#   nu deploy/node-container.nu --alias box-1 --image ghcr.io/orbsh/effector:2026-11-17 \
#       --plane ws://prism.d/effector --token (open --raw ~/.effector-token | str trim)
#
# Retention: the container's grace period IS the node's lease (`--idle-secs`), because the process's
# lifetime is the container's. A task's later calls arrive on the same connection and cancel the
# timer; when it expires the node exits and docker reaps the container.
#
# `--network` is a launch fact, not a node field. The default is `bridge` and it has to be: the
# node's first act is a dial to the plane, and `none` leaves the container a loopback with no route —
# a container started that way dies at startup, before anything is delivered. `none` stays available
# for a container that needs no dial (it reaches the plane through a socket mounted in, ADR-0001's
# surviving promise); a guest's own egress goes through the node's proxies, never an allowlist the
# node keeps.

def main [
    --alias: string          # what the plane mints the token for
    --image: string          # what ships `effector` and its config
    --plane: string          # control-plane base URL (the mount is derived from it)
    --token: string          # the one credential; rides the node's stdin, never its argv
    --network: string = "bridge"  # docker's network for this container
    --idle-secs: int = 15        # the grace period; the node exits when it expires
    --root: string = "/work/resources"  # where resources land INSIDE the container
    --memory: string = "2g"
    --cpus: string = "2"
] {
    if ($token | is-empty) {
        error make { msg: "node-container: --token is empty — mint one with POST /admin/nodes" }
    }

    # `--init` is tini: the node is PID 1 and must reap its children, and the parts of that job it
    # does not need to know about are the parts tini covers.
    #
    # `--rm`: leaving is not a state to clean up later. The container is disposable by construction,
    # and a container that lingers after its node exited is exactly the orphan this shape removes.
    let argv = [
        run --rm --init -i
        --network $network
        --memory $memory
        --cpus $cpus
        --env $"EFFECTOR_RESOURCES_ROOT=($root)"
        $image
        effector --control-plane $plane --alias $alias --token-stdin --idle-secs $idle_secs --exit-on-disconnect
    ]

    # The token goes in on stdin: argv is world-readable (`ps`), and the node's environment is
    # inherited by the scripts it spawns, so stdin is the only channel that belongs to the process
    # alone (ADR-0003 §11).
    print -n $token | ^docker ...$argv
}