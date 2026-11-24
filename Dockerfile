# syntax=docker/dockerfile:1.7
#
# The NODE image — effector ADR-0003 §10, and the thing PLAN milestone 7's "container backend" was
# waiting for. A container node is not a per-call sandbox: it IS a container, the launcher starts it
# with `docker run --rm --init -i`, and its filesystem is the boundary the class claims.
#
#   docker build -t effector-node:dev .
#
# THE CONTEXT IS THIS REPOSITORY, and the command above is the whole story: it works on a fresh
# checkout and in CI, where no sibling directory exists. The one out-of-tree dependency — four
# `../induction` crates, taken as path dependencies — is CLONED INSIDE THE BUILD at a pinned
# revision (`INDUCTION_REV`). Nothing outside this repository is read from the host.
#
# The pin is a revision, not a branch: the lockfile records the induction crates' VERSIONS but not
# their commits, so `--locked` and this ARG are two halves of one statement — when the induction
# crates change version, bump `INDUCTION_REV` in the same commit as the lockfile.
#
# WHAT THE IMAGE OWES, each item with code waiting for it:
#
#   /usr/local/bin/effector        the launcher's argv spells a bare `effector`
#   /etc/effector/effector.json    `effector` with no argument reads ./effector.json, and the
#                                  launcher passes no config path — so this is the WORKDIR
#   /runners/*                     a mount's `source` is a FIXED deployment artifact and the plane
#                                  names its path (PRISM_LANGUAGES' `runner`); the binary spawns it
#                                  and never contains it
#   nu, python3                    what those runners execute
#   the tool set                   what a delivered script reaches for (its own layer, below)
#   /work/resources                EFFECTOR_RESOURCES_ROOT, writable — a class that bounds the
#                                  filesystem must let a fetch land where the script can read it
#                                  (ADR-0003 §9)
#
# What it deliberately does NOT carry: a token (the launcher writes it to stdin), an alias (the
# launcher passes `--alias`, because a call names its executor and one image serves many nodes), or a
# credential of any kind.

ARG RUST_IMAGE=rust:1.100-bookworm
ARG NODE_IMAGE=debian:bookworm-slim
ARG INDUCTION_REPO=https://github.com/orbsh/induction
ARG INDUCTION_REV=cd923ebec266c5f62bc5e6b8825b9b5e1e42de1b

# ---------------------------------------------------------------------------------------------
# Build: the workspace, with its out-of-tree dependency cloned beside it at the depth the manifests
# expect (/src/effector/crates/protocol/../../../induction = /src/induction).
# ---------------------------------------------------------------------------------------------
FROM ${RUST_IMAGE} AS build
ARG INDUCTION_REPO
ARG INDUCTION_REV
WORKDIR /src

COPY Cargo.toml Cargo.lock /src/effector/
COPY crates /src/effector/crates

# Shallow where the server allows it, then the exact revision: a clone that follows a branch would
# make two builds of the same commit differ.
RUN git clone --quiet "${INDUCTION_REPO}" /src/induction \
 && git -C /src/induction checkout --quiet "${INDUCTION_REV}" \
 && git -C /src/induction log --oneline -1

WORKDIR /src/effector
# `--locked`: the committed lock is the pin. A build that silently re-resolves is a build whose
# reproducibility nobody can state.
RUN cargo build --release --locked -p effector-runtime --bin effector

# ---------------------------------------------------------------------------------------------
# Node: the smallest image that can honour the contract above
# ---------------------------------------------------------------------------------------------
FROM ${NODE_IMAGE} AS node
ARG NU_VERSION=0.117.0
ARG TARGETARCH=amd64

# THE TOOL LAYER — one layer, deliberately, and it is the `debian` stack the xy images install
# (hub.yaml's base/dev/ssh/net/diag/file/archive/network-tools/json/db groups), trimmed to what a
# delivered script actually reaches for on a node. A script that has to `apt-get install jq` mid-call
# is a script that fails on a network-restricted node, and the failure arrives as the script's own
# error rather than as a missing capability.
#   rust-first CLIs where the distribution has one: ripgrep, fd (Debian ships `fdfind`)
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        ca-certificates curl jq git openssh-client \
        rsync tree patch \
        unzip zip xz-utils zstd tar \
        procps lsof socat strace \
        iproute2 net-tools iputils-ping \
        sqlite3 ripgrep fd-find; \
    ln -sf /usr/bin/fdfind /usr/local/bin/fd; \
    rm -rf /var/lib/apt/lists/*

# The interpreters the runners are written in. python3 is a distribution package; nushell is not —
# bookworm has none, and the VERSION is part of the contract (a runner written against 0.117's
# syntax must not land on 0.99).
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends python3; \
    rm -rf /var/lib/apt/lists/*

# The tarball is DOWNLOADED TO A FILE and RESUMED on failure, never piped into tar. Measured twice on
# this network: `curl … | tar -xz` died with `curl: (18) HTTP/2 stream 1 was not closed cleanly`, and a
# plain retry loop died the same way 71 MB into the body (`transfer closed with 71868986 bytes
# remaining`) — a 92 MB fetch from this host is not one-shot-reliable. `-C -` makes each attempt
# continue where the last stopped, so the loop converges instead of restarting the whole transfer;
# `--retry-all-errors` alone did not, because a plain retry begins at byte zero.
RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64) asset="x86_64-unknown-linux-musl" ;; \
      arm64) asset="aarch64-unknown-linux-musl" ;; \
      *) echo "effector-node: unsupported arch ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    url="https://github.com/nushell/nushell/releases/download/${NU_VERSION}/nu-${NU_VERSION}-${asset}.tar.gz"; \
    attempt=1; \
    until curl -fL --http1.1 --connect-timeout 20 -C - -o /tmp/nu.tar.gz "$url"; do \
      attempt=$((attempt + 1)); \
      if [ "$attempt" -gt 8 ]; then echo "effector-node: nu ${NU_VERSION} would not download" >&2; exit 1; fi; \
      echo "effector-node: retrying the nu tarball (attempt $attempt)"; \
      sleep 3; \
    done; \
    tar -xzf /tmp/nu.tar.gz -C /opt; \
    install -m 0755 "/opt/nu-${NU_VERSION}-${asset}/nu" /usr/local/bin/nu; \
    rm -rf "/opt/nu-${NU_VERSION}-${asset}" /tmp/nu.tar.gz

COPY --from=build /src/effector/target/release/effector /usr/local/bin/effector
COPY deploy/runners/ /runners/
# The spawn spec is the file's PATH and nothing else (`Body::Remote{source}` is the mount's runner,
# split into argv on the node) — so the kernel execs this file directly and it needs its shebang and
# its exec bit. The repository keeps them 0644; without this line every call dies at spawn with
# EACCES, long after the node registered successfully.
RUN chmod 0755 /runners/*
COPY deploy/effector.json /etc/effector/effector.json

# The resources root is a directory, not a volume: disposable is the point, and a named volume would
# make every container inherit the last one's leftovers.
RUN mkdir -p /work/resources && chmod 0777 /work/resources

ENV EFFECTOR_RESOURCES_ROOT=/work/resources

WORKDIR /etc/effector

# No ENTRYPOINT and no CMD: the launcher supplies the command
# (`effector --control-plane ws://… --alias … --token-stdin`) and the token on stdin. An image that
# also had a command would be a second, silently different way to start a node.