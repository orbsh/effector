# syntax=docker/dockerfile:1.7
#
# The NODE image — effector ADR-0003 §10, and the thing PLAN milestone 7's "container backend" was
# waiting for. A container node is not a per-call sandbox: it IS a container, the launcher starts it
# with `docker run --rm --init -i`, and its filesystem is the boundary the class claims.
#
#   docker build -f effector/Dockerfile -t effector-node:dev ~/world
#
# THE BUILD CONTEXT IS THE PARENT DIRECTORY. `effector/crates/{protocol,runtime}` take four
# `../induction` crates as PATH dependencies, so a context of this repository alone fails at manifest
# load (`no such file: ../../../induction/crates/induction-protocol/Cargo.toml`). `~/world/.dockerignore`
# whitelists exactly `effector/` and `induction/` so nothing else in that directory is uploaded.
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
#   /work/resources                EFFECTOR_RESOURCES_ROOT, writable — a class that bounds the
#                                  filesystem must let a fetch land where the script can read it
#                                  (ADR-0003 §9)
#
# What it deliberately does NOT carry: a token (the launcher writes it to stdin), an alias (the
# launcher passes `--alias`, because a call names its executor and one image serves many nodes), or a
# credential of any kind.

ARG RUST_IMAGE=rust:1.100-bookworm
ARG NODE_IMAGE=debian:bookworm-slim

# ---------------------------------------------------------------------------------------------
# Build: the workspace, with its sibling dependency tree beside it at the depth the manifests expect
# (/src/effector/crates/protocol/../../../induction = /src/induction).
# ---------------------------------------------------------------------------------------------
FROM ${RUST_IMAGE} AS build
WORKDIR /src

COPY effector/Cargo.toml effector/Cargo.lock /src/effector/
COPY effector/crates /src/effector/crates
COPY induction /src/induction

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

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl python3 \
 && rm -rf /var/lib/apt/lists/*

# nushell's release tarball, not a distribution package: bookworm has none, and the version is part
# of the contract (a runner written against 0.117's syntax must not land on 0.99).
# The tarball is DOWNLOADED TO A FILE, not piped into tar. Measured on the first build of this
# image: `curl … | tar -xz` died with `curl: (18) HTTP/2 stream 1 was not closed cleanly`, which tar
# turned into a truncated archive — a failure that reads like a broken release and is really a
# dropped 92 MB body. Fetch it with retries over HTTP/1.1, then unpack.
RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64) asset="x86_64-unknown-linux-musl" ;; \
      arm64) asset="aarch64-unknown-linux-musl" ;; \
      *) echo "effector-node: unsupported arch ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fL --http1.1 --retry 5 --retry-delay 2 --retry-all-errors \
         -o /tmp/nu.tar.gz \
         "https://github.com/nushell/nushell/releases/download/${NU_VERSION}/nu-${NU_VERSION}-${asset}.tar.gz"; \
    tar -xzf /tmp/nu.tar.gz -C /opt; \
    install -m 0755 "/opt/nu-${NU_VERSION}-${asset}/nu" /usr/local/bin/nu; \
    rm -rf "/opt/nu-${NU_VERSION}-${asset}" /tmp/nu.tar.gz

COPY --from=build /src/effector/target/release/effector /usr/local/bin/effector
COPY effector/deploy/runners/ /runners/
# The spawn spec is the file's PATH and nothing else (`Body::Remote{source}` is the mount's runner,
# split into argv on the node) — so the kernel execs this file directly and it needs its shebang and
# its exec bit. The repository keeps them 0644; without this line every call dies at spawn with
# EACCES, long after the node registered successfully.
RUN chmod 0755 /runners/*
COPY effector/deploy/effector.json /etc/effector/effector.json

# The resources root is a directory, not a volume: disposable is the point, and a named volume would
# make every container inherit the last one's leftovers.
RUN mkdir -p /work/resources && chmod 0777 /work/resources

ENV EFFECTOR_RESOURCES_ROOT=/work/resources

WORKDIR /etc/effector

# No ENTRYPOINT and no CMD: the launcher supplies the command
# (`effector --control-plane ws://… --alias … --token-stdin`) and the token on stdin. An image that
# also had a command would be a second, silently different way to start a node.