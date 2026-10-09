//! Effector runtime: executes the task contract inside a container-isolated node.
//!
//! Carriers live in `carrier` (steel / python / wasmtime embedded,
//! feature-gated). Executable documentation for the carrier contract is in
//! `tests/carriers.rs`.

pub mod carrier;

/// Remote wrapper (Phase 3): outbound WS, registration, task loop.
pub mod remote;
pub mod sandbox;
