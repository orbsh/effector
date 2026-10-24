//! Effector runtime: executes the task contract inside a container-isolated node.
//!
//! Carriers live in `carrier` (steel / python embedded,
//! feature-gated). Executable documentation for the carrier contract is in
//! `tests/carriers.rs`.


// Unit tests may use the unwrap/panic vocabulary the production targets are
// denied (a panicking test is a failing test). `cfg(test)` is on only for this
// crate's own test build; `tests/` crates set the same line at their own root.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::unwrap_in_result, clippy::panic_in_result_fn))]
pub mod carrier;

/// Remote wrapper (Phase 3): outbound WS, registration, task loop.
pub mod remote;
pub mod sandbox;
