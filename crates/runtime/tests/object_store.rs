//! The object arm against a real store.
//!
//! Everything on this node's side is already exercised against a two-reply fake (unit tests in
//! `resource.rs`); what a fake cannot prove is the half that only exists on the wire — that a
//! SigV4 signature built from a one-string credential is accepted, that the ETag comes back, and
//! that what was uploaded is what comes down.
//!
//! It runs only when a store is pointed at, because a node has no business inventing one:
//!
//! ```text
//! S3_ENDPOINT=http://s3.d S3_BUCKET=effector-test \
//! S3_ACCESS_KEY=… S3_SECRET_KEY=… S3_REGION=us-east-1 \
//!   cargo test -p effector-runtime --test object_store -- --ignored --nocapture
//! ```
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use effector_runtime::resource::{self, Material, Mode, Spec, Transport};
use std::path::Path;

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

#[test]
#[ignore = "needs a real object store: set S3_ENDPOINT, S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY"]
fn an_object_goes_up_and_comes_back() {
    let (endpoint, bucket) = (
        env("S3_ENDPOINT").expect("S3_ENDPOINT"),
        env("S3_BUCKET").expect("S3_BUCKET"),
    );
    let (access_key, secret_key) = (
        env("S3_ACCESS_KEY").expect("S3_ACCESS_KEY"),
        env("S3_SECRET_KEY").expect("S3_SECRET_KEY"),
    );
    let region = env("S3_REGION").unwrap_or_else(|| "us-east-1".into());

    // The name is unique per run so two runs (or two machines) do not fight over one object. The
    // GRANT is the bucket; the script's name is what it puts inside it.
    let remote = format!("effector-test/{}.txt", std::process::id());
    let base = format!("{}/{}", endpoint.trim_end_matches('/'), bucket);
    let url = format!("{base}/{remote}");
    let spec = Spec {
        kind: "object".into(),
        transport: Transport::Object,
        at: None,
        mode: Mode::ReadWrite,
    };
    let material = Material {
        base: base.clone(),
        credential: Some(format!("{access_key}:{secret_key}:{region}")),
    };

    let dir = tempfile::tempdir().unwrap();
    let root: &Path = dir.path();

    // A write half: ensure (nothing there yet — the store answers 404 as an empty body), write,
    // publish. The reference is the store's own word for what it now holds.
    // Nothing is there yet, and this is a WRITABLE resource: the empty workspace is the correct
    // answer, and producing the first object is this half's job.
    let path = resource::ensure(&spec, &material, &remote, None, root, None)
        .expect("an empty workspace, not a failure");
    std::fs::write(&path, b"effector was here").unwrap();
    let reference =
        resource::sync(&spec, &material, &remote, None, root, None).expect("the object is published");
    assert!(!reference.is_empty(), "a publish answers with what it stored");
    eprintln!("published {url} -> {reference}");

    // A read half in a clean directory: what comes down is what went up.
    let other = tempfile::tempdir().unwrap();
    let read_spec = Spec {
        mode: Mode::ReadOnly,
        ..spec
    };
    let read_path =
        resource::ensure(&read_spec, &material, &remote, None, other.path(), None).expect("fetched");
    assert_eq!(
        std::fs::read(read_path).unwrap(),
        b"effector was here",
        "the bytes that come down are the bytes that went up"
    );

    // A credential that is not `access_key:secret_key[:region]` is refused before any request.
    let bad = Material {
        base: base.clone(),
        credential: Some("not-a-pair".into()),
    };
    let third = tempfile::tempdir().unwrap();
    let err = resource::ensure(&read_spec, &bad, &remote, None, third.path(), None)
        .unwrap_err()
        .to_string();
    assert!(err.contains("access_key:secret_key"), "{err}");

    // The object is LEFT IN PLACE on purpose: what a test uploaded is the thing somebody wants to
    // look at in the store's own console, and deleting evidence by default is worse than a stray
    // key. `S3_CLEAN=1` removes it, and the DELETE status is then the proof that it was really
    // there.
    eprintln!("left in the bucket: {url}");
    if std::env::var("S3_CLEAN").ok().as_deref() != Some("1") {
        return;
    }
    let status = std::process::Command::new("curl")
        .args(["-sS", "--request", "DELETE", "--output", "/dev/null"])
        .args(["--write-out", "%{http_code}"])
        .args(["--user", &format!("{access_key}:{secret_key}")])
        .args(["--aws-sigv4", &format!("aws:amz:{region}:s3")])
        .arg(&url)
        .output()
        .expect("curl runs");
    assert_eq!(
        String::from_utf8_lossy(&status.stdout).trim(),
        "204",
        "the object that was published is the object that was deleted"
    );
}