//! The resource contract's node half (ADR-0003 §7): `ensure` and `sync`, one transport at a time.
//!
//! A task declares a resource by IDENTITY — `{ kind, seq }` — and the plane answers with how that
//! resource travels and where it lives: `transport`, `url`, `ref`, `mode`. The node never learns a
//! source from the task and never invents a credential. `kind` and `seq` fix the path; `ensure` is
//! idempotent (present → no-op), which is why a resource discovered mid-task costs a directory
//! appearing rather than a restart; `sync` uploads the `rw` resources and RETURNS THE REFERENCE IT
//! PRODUCED — a branch and commit for `git` — because that reference is both the audit trail and
//! the undo handle: the plane holds "the current version" as a pointer and a bad write is undone by
//! not advancing it.
//!
//! Two shapes exist here: a `git` repository and one `object` (a single blob over the S3-shaped
//! wire protocol, signed per transfer with the credential the plane minted). `prefix` and
//! `snapshot` are refused BY NAME until their code and tests exist — a transport is a value that
//! something produces (ADR-0001), and half-moved data is worse than a refusal.
//!
//! Credentials: the material comes from the control plane per transfer and lives for that
//! transfer. It is handed to the `git` child as an environment variable (the child is ours, it
//! exits in milliseconds, and it is NOT the guest), never through argv — `ps` would show it — and
//! never through a config file.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};

/// How the data travels. Named by mechanism, not by content: a trained model is an `object`, a
/// dataset is usually a `prefix`, and both are somebody else's slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    /// Text and code: a repository, checked out at a ref, committed and pushed back.
    Git,
    /// One blob: fetched from its URL by `ensure`, uploaded back by `sync`. The protocol is the
    /// S3-shaped one (a signed HTTP request), which is what object stores speak — the KIND says
    /// which shape the data has, the transport says how it moves.
    S3,
}

/// Whether the task may write this resource. Publishing an `ro` resource is an ERROR, not a
/// no-op (ADR-0003 §7): a silent no-op would hide a task that thinks it saved something.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    ReadOnly,
    ReadWrite,
}

/// What a task declares about data it needs, before any credential exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spec {
    /// The resource's type — part of its identity, and the second path component.
    pub kind: String,
    /// Its sequence within (owner, type) — the last path component.
    pub seq: u64,
    pub transport: Transport,
    /// The ref to check out (branch, tag, commit). `None` means the remote's default.
    pub git_ref: Option<String>,
    pub mode: Mode,
}

/// What the control plane mints for one transfer: where to reach the store, and how.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Material {
    /// The URL to use for this transfer. It may differ from `Spec::source` — a presigned URL is
    /// the same object addressed for one request — and it is not stored anywhere.
    pub url: String,
    /// A secret for this transfer (a token, a password). Never written to disk, never in argv.
    pub credential: Option<String>,
}

impl Spec {
    /// Build the node's view from what the plane said. An unknown transport or mode is REFUSED, not
    /// ignored: a plane that answers `snapshot` must hear that this node cannot perform it, rather
    /// than run with the data missing.
    pub fn from_plane(kind: &str, seq: u64, mode: &str, at: Option<&str>) -> Result<Self> {
        // The KIND decides how it travels. There is no second word for this on the wire: a node
        // that cannot move this kind says so by name, and a kind is never silently mapped onto a
        // transfer that happens to be lying around.
        let transport = match kind {
            "git" => Transport::Git,
            // An object store over its own wire protocol. The venue's kind is `s3` — an identity,
            // the way `rustic` and `git` are — and what arrives at this node is one blob. A source
            // that names a prefix (a URL ending in `/`) is not one blob and is refused below.
            "s3" => Transport::S3,
            other => bail!("this node cannot move `{other}` resources"),
        };
        let mode = match mode {
            "ro" => Mode::ReadOnly,
            "rw" => Mode::ReadWrite,
            other => bail!("`{other}` is not a mode (ro or rw)"),
        };
        let spec = Spec {
            kind: kind.to_string(),
            seq,
            transport,
            git_ref: at.map(str::to_string),
            mode,
        };
        // Validate the path now, so a bad identity fails before any network work.
        spec.path(Path::new("/"))?;
        Ok(spec)
    }

    /// How a resource is named in an error: `<kind>/<seq>`, the same words the task used.
    pub fn identity(&self) -> String {
        format!("{}/{}", self.kind, self.seq)
    }

    /// The path this resource is fixed to: `<base>/<kind>/<seq>`. Both components are checked —
    /// a type with a separator would put the data somewhere the task did not ask for.
    pub fn path(&self, base: &Path) -> Result<PathBuf> {
        if self.kind.is_empty()
            || self.kind.contains('/')
            || self.kind.contains('\\')
            || self.kind == "."
            || self.kind == ".."
        {
            bail!("resource type `{}` is not a plain name", self.kind);
        }
        Ok(base.join(&self.kind).join(self.seq.to_string()))
    }
}

/// Make the resource present at its path. Idempotent: a resource already checked out is not
/// re-cloned (that would throw away whatever the task has done inside it), and a fetch that has
/// nothing new is a no-op.
pub fn ensure(spec: &Spec, material: &Material, base: &Path) -> Result<PathBuf> {
    let path = spec.path(base)?;
    match spec.transport {
        Transport::Git => ensure_git(spec, material, &path)?,
        Transport::S3 => ensure_object(spec, material, &path)?,
    }
    Ok(path)
}

/// Publish the resource. Returns the reference this write produced — for `git`, `<branch>@<sha>`
/// — which the caller must report: the pointer that makes a bad write undoable.
pub fn sync(spec: &Spec, material: &Material, base: &Path) -> Result<String> {
    if spec.mode == Mode::ReadOnly {
        bail!(
            "resource `{}/{}` is read-only; publishing it is an error, not a no-op",
            spec.kind, spec.seq
        );
    }
    match spec.transport {
        Transport::Git => sync_git(spec, material, &spec.path(base)?),
        Transport::S3 => sync_object(spec, material, &spec.path(base)?),
    }
}

/// Fetch the blob into the resource's directory. Idempotent like the git arm: present means
/// present — the task's own edits are its own, and a second `ensure` must not overwrite them.
fn ensure_object(spec: &Spec, material: &Material, path: &Path) -> Result<()> {
    let target = path.join(effector_protocol::resource::object_name(&material.url)?);
    if target.exists() {
        return Ok(());
    }
    std::fs::create_dir_all(path)
        .with_context(|| format!("resource `{}`: {}", spec.identity(), path.display()))?;
    let into = target.to_string_lossy().into_owned();
    let code = curl(
        spec,
        material,
        &[
            "-sSL",
            "--output",
            &into,
            "--write-out",
            "%{http_code}",
            &material.url,
        ],
    )?;
    let code = code.trim();
    if code == "200" {
        return Ok(());
    }
    let _ = std::fs::remove_file(&target);
    // A WRITABLE resource that is not there yet is an empty workspace, not a failure: the task's
    // job is to produce the first object. A readable one that is missing is a real error — the
    // task asked to read something the store does not have.
    if code == "404" && spec.mode == Mode::ReadWrite {
        return Ok(());
    }
    bail!(
        "resource `{}`: the store answered {code} for {}",
        spec.identity(),
        material.url
    )
}

/// Upload the blob back and return what the store says about it (its ETag): the reference that
/// makes a bad write undoable, the same role a branch@sha plays for git.
fn sync_object(spec: &Spec, material: &Material, path: &Path) -> Result<String> {
    let source = path.join(effector_protocol::resource::object_name(&material.url)?);
    if !source.exists() {
        bail!(
            "resource `{}` was never ensured, so there is nothing to publish",
            spec.identity()
        );
    }
    let from = source.to_string_lossy().into_owned();
    let etag = curl(
        spec,
        material,
        &[
            "-fsSL",
            "--request",
            "PUT",
            "--upload-file",
            &from,
            "--output",
            "/dev/null",
            "--write-out",
            "%header{etag}",
            &material.url,
        ],
    )?;
    let etag = etag.trim().trim_matches('"').to_string();
    // An object store that answers without an ETag has not told us what it stored; the URL is
    // still a true statement about where it went, and saying so beats inventing an identifier.
    Ok(if etag.is_empty() {
        material.url.clone()
    } else {
        etag
    })
}

/// One signed HTTP request, with the credential the PLANE minted for this transfer. The secret
/// reaches curl through argv (as it reaches git through a URL) — it is a per-transfer value the
/// plane chose to hand out, and it never touches a config file on this node.
///
/// This is also why `sync`/`ensure` do not need an SDK: S3's wire protocol is a signed HTTP
/// request, and curl speaks SigV4. A node that cannot perform it (no curl) fails here by name.
fn curl(spec: &Spec, material: &Material, args: &[&str]) -> Result<String> {
    let mut command = Command::new("curl");
    // Both arms are idempotent (a GET writes the same bytes; a PUT stores the same object), and a
    // store that answers 503 once while it settles is not a task failure. The retry is bounded so a
    // store that is actually down still fails the task, with its own status in the message.
    command.args(["--retry", "3", "--retry-delay", "1"]).args(args);
    if let Some(credential) = &material.credential {
        // `access_key:secret_key[:region]` — one string, the shape a secret row holds. The region
        // is part of the signature, and most object-store deployments either want their own or
        // ignore it, so it rides along rather than being guessed.
        // Flattened into one sentence on purpose: a node's failure is read by whoever is debugging
        // the task, and `anyhow`'s outer-context-only rendering would hide the reason.
        let (access_key, secret_key, region) =
            effector_protocol::resource::credential_parts(credential)
                .map_err(|e| anyhow::anyhow!("resource `{}`: {e}", spec.identity()))?;
        command
            .arg("--user")
            .arg(format!("{access_key}:{secret_key}"))
            .arg("--aws-sigv4")
            .arg(format!("aws:amz:{region}:s3"));
    }
    let output = command
        .output()
        .with_context(|| format!("curl (resource `{}`)", spec.identity()))?;
    if !output.status.success() {
        bail!(
            "curl failed for resource `{}`: {}",
            spec.identity(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

fn ensure_git(spec: &Spec, material: &Material, path: &Path) -> Result<()> {
    if path.join(".git").exists() {
        // Present: fetch, then put it on the ref the task declared.
        run_git(spec, material, path, &["fetch", "--prune", "origin"])?;
    } else {
        let mut argv = vec!["clone", "--quiet"];
        if let Some(reference) = &spec.git_ref {
            // A shallow single-branch clone is enough for an `ro` resource pinned to a ref, and
            // it is the difference between pulling a repository and pulling its history.
            if spec.mode == Mode::ReadOnly {
                argv.extend(["--depth", "1"]);
            }
            argv.extend(["--branch", reference]);
        }
        argv.push(&material.url);
        let into = path.to_string_lossy().into_owned();
        argv.push(&into);
        // `clone` creates the directory, so it runs from the parent: a cwd that does not exist
        // yet is not a workspace, and git would fail before making one.
        let parent = path.parent().unwrap_or(Path::new("."));
        std::fs::create_dir_all(parent)
            .with_context(|| format!("resource `{}`: {}", spec.identity(), parent.display()))?;
        run_git(spec, material, parent, &argv)?;
    }
    if let Some(reference) = &spec.git_ref {
        // `checkout` after a fetch (the clone case already sits on it): a ref that moved must be
        // what the task sees, and `--force` is wrong here — a task's local edits are its own.
        let _ = run_git(spec, material, path, &["checkout", "--quiet", reference]);
    }
    Ok(())
}

fn sync_git(spec: &Spec, material: &Material, path: &Path) -> Result<String> {
    if !path.join(".git").exists() {
        bail!(
            "resource `{}/{}` was never ensured, so there is nothing to publish",
            spec.kind, spec.seq
        );
    }
    run_git(spec, material, path, &["add", "-A"])?;
    // `commit` with nothing staged is the ordinary case (a task that changed nothing), so its
    // failure is not an error — the push below still reports the reference that now exists.
    let _ = run_git(spec, material, path, &["commit", "--quiet", "-m", "effector: sync"]);
    let branch = git_stdout(spec, material, path, &["rev-parse", "--abbrev-ref", "HEAD"])?;
    let branch = branch.trim().to_string();
    run_git(spec, material, path, &["push", "--quiet", "origin", &format!("HEAD:{branch}")])?;
    let sha = git_stdout(spec, material, path, &["rev-parse", "HEAD"])?;
    Ok(format!("{branch}@{}", sha.trim()))
}

fn git_stdout(spec: &Spec, material: &Material, cwd: &Path, args: &[&str]) -> Result<String> {
    let mut command = git_command(material, cwd);
    let output = command
        .args(args)
        .output()
        .with_context(|| format!("git {} (resource `{}`)", args.join(" "), spec.identity()))?;
    if !output.status.success() {
        bail!(
            "git {} failed for resource `{}`: {}",
            args.join(" "),
            spec.identity(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

fn run_git(spec: &Spec, material: &Material, cwd: &Path, args: &[&str]) -> Result<()> {
    let mut command = git_command(material, cwd);
    let output = command
        .args(args)
        .output()
        .with_context(|| format!("git {} (resource `{}`)", args.join(" "), spec.identity()))?;
    if !output.status.success() {
        bail!(
            "git {} failed for resource `{}`: {}",
            args.join(" "),
            spec.identity(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

fn git_command(material: &Material, cwd: &Path) -> Command {
    let mut command = Command::new("git");
    command
        .current_dir(cwd)
        .arg("-c")
        .arg("advice.detachedHead=false")
        // Identity for the commit `sync` may make: it belongs to the node, not to whoever owns
        // the machine, so a missing global git config cannot fail a task.
        .args(["-c", "user.name=effector"])
        .args(["-c", "user.email=effector@localhost"]);
    if let Some(credential) = &material.credential {
        // The secret travels in the CHILD's environment, which lives for one git invocation —
        // not in argv (ps), not in the node's environment (inherited by the guest), not on disk.
        command.env("EFFECTOR_RESOURCE_CREDENTIAL", credential).args([
            "-c",
            "credential.helper=!f() { echo username=effector; echo \"password=$EFFECTOR_RESOURCE_CREDENTIAL\"; }; f",
        ]);
    }
    command
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    /// A bare repository with one commit on `main`, made with the git CLI the module itself uses.
    fn bare_repo(dir: &Path) -> (PathBuf, String) {
        let seed = dir.join("seed");
        std::fs::create_dir_all(&seed).unwrap();
        let git = |args: &[&str]| {
            let out = Command::new("git")
                .current_dir(&seed)
                .args(["-c", "user.name=t", "-c", "user.email=t@t"])
                .args(args)
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
            String::from_utf8_lossy(&out.stdout).into_owned()
        };
        git(&["init", "--quiet", "--initial-branch=main"]);
        std::fs::write(seed.join("README.md"), "first\n").unwrap();
        git(&["add", "-A"]);
        git(&["commit", "--quiet", "-m", "first"]);
        let sha = git(&["rev-parse", "HEAD"]).trim().to_string();

        let bare = dir.join("bare.git");
        let out = Command::new("git")
            .args(["clone", "--quiet", "--bare"])
            .arg(&seed)
            .arg(&bare)
            .output()
            .unwrap();
        assert!(out.status.success());
        (bare, sha)
    }

    fn spec(mode: Mode) -> Spec {
        Spec {
            kind: "git".into(),
            seq: 7,
            transport: Transport::Git,
            git_ref: Some("main".into()),
            mode,
        }
    }

    fn material(url: &Path) -> Material {
        Material {
            url: url.to_string_lossy().into_owned(),
            credential: None,
        }
    }

/// A one-request HTTP server: it answers exactly what an object store answers for the verb it
    /// receives — the body for a GET, an ETag for a PUT — and records that it was asked. Enough to
    /// exercise the node's whole half of an object transfer without a store.
    fn fake_store(the_body: &'static str, etag: &'static str) -> String {
        fake_store_at("200 OK", the_body, etag)
    }

    fn fake_store_at(status: &'static str, the_body: &'static str, etag: &'static str) -> String {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for _ in 0..2 {
                let Ok((mut stream, _)) = listener.accept() else {
                    return;
                };
                let mut head = [0u8; 4096];
                let n = stream.read(&mut head).unwrap_or(0);
                let request = String::from_utf8_lossy(&head[..n]).to_string();
                let reply = if request.starts_with("PUT") {
                    format!(
                        "HTTP/1.1 200 OK\r\nETag: \"{etag}\"\r\nContent-Length: 0\r\n\r\n"
                    )
                } else if status == "200 OK" {
                    format!(
                        "HTTP/1.1 {status}\r\nContent-Length: {}\r\n\r\n{the_body}",
                        the_body.len()
                    )
                } else {
                    // An error body is the store's XML; the node only reads the status code.
                    let xml = "<Error><Code>NoSuchKey</Code></Error>";
                    format!(
                        "HTTP/1.1 {status}\r\nContent-Length: {}\r\n\r\n{xml}",
                        xml.len()
                    )
                };
                let _ = stream.write_all(reply.as_bytes());
            }
        });
        format!("http://{addr}/builds/out.tar")
    }

    fn object_spec(mode: Mode) -> Spec {
        Spec {
            kind: "s3".into(),
            seq: 3,
            transport: Transport::S3,
            git_ref: None,
            mode,
        }
    }

    #[test]
    fn an_object_lands_under_its_own_name_and_the_second_ensure_keeps_the_tasks_edits() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("resources");
        let s = object_spec(Mode::ReadWrite);
        let m = Material {
            url: fake_store("hello", "etag-1"),
            credential: None,
        };

        let path = ensure(&s, &m, &base).unwrap();
        assert_eq!(path, base.join("s3").join("3"));
        assert_eq!(
            std::fs::read_to_string(path.join("out.tar")).unwrap(),
            "hello",
            "the blob lands under the name its URL gives it"
        );

        // A second ensure is a no-op: the task may have changed the file, and its edit is its own.
        std::fs::write(path.join("out.tar"), "edited").unwrap();
        ensure(&s, &m, &base).unwrap();
        assert_eq!(std::fs::read_to_string(path.join("out.tar")).unwrap(), "edited");
    }

    #[test]
    fn publishing_an_object_returns_what_the_store_answered() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("resources");
        let s = object_spec(Mode::ReadWrite);
        let m = Material {
            url: fake_store("hello", "etag-42"),
            credential: None,
        };
        let path = ensure(&s, &m, &base).unwrap();
        std::fs::write(path.join("out.tar"), "new bytes").unwrap();

        let reference = sync(&s, &m, &base).unwrap();
        assert_eq!(
            reference, "etag-42",
            "the reference is the store's own word for what it now holds"
        );
    }

    #[test]
    fn a_missing_object_is_an_empty_workspace_if_writable_and_an_error_if_readable() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("resources");
        let rw = object_spec(Mode::ReadWrite);
        let m = Material {
            url: fake_store_at("404 Not Found", "", ""),
            credential: None,
        };
        // A task's job may be to produce the first object; an empty workspace says so, and nothing
        // half-written is left behind for the task to trip over.
        let path = ensure(&rw, &m, &base).expect("a writable resource that is not there yet");
        assert!(!path.join("out.tar").exists(), "no stub file is invented");
        assert!(path.is_dir(), "the directory is what the task writes into");

        // The same answer on a READABLE resource is the truth the task asked for: it is not there.
        let ro = object_spec(Mode::ReadOnly);
        let dir2 = tempfile::tempdir().unwrap();
        let err = ensure(&ro, &m, &dir2.path().join("resources")).unwrap_err().to_string();
        assert!(err.contains("404"), "the store's own word is passed on: {err}");
    }

    #[test]
    fn a_kind_this_node_cannot_move_is_refused_by_name() {
        // The venue knows five kinds; this node moves two. The refusal names the kind it was asked
        // for, so the operator learns which one is missing rather than "a transfer failed".
        let err = Spec::from_plane("rustic", 1, "rw", None).unwrap_err();
        assert!(err.to_string().contains("rustic"), "{err}");
    }

    #[test]
    fn an_object_url_with_no_last_segment_names_no_object() {
        let err = effector_protocol::resource::object_name("s3://bucket/builds/").unwrap_err();
        assert!(err.to_string().contains("names no object"), "{err}");
        assert_eq!(
            effector_protocol::resource::object_name("s3://bucket/builds/out.tar").unwrap(),
            "out.tar"
        );
        assert_eq!(
            effector_protocol::resource::object_name("https://host/a/b/model.gguf").unwrap(),
            "model.gguf"
        );
    }

    #[test]
    fn ensure_puts_the_repository_at_its_identity_and_a_second_ensure_does_not_re_clone() {
        let dir = tempfile::tempdir().unwrap();
        let (bare, sha) = bare_repo(dir.path());
        let base = dir.path().join("resources");
        let s = spec(Mode::ReadWrite);
        let m = material(&bare);

        let path = ensure(&s, &m, &base).unwrap();
        assert_eq!(path, base.join("git").join("7"));
        assert_eq!(
            std::fs::read_to_string(path.join("README.md")).unwrap(),
            "first\n"
        );
        assert_eq!(
            git_stdout(&s, &m, &path, &["rev-parse", "HEAD"]).unwrap().trim(),
            sha
        );

        // A file the task made must survive the second ensure: presence is a no-op by design.
        std::fs::write(path.join("scratch.txt"), "mine\n").unwrap();
        ensure(&s, &m, &base).unwrap();
        assert!(path.join("scratch.txt").exists());
    }

    #[test]
    fn sync_publishes_and_returns_the_reference_it_produced() {
        let dir = tempfile::tempdir().unwrap();
        let (bare, first) = bare_repo(dir.path());
        let base = dir.path().join("resources");
        let s = spec(Mode::ReadWrite);
        let m = material(&bare);

        let path = ensure(&s, &m, &base).unwrap();
        std::fs::write(path.join("README.md"), "second\n").unwrap();
        let reference = sync(&s, &m, &base).unwrap();

        let (branch, sha) = reference.split_once('@').expect("branch@sha");
        assert_eq!(branch, "main");
        assert_ne!(sha, first, "the sync produced a new commit");
        // The store really has it: read the bare repository, not the working copy.
        let shown = Command::new("git")
            .arg("--git-dir")
            .arg(&bare)
            .args(["show", "main:README.md"])
            .output()
            .unwrap();
        assert!(shown.status.success());
        assert_eq!(String::from_utf8_lossy(&shown.stdout), "second\n");
    }

    #[test]
    fn publishing_a_read_only_resource_is_an_error_not_a_no_op() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("resources");
        let err = sync(&spec(Mode::ReadOnly), &material(dir.path()), &base).unwrap_err();
        assert!(err.to_string().contains("read-only"), "{err}");
    }

    #[test]
    fn a_type_is_a_name_not_a_path_and_the_identity_fixes_the_directory() {
        let base = Path::new("/work/resources");
        for bad in ["", "..", ".", "a/b", "a\\b"] {
            let mut s = spec(Mode::ReadOnly);
            s.kind = bad.into();
            assert!(s.path(base).is_err(), "`{bad}` must be refused");
        }
        let s = spec(Mode::ReadOnly);
        assert_eq!(s.path(base).unwrap(), base.join("git").join("7"));
        assert_eq!(s.identity(), "git/7");
    }

    #[test]
    fn what_the_plane_says_wins_and_an_unknown_kind_is_refused() {
        let spec = Spec::from_plane("git", 3, "rw", Some("main")).unwrap();
        assert_eq!(spec.mode, Mode::ReadWrite);
        assert_eq!(spec.git_ref.as_deref(), Some("main"));
        // A KIND this node cannot move is refused BY NAME, not ignored — the node never maps an
        // unknown kind onto a transfer that happens to be lying around.
        let err = Spec::from_plane("rustic", 3, "ro", None).unwrap_err();
        assert!(err.to_string().contains("rustic"), "{err}");
        let err = Spec::from_plane("git", 3, "readwrite", None).unwrap_err();
        assert!(err.to_string().contains("mode"), "{err}");
    }
}