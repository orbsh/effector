//! The resource contract's node half (ADR-0003 §7, revised by ADR-0049): `ensure` and `sync`, one
//! mechanism at a time.
//!
//! A script asks for durable data while it runs (`ctx.resource`): it names a KIND and a `remote` —
//! its own name for the thing, the way an S3 client names a key — and the node asks the plane for
//! what reaching that kind takes. The plane answers a ROOT (already separated by tenant) plus a
//! credential; this module joins the script's name onto the root and moves the bytes. Neither the
//! root nor the credential is ever handed to the script.
//!
//! `ensure` is idempotent (present → no-op), which is why a resource discovered mid-script costs a
//! file or a directory appearing rather than a restart. `sync` produces a REFERENCE —
//! `<branch>@<sha>` for git, the store's ETag for an object — and that reference goes back to the
//! SCRIPT, not into a table: the store already keeps the history, and a pointer kept here would be
//! a second copy of the truth that can disagree with the first one.
//!
//! Two mechanisms exist here: `git` and `object`. `archive` (an incremental repository) is refused
//! BY NAME until its code and tests exist — a mechanism is something that actually moves the data,
//! and half-moved data is worse than a refusal.
//!
//! Credentials: the material comes from the control plane per transfer and lives for that
//! transfer. It is handed to the `git` child as an environment variable (the child is ours, it
//! exits in milliseconds, and it is NOT the guest), never through argv — `ps` would show it — and
//! never through a config file.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};

/// How the data travels, named by MECHANISM rather than by the store it happens to sit in: the same
/// bucket can hold one object or an incremental repository, and only the mechanism says which.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    /// Text and code: a repository, checked out at a ref, committed and pushed back.
    Git,
    /// One blob: fetched from its address by `ensure`, uploaded back by `sync`. The protocol is the
    /// S3-shaped one (a signed HTTP request), which is what object stores speak.
    Object,
}

/// Whether the task may write this kind. Publishing an `ro` grant is an ERROR, not a no-op
/// (ADR-0003 §7): a silent no-op would hide a task that thinks it saved something.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    ReadOnly,
    ReadWrite,
}

/// What a script asked for, reduced to what the node needs to pick a mechanism.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spec {
    /// The mechanism's identity, as the venue names it.
    pub kind: String,
    pub transport: Transport,
    /// The ref to check out for `git` (branch, tag, commit). `None` means the remote's default.
    pub at: Option<String>,
    pub mode: Mode,
}

/// What the plane hands over for one kind: the root this execution may reach, and the credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Material {
    /// `<base>/<tenant>/<kind>` — the slice of the store this execution may name things inside.
    pub base: String,
    /// A secret for this transfer (a token, a password). Never written to disk, never in argv.
    pub credential: Option<String>,
}

impl Spec {
    /// Build the node's view from what the plane said. An unknown mechanism or mode is REFUSED, not
    /// ignored: a plane that answers `archive` must hear that this node cannot perform it, rather
    /// than run with the data missing.
    pub fn from_plane(kind: &str, mode: &str, at: Option<&str>) -> Result<Self> {
        let transport = match kind {
            "git" => Transport::Git,
            "object" => Transport::Object,
            other => bail!("this node cannot move `{other}` resources"),
        };
        let mode = match mode {
            "ro" => Mode::ReadOnly,
            "rw" => Mode::ReadWrite,
            other => bail!("`{other}` is not a mode (ro or rw)"),
        };
        Ok(Spec {
            kind: kind.to_string(),
            transport,
            at: at.filter(|s| !s.is_empty()).map(str::to_string),
            mode,
        })
    }

    /// How one ask is named in an error: `<kind>:<remote>`, in the words the script used.
    fn label(&self, remote: &str) -> String {
        format!("{}:{remote}", self.kind)
    }
}

/// A script's `remote` is a RELATIVE name — one or more plain segments. It cannot climb, cannot be
/// absolute, and cannot carry a backslash: the tenant prefix is the node's to compose, and a name
/// that could undo it would put the data somewhere nobody asked for.
fn plain_remote(remote: &str) -> Result<()> {
    if remote.is_empty() {
        bail!("a resource needs a `remote` name");
    }
    if remote.starts_with('/') || remote.ends_with('/') {
        bail!("`{remote}` is not a relative name (no leading or trailing `/`)");
    }
    if remote.contains('\\') || remote.contains('\u{1f}') {
        bail!("`{remote}` is not a plain name");
    }
    for segment in remote.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            bail!("`{remote}` may not climb or carry an empty segment");
        }
    }
    Ok(())
}

/// Where a name lands on this machine: the `mount` the script asked for, or `<root>/<remote>`.
pub fn location(root: &Path, mount: Option<&Path>, remote: &str) -> Result<PathBuf> {
    plain_remote(remote)?;
    Ok(match mount {
        Some(path) => path.to_path_buf(),
        None => root.join(remote),
    })
}

/// The address one name reaches: `<root>/<remote>`, joined once so nothing else has to think about
/// trailing slashes.
fn address(material: &Material, remote: &str) -> String {
    format!("{}/{}", material.base.trim_end_matches('/'), remote)
}

/// "Was this call cancelled?" — the one question a transfer asks while it runs.
///
/// A transfer is the only thing a node does that can take minutes, so it is the only thing where
/// "the caller gave up" has to reach the WORK and not just the bookkeeping. The token is the call's
/// own (`CallControl`), never a node-wide flag: a flag that outlived a call would stop the next one.
pub trait Interrupt {
    fn cancelled(&self) -> bool;

    /// Called while a child is running, on every poll: "the work is still moving".
    ///
    /// A transfer cannot say it is alive the way a conversation can — there is no turn to answer —
    /// so the ONLY party that knows a clone is still receiving bytes is the process watching it.
    /// Implementations throttle; the poll rate is this file's, not the wire's.
    fn advanced(&self) {}
}

impl Interrupt for crate::carrier::CallControl {
    fn cancelled(&self) -> bool {
        self.is_cancelled()
    }
}

fn cancelled(cancel: Option<&dyn Interrupt>) -> bool {
    cancel.is_some_and(|c| c.cancelled())
}

/// Run one child to completion — or kill it the moment the call is cancelled.
///
/// `Command::output()` cannot be interrupted: it blocks until the child is gone, so a 2 GB clone
/// ignores every cancellation until it finishes. Spawning plus a poll loop is the only way the
/// answer reaches the transfer, and the pipes are drained on their own threads because a child that
/// fills a pipe while we are polling would deadlock instead of exiting.
fn run_child(
    command: &mut Command,
    cancel: Option<&dyn Interrupt>,
    what: &str,
) -> Result<std::process::Output> {
    use std::io::Read as _;
    use std::process::Stdio;

    if cancelled(cancel) {
        bail!("`{what}` was cancelled before it started");
    }
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().with_context(|| format!("starting {what}"))?;
    let drain = |pipe: Option<Box<dyn std::io::Read + Send>>| {
        pipe.map(|mut p| {
            std::thread::spawn(move || {
                let mut out = Vec::new();
                let _ = p.read_to_end(&mut out);
                out
            })
        })
    };
    let out_thread = drain(child.stdout.take().map(|s| Box::new(s) as Box<dyn std::io::Read + Send>));
    let err_thread = drain(child.stderr.take().map(|s| Box::new(s) as Box<dyn std::io::Read + Send>));
    let status = loop {
        if cancelled(cancel) {
            // Kill and REPAP: a zombie that nobody waits on is a leaked pid, and the caller is about
            // to be told the transfer did not finish, so there is nothing to read back.
            let _ = child.kill();
            let _ = child.wait();
            bail!("`{what}` was cancelled");
        }
        match child.try_wait().with_context(|| format!("waiting on {what}"))? {
            Some(status) => break status,
            None => {
                // The child is still working: the one fact only this loop can observe.
                if let Some(c) = cancel {
                    c.advanced();
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }
    };
    let join = |h: Option<std::thread::JoinHandle<Vec<u8>>>| {
        h.and_then(|h| h.join().ok()).unwrap_or_default()
    };
    Ok(std::process::Output {
        status,
        stdout: join(out_thread),
        stderr: join(err_thread),
    })
}

/// Make the data present at its location. Idempotent: a repository already checked out is not
/// re-cloned (that would throw away whatever the task has done inside it), and an object already
/// fetched is not overwritten.
pub fn ensure(
    spec: &Spec,
    material: &Material,
    remote: &str,
    mount: Option<&Path>,
    root: &Path,
    cancel: Option<&dyn Interrupt>,
) -> Result<PathBuf> {
    let path = location(root, mount, remote)?;
    let outcome = match spec.transport {
        Transport::Git => ensure_git(spec, material, remote, &path, cancel),
        Transport::Object => ensure_object(spec, material, remote, &path, cancel),
    };
    if let Err(e) = outcome {
        // A cancelled transfer is not a FAILED one, it is an UNFINISHED one — and what it leaves is a
        // lie that the next caller cannot tell from the truth: half a clone has a `.git`, half a
        // download has the right name. Removing it is the only state the next `ensure` can trust.
        if cancelled(cancel) {
            let _ = std::fs::remove_dir_all(&path);
            let _ = std::fs::remove_file(&path);
        }
        return Err(e);
    }
    Ok(path)
}

/// Publish. Returns the reference this write produced — `<branch>@<sha>` for git, the store's ETag
/// for an object — which goes back to the script: it is the caller's handle on what it just wrote.
pub fn sync(
    spec: &Spec,
    material: &Material,
    remote: &str,
    mount: Option<&Path>,
    root: &Path,
    cancel: Option<&dyn Interrupt>,
) -> Result<String> {
    if spec.mode == Mode::ReadOnly {
        bail!(
            "`{}` is read-only; publishing it is an error, not a no-op",
            spec.label(remote)
        );
    }
    let path = location(root, mount, remote)?;
    match spec.transport {
        // A cancelled publish does NOT clean up: the bytes on disk are the task's own work, and
        // deleting them because the caller gave up would destroy the only copy of what it made.
        Transport::Git => sync_git(spec, material, remote, &path, cancel),
        Transport::Object => sync_object(spec, material, remote, &path, cancel),
    }
}

/// Fetch the blob to its location. Idempotent like the git arm: present means present — the task's
/// own bytes are its own, and a second `ensure` must not overwrite them.
fn ensure_object(
    spec: &Spec,
    material: &Material,
    remote: &str,
    path: &Path,
    cancel: Option<&dyn Interrupt>,
) -> Result<()> {
    if path.exists() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("`{}`: {}", spec.label(remote), parent.display()))?;
    }
    let into = path.to_string_lossy().into_owned();
    let url = address(material, remote);
    let code = curl(
        spec,
        material,
        remote,
        &["-sSL", "--output", &into, "--write-out", "%{http_code}", &url],
        cancel,
    )?;
    let code = code.trim();
    if code == "200" {
        return Ok(());
    }
    let _ = std::fs::remove_file(path);
    // A WRITABLE name that is not there yet is an empty workspace, not a failure: the task's job is
    // to produce the first object. A readable one that is missing is a real error — the task asked
    // to read something the store does not have.
    if code == "404" && spec.mode == Mode::ReadWrite {
        return Ok(());
    }
    bail!(
        "`{}`: the store answered {code} for {url}",
        spec.label(remote)
    )
}

/// Upload the blob and return what the store says about it (its ETag).
fn sync_object(
    spec: &Spec,
    material: &Material,
    remote: &str,
    path: &Path,
    cancel: Option<&dyn Interrupt>,
) -> Result<String> {
    if !path.exists() {
        bail!(
            "`{}` was never ensured, so there is nothing to publish",
            spec.label(remote)
        );
    }
    let from = path.to_string_lossy().into_owned();
    let url = address(material, remote);
    let etag = curl(
        spec,
        material,
        remote,
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
            &url,
        ],
        cancel,
    )?;
    let etag = etag.trim().trim_matches('"').to_string();
    // An object store that answers without an ETag has not told us what it stored; the address is
    // still a true statement about where it went, and saying so beats inventing an identifier.
    Ok(if etag.is_empty() { url } else { etag })
}

/// One signed HTTP request, with the credential the PLANE handed over for this transfer. The secret
/// reaches curl through argv, which lives for one process and is not the guest.
///
/// This is also why `sync`/`ensure` do not need an SDK: S3's wire protocol is a signed HTTP
/// request, and curl speaks SigV4. A node that cannot perform it (no curl) fails here by name.
fn curl(
    spec: &Spec,
    material: &Material,
    remote: &str,
    args: &[&str],
    cancel: Option<&dyn Interrupt>,
) -> Result<String> {
    let mut command = Command::new("curl");
    // Both arms are idempotent (a GET writes the same bytes; a PUT stores the same object), and a
    // store that answers 503 once while it settles is not a task failure. The retry is bounded so a
    // store that is actually down still fails the task, with its own status in the message.
    command.args(["--retry", "3", "--retry-delay", "1"]).args(args);
    if let Some(credential) = &material.credential {
        // `access_key:secret_key[:region]` — one string, the shape a secret holds. The region is
        // part of the signature and most object-store deployments either want their own or ignore
        // it, so it rides along rather than being guessed.
        //
        // Flattened into one sentence on purpose: a node's failure is read by whoever is debugging
        // the task, and `anyhow`'s outer-context-only rendering would hide the reason.
        let (access_key, secret_key, region) =
            effector_protocol::resource::credential_parts(credential)
                .map_err(|e| anyhow::anyhow!("`{}`: {e}", spec.label(remote)))?;
        command
            .arg("--user")
            .arg(format!("{access_key}:{secret_key}"))
            .arg("--aws-sigv4")
            .arg(format!("aws:amz:{region}:s3"));
    }
    let label = format!("curl (`{}`)", spec.label(remote));
    let output = run_child(&mut command, cancel, &label)?;
    if !output.status.success() {
        bail!(
            "curl failed for `{}`: {}",
            spec.label(remote),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

fn ensure_git(
    spec: &Spec,
    material: &Material,
    remote: &str,
    path: &Path,
    cancel: Option<&dyn Interrupt>,
) -> Result<()> {
    let url = address(material, remote);
    if path.join(".git").exists() {
        // Present: fetch, then put it on the ref the task asked for.
        run_git(spec, material, remote, path, &["fetch", "--prune", "origin"], cancel)?;
    } else {
        let mut argv = vec!["clone", "--quiet"];
        if let Some(reference) = &spec.at {
            // A shallow single-branch clone is enough for a pinned `ro` read, and it is the
            // difference between pulling a repository and pulling its history.
            if spec.mode == Mode::ReadOnly {
                argv.extend(["--depth", "1"]);
            }
            argv.extend(["--branch", reference]);
        }
        argv.push(&url);
        let into = path.to_string_lossy().into_owned();
        argv.push(&into);
        // `clone` creates the directory, so it runs from the parent: a cwd that does not exist yet
        // is not a workspace, and git would fail before making one.
        let parent = path.parent().unwrap_or(Path::new("."));
        std::fs::create_dir_all(parent)
            .with_context(|| format!("`{}`: {}", spec.label(remote), parent.display()))?;
        run_git(spec, material, remote, parent, &argv, cancel)?;
    }
    if let Some(reference) = &spec.at {
        // `checkout` after a fetch (the clone case already sits on it): a ref that moved must be
        // what the task sees, and `--force` is wrong here — a task's local edits are its own.
        let _ = run_git(
            spec,
            material,
            remote,
            path,
            &["checkout", "--quiet", reference],
            cancel,
        );
    }
    Ok(())
}

fn sync_git(
    spec: &Spec,
    material: &Material,
    remote: &str,
    path: &Path,
    cancel: Option<&dyn Interrupt>,
) -> Result<String> {
    if !path.join(".git").exists() {
        bail!(
            "`{}` was never ensured, so there is nothing to publish",
            spec.label(remote)
        );
    }
    run_git(spec, material, remote, path, &["add", "-A"], cancel)?;
    // `commit` with nothing staged is the ordinary case (a task that changed nothing), so its
    // failure is not an error — the push below still reports the reference that now exists.
    let _ = run_git(
        spec,
        material,
        remote,
        path,
        &["commit", "--quiet", "-m", "effector: sync"],
        cancel,
    );
    let branch = git_stdout(
        spec,
        material,
        remote,
        path,
        &["rev-parse", "--abbrev-ref", "HEAD"],
        cancel,
    )?;
    let branch = branch.trim().to_string();
    run_git(
        spec,
        material,
        remote,
        path,
        &["push", "--quiet", "origin", &format!("HEAD:{branch}")],
        cancel,
    )?;
    let sha = git_stdout(spec, material, remote, path, &["rev-parse", "HEAD"], cancel)?;
    Ok(format!("{branch}@{}", sha.trim()))
}

fn git_stdout(
    spec: &Spec,
    material: &Material,
    remote: &str,
    cwd: &Path,
    args: &[&str],
    cancel: Option<&dyn Interrupt>,
) -> Result<String> {
    let mut command = git_command(material, cwd);
    command.args(args);
    let what = format!("git {} (`{}`)", args.join(" "), spec.label(remote));
    let output = run_child(&mut command, cancel, &what)?;
    if !output.status.success() {
        bail!(
            "git {} failed for `{}`: {}",
            args.join(" "),
            spec.label(remote),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

fn run_git(
    spec: &Spec,
    material: &Material,
    remote: &str,
    cwd: &Path,
    args: &[&str],
    cancel: Option<&dyn Interrupt>,
) -> Result<()> {
    let mut command = git_command(material, cwd);
    command.args(args);
    let what = format!("git {} (`{}`)", args.join(" "), spec.label(remote));
    let output = run_child(&mut command, cancel, &what)?;
    if !output.status.success() {
        bail!(
            "git {} failed for `{}`: {}",
            args.join(" "),
            spec.label(remote),
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
        // Identity for the commit `sync` may make: it belongs to the node, not to whoever owns the
        // machine, so a missing global git config cannot fail a task.
        .args(["-c", "user.name=effector"])
        .args(["-c", "user.email=effector@localhost"]);
    if let Some(credential) = &material.credential {
        // The secret travels in the CHILD's environment, which lives for one git invocation — not
        // in argv (ps), not in the node's environment (inherited by the guest), not on disk.
        command.env("EFFECTOR_RESOURCE_CREDENTIAL", credential).args([
            "-c",
            "credential.helper=!f() { echo username=effector; echo \"password=*** }; f",
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
            transport: Transport::Git,
            at: Some("main".into()),
            mode,
        }
    }

    /// A call's cancellation as the resource layer sees it: one flag, set from another thread —
    /// exactly what `CallControl::interrupt` does to a live transfer.
    struct Stub(std::sync::Arc<std::sync::atomic::AtomicBool>);

    impl Interrupt for Stub {
        fn cancelled(&self) -> bool {
            self.0.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    /// The other half of the poll loop: a child that is still running says so. Without this a
    /// transfer is indistinguishable from a hang, and the plane's deadline kills healthy clones.
    #[test]
    fn a_running_child_reports_that_it_is_alive() {
        struct Counting(std::sync::Arc<std::sync::atomic::AtomicUsize>);
        impl Interrupt for Counting {
            fn cancelled(&self) -> bool {
                false
            }
            fn advanced(&self) {
                self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        let ticks = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = Counting(ticks.clone());
        let mut command = Command::new("sleep");
        command.arg("0.4");
        let out = run_child(&mut command, Some(&counter), "sleep").unwrap();
        assert!(out.status.success());
        // 400 ms at one poll per 50 ms: a handful, and certainly more than "the child started".
        assert!(
            ticks.load(std::sync::atomic::Ordering::Relaxed) >= 3,
            "the poll loop reports while the child works"
        );
    }

    /// `output()` would wait for the child no matter what; this is the test that says a cancelled
    /// call ends the TRANSFER, not just the waiting.
    #[test]
    fn a_child_is_killed_when_the_call_is_cancelled() {
        let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stub = Stub(flag.clone());
        let tick = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(200));
            flag.store(true, std::sync::atomic::Ordering::Relaxed);
        });
        let started = std::time::Instant::now();
        let mut command = Command::new("sleep");
        command.arg("30");
        let err = run_child(&mut command, Some(&stub), "sleep")
            .unwrap_err()
            .to_string();
        tick.join().unwrap();
        assert!(err.contains("cancelled"), "{err}");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "the kill lands when the call is cancelled, not when the child would have finished"
        );
    }

    /// A half clone reads like a clone: `.git` exists and the next `ensure` would believe it. The
    /// cancelled path therefore removes what it made.
    #[test]
    fn a_cancelled_ensure_leaves_nothing_behind() {
        let dir = tempfile::tempdir().unwrap();
        let (bare, _) = bare_repo(dir.path());
        let root = dir.path().join("resources");
        let m = material(&bare);
        let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let err = ensure(&spec(Mode::ReadOnly), &m, "bare.git", None, &root, Some(&Stub(flag)))
            .unwrap_err()
            .to_string();
        assert!(err.contains("cancelled"), "{err}");
        assert!(
            !root.join("bare.git").exists(),
            "an unfinished transfer must not leave a directory the next call trusts"
        );
    }

    fn material(base: &Path) -> Material {
        Material {
            base: base.to_string_lossy().into_owned(),
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
                    format!("HTTP/1.1 200 OK\r\nETag: \"{etag}\"\r\nContent-Length: 0\r\n\r\n")
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
        format!("http://{addr}/builds")
    }

    fn object_spec(mode: Mode) -> Spec {
        Spec {
            kind: "object".into(),
            transport: Transport::Object,
            at: None,
            mode,
        }
    }

    #[test]
    fn a_remote_is_a_relative_name_and_cannot_climb_out_of_its_slice() {
        let root = Path::new("/work/resources");
        for bad in ["", "/abs", "a/", "a//b", "..", "a/../../b", "a\\b", "."] {
            assert!(location(root, None, bad).is_err(), "`{bad}` must be refused");
        }
        assert_eq!(
            location(root, None, "org/repo").unwrap(),
            root.join("org").join("repo"),
            "nested names are ordinary: a repository, an object under a prefix"
        );
        assert_eq!(
            location(root, Some(Path::new("/elsewhere/x")), "org/repo").unwrap(),
            Path::new("/elsewhere/x"),
            "a mount says where the bytes land, and it need not be under the root"
        );
    }

    #[test]
    fn an_object_lands_where_the_name_says_and_the_second_ensure_keeps_the_tasks_edits() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        let s = object_spec(Mode::ReadWrite);
        let m = Material {
            base: fake_store("hello", "etag-1"),
            credential: None,
        };

        let path = ensure(&s, &m, "out.tar", None, &root, None).unwrap();
        assert_eq!(path, root.join("out.tar"));
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "hello",
            "the blob lands at the name the script gave"
        );

        // A second ensure is a no-op: the task may have changed the file, and its edit is its own.
        std::fs::write(&path, "edited").unwrap();
        ensure(&s, &m, "out.tar", None, &root, None).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "edited");
    }

    #[test]
    fn publishing_an_object_returns_what_the_store_answered() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        let s = object_spec(Mode::ReadWrite);
        let m = Material {
            base: fake_store("hello", "etag-42"),
            credential: None,
        };
        let path = ensure(&s, &m, "out.tar", None, &root, None).unwrap();
        std::fs::write(&path, "new bytes").unwrap();

        let reference = sync(&s, &m, "out.tar", None, &root, None).unwrap();
        assert_eq!(
            reference, "etag-42",
            "the reference is the store's own word for what it now holds"
        );
    }

    #[test]
    fn a_missing_object_is_an_empty_workspace_if_writable_and_an_error_if_readable() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        let rw = object_spec(Mode::ReadWrite);
        let m = Material {
            base: fake_store_at("404 Not Found", "", ""),
            credential: None,
        };
        // A task's job may be to produce the first object; an empty workspace says so, and nothing
        // half-written is left behind for the task to trip over.
        let path = ensure(&rw, &m, "out.tar", None, &root, None).expect("nothing there yet is fine");
        assert!(!path.exists(), "no stub file is invented");

        // The same answer on a READABLE name is the truth the task asked for: it is not there.
        let ro = object_spec(Mode::ReadOnly);
        let dir2 = tempfile::tempdir().unwrap();
        let err = ensure(
            &ro,
            &m,
            "out.tar",
            None,
            &dir2.path().join("resources"),
            None,
        )
            .unwrap_err()
            .to_string();
        assert!(err.contains("404"), "the store's own word is passed on: {err}");
    }

    #[test]
    fn a_mechanism_this_node_cannot_move_is_refused_by_name() {
        // The venue serves three; this node moves two. The refusal names the kind it was asked for,
        // so the operator learns which one is missing rather than "a transfer failed".
        let err = Spec::from_plane("archive", "rw", None).unwrap_err();
        assert!(err.to_string().contains("archive"), "{err}");
    }

    #[test]
    fn ensure_puts_the_repository_at_its_name_and_a_second_ensure_does_not_re_clone() {
        let dir = tempfile::tempdir().unwrap();
        let (_bare, sha) = bare_repo(dir.path());
        let root = dir.path().join("resources");
        let s = spec(Mode::ReadWrite);
        // The grant is the DIRECTORY the repositories live in; the script names which one.
        let m = material(dir.path());

        let path = ensure(&s, &m, "bare.git", None, &root, None).unwrap();
        assert_eq!(path, root.join("bare.git"));
        assert_eq!(
            std::fs::read_to_string(path.join("README.md")).unwrap(),
            "first\n"
        );
        assert_eq!(
            git_stdout(&s, &m, "bare.git", &path, &["rev-parse", "HEAD"], None)
                .unwrap()
                .trim(),
            sha
        );

        // A file the task made must survive the second ensure: presence is a no-op by design.
        std::fs::write(path.join("scratch.txt"), "mine\n").unwrap();
        ensure(&s, &m, "bare.git", None, &root, None).unwrap();
        assert!(path.join("scratch.txt").exists());
    }

    #[test]
    fn sync_publishes_and_returns_the_reference_it_produced() {
        let dir = tempfile::tempdir().unwrap();
        let (bare, first) = bare_repo(dir.path());
        let root = dir.path().join("resources");
        let s = spec(Mode::ReadWrite);
        let m = material(dir.path());

        let path = ensure(&s, &m, "bare.git", None, &root, None).unwrap();
        std::fs::write(path.join("README.md"), "second\n").unwrap();
        let reference = sync(&s, &m, "bare.git", None, &root, None).unwrap();

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
    fn publishing_against_a_read_only_grant_is_an_error_not_a_no_op() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        let err = sync(
            &spec(Mode::ReadOnly),
            &material(dir.path()),
            "bare.git",
            None,
            &root,
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("read-only"), "{err}");
    }

    #[test]
    fn what_the_plane_says_wins_and_an_unknown_mechanism_is_refused() {
        let spec = Spec::from_plane("git", "rw", Some("main")).unwrap();
        assert_eq!(spec.mode, Mode::ReadWrite);
        assert_eq!(spec.at.as_deref(), Some("main"));
        let err = Spec::from_plane("git", "readwrite", None).unwrap_err();
        assert!(err.to_string().contains("mode"), "{err}");
    }

    #[test]
    fn a_mount_outside_the_root_is_where_the_bytes_land() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        let mount = dir.path().join("scratch").join("model");
        let s = object_spec(Mode::ReadWrite);
        let m = Material {
            base: fake_store("weights", "etag-9"),
            credential: None,
        };
        let path = ensure(&s, &m, "anything", Some(&mount), &root, None).unwrap();
        assert_eq!(path, mount);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "weights");
        assert!(
            !root.exists(),
            "nothing is invented under the root when the script chose a mount"
        );
    }
}