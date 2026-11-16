//! The resource contract's node half (ADR-0003 §7): `ensure` and `sync`, one transport at a time.
//!
//! A resource is `{ name, transport, source, ref?, mode }`. `name` fixes the path; the transport
//! names HOW data moves, never what it is; `ensure` is idempotent (present → no-op), which is why
//! a resource discovered mid-task costs a directory appearing rather than a restart; `sync`
//! uploads the `rw` resources and RETURNS THE REFERENCE IT PRODUCED — a branch and commit for
//! `git` — because that reference is both the audit trail and the undo handle: the control plane
//! holds "the current version" as a pointer, and a bad write is undone by not advancing it.
//!
//! Only `git` exists here, and that is deliberate: a transport is a value that something produces
//! (ADR-0001). `object`/`prefix` and `snapshot` arrive with their own code and their own tests;
//! until then `Transport::Git` is the only variant the type can express, so no configuration can
//! claim a transport this node cannot perform.
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
    pub name: String,
    pub transport: Transport,
    pub source: String,
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
    /// The path this resource is fixed to: `<base>/<name>`. A name with a path separator or a
    /// `..` is refused rather than sanitised — a resource is a name, not a path, and quietly
    /// rewriting one would put a task's data somewhere it did not ask for.
    pub fn path(&self, base: &Path) -> Result<PathBuf> {
        if self.name.is_empty()
            || self.name.contains('/')
            || self.name.contains('\\')
            || self.name == "."
            || self.name == ".."
        {
            bail!("resource name `{}` is not a plain name", self.name);
        }
        Ok(base.join(&self.name))
    }
}

/// Make the resource present at its path. Idempotent: a resource already checked out is not
/// re-cloned (that would throw away whatever the task has done inside it), and a fetch that has
/// nothing new is a no-op.
pub fn ensure(spec: &Spec, material: &Material, base: &Path) -> Result<PathBuf> {
    let path = spec.path(base)?;
    match spec.transport {
        Transport::Git => ensure_git(spec, material, &path)?,
    }
    Ok(path)
}

/// Publish the resource. Returns the reference this write produced — for `git`, `<branch>@<sha>`
/// — which the caller must report: the pointer that makes a bad write undoable.
pub fn sync(spec: &Spec, material: &Material, base: &Path) -> Result<String> {
    if spec.mode == Mode::ReadOnly {
        bail!(
            "resource `{}` is read-only; publishing it is an error, not a no-op",
            spec.name
        );
    }
    match spec.transport {
        Transport::Git => sync_git(spec, material, &spec.path(base)?),
    }
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
            .with_context(|| format!("resource `{}`: {}", spec.name, parent.display()))?;
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
            "resource `{}` was never ensured, so there is nothing to publish",
            spec.name
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
        .with_context(|| format!("git {} (resource `{}`)", args.join(" "), spec.name))?;
    if !output.status.success() {
        bail!(
            "git {} failed for resource `{}`: {}",
            args.join(" "),
            spec.name,
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
        .with_context(|| format!("git {} (resource `{}`)", args.join(" "), spec.name))?;
    if !output.status.success() {
        bail!(
            "git {} failed for resource `{}`: {}",
            args.join(" "),
            spec.name,
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
            name: "notes".into(),
            transport: Transport::Git,
            source: "the-store".into(),
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

    #[test]
    fn ensure_puts_the_repository_at_its_name_and_a_second_ensure_does_not_re_clone() {
        let dir = tempfile::tempdir().unwrap();
        let (bare, sha) = bare_repo(dir.path());
        let base = dir.path().join("resources");
        let s = spec(Mode::ReadWrite);
        let m = material(&bare);

        let path = ensure(&s, &m, &base).unwrap();
        assert_eq!(path, base.join("notes"));
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
    fn a_name_is_a_name_not_a_path() {
        let base = Path::new("/work/resources");
        for bad in ["", "..", ".", "a/b", "a\\b"] {
            let mut s = spec(Mode::ReadOnly);
            s.name = bad.into();
            assert!(s.path(base).is_err(), "`{bad}` must be refused");
        }
        let mut ok = spec(Mode::ReadOnly);
        ok.name = "dataset-2024".into();
        assert_eq!(ok.path(base).unwrap(), base.join("dataset-2024"));
    }
}