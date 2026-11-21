//! `effector-launch` — the privileged half of ADR-0003 §10.
//!
//! It mints a token for the alias, starts the node as a container's PID 1, writes the token to
//! that process's stdin, and waits. It never runs delivered code and never holds a credential of
//! its own beyond the mint it just performed.

use std::io::Write;
use std::process::{Command, Stdio};

use effector_launcher::{docker_argv, mint_token, mount_url};

struct Args {
    alias: String,
    image: String,
    control_plane: String,
    dry_run: bool,
    extra: Vec<String>,
}

fn main() -> Result<(), String> {
    let args = parse(std::env::args().skip(1))?;
    let mount = mount_url(&args.control_plane, &args.alias)?;

    if args.dry_run {
        // The dry run goes through the SAME argv builder the real path uses, so a printed
        // command cannot drift from the one that runs.
        println!(
            "{}",
            docker_argv(&args.image, &mount, &args.extra).join(" ")
        );
        return Ok(());
    }

    let minted = mint_token(&args.control_plane, &args.alias)?;
    let argv = docker_argv(&args.image, &mount, &args.extra);

    let mut child = Command::new(&argv[0])
        .args(&argv[1..])
        .stdin(Stdio::piped())
        .spawn()
        .map_err(|e| format!("{}: {e}", argv[0]))?;

    // One line, then close: the node reads it once at startup, and nothing in the container —
    // not the guest, not `ps`, not `/proc/self/environ` — ever held it.
    {
        let stdin = child
            .stdin
            .as_mut()
            .ok_or_else(|| "the node's stdin is not piped".to_string())?;
        writeln!(stdin, "{}", minted.token).map_err(|e| format!("writing the token: {e}"))?;
        stdin
            .flush()
            .map_err(|e| format!("writing the token: {e}"))?;
    }
    drop(child.stdin.take());

    let status = child
        .wait()
        .map_err(|e| format!("waiting for the node: {e}"))?;
    if !status.success() {
        return Err(format!("the node left with {status}"));
    }
    Ok(())
}

const USAGE: &str = "\
effector-launch — start one effector node as a container's PID 1

  effector-launch --alias <alias> --image <image> --control-plane <http://host:port>
                  [--dry-run] [-- <extra docker run args>…]

  --alias           the node's alias: what the control plane mints a token for, and the last
                    segment of the mount path the node dials
  --image           the image that ships `effector` on PATH and its own effector.json (the
                    shape: alias, carriers, lease). The launcher supplies only where to dial
  --control-plane   the control plane's base URL. The admin surface (POST /admin/nodes) and the
                    mount are both derived from it; plain http, so keep it on loopback or a
                    private network
  --dry-run         print the docker command and exit; no token is minted
  --                everything after it goes to `docker run` verbatim (--network, -v, --device…)

What it deliberately does NOT do: create the container itself (no create/attach/exec/destroy —
`docker run --rm --init` owns the lifecycle), verify the node (the control plane decides at the
dial), or hold the token (it writes it to the node's stdin and drops it).";

fn parse(mut argv: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut alias = None;
    let mut image = None;
    let mut control_plane = None;
    let mut dry_run = false;
    let mut extra = Vec::new();

    while let Some(arg) = argv.next() {
        match arg.as_str() {
            "-h" | "--help" | "help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            "--alias" => alias = Some(next(&mut argv, "--alias")?),
            "--image" => image = Some(next(&mut argv, "--image")?),
            "--control-plane" => control_plane = Some(next(&mut argv, "--control-plane")?),
            "--dry-run" => dry_run = true,
            "--" => {
                extra.extend(argv);
                break;
            }
            other => return Err(format!("unknown argument `{other}` (see --help)")),
        }
    }

    Ok(Args {
        alias: alias.ok_or("--alias is required")?,
        image: image.ok_or("--image is required")?,
        control_plane: control_plane.ok_or("--control-plane is required")?,
        dry_run,
        extra,
    })
}

fn next(argv: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    argv.next().ok_or_else(|| format!("{flag} needs a value"))
}
