//! The effector binary: load config and serve, or one handshake and exit.
//!
//! Subcommands are CLI form only — the library keeps the one entry point it had, so an
//! embedded deployment (aura's local mode) is unaffected by how a node operator drives this
//! binary. `--control-plane` and `--token` exist for the container form (ADR-0003), where the
//! launcher supplies where to dial and the authority to dial with, and nothing else.

use effector_config::EffectorConfig;

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let mut config_path: Option<String> = None;
    let mut control_plane: Option<String> = None;
    let mut token: Option<String> = None;
    let mut token_from_stdin = false;
    let mut idle_secs: Option<u64> = None;
    let mut exit_on_disconnect = false;
    let mut once = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" | "help" => {
                print!("{USAGE}");
                return Ok(());
            }
            // `effector register [config]`: one handshake, print the verdict, exit.
            "register" if config_path.is_none() && !once => once = true,
            "--control-plane" => {
                control_plane = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--control-plane needs a ws:// url"))?,
                );
            }
            "--token" => {
                token = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--token needs a value"))?,
                );
            }
            // A launcher hands over the token this way: `/proc/<pid>/cmdline` is readable by
            // every process of the same uid, so argv is not a place for a secret, and the
            // node's environment is inherited by the guest it spawns, so env is not one
            // either. stdin belongs to the process alone.
            "--token-stdin" => token_from_stdin = true,
            // The grace period, for the container form: the node's lifetime IS the container's, so
            // "how long do I wait for the task's next call" is a launch fact, not a file edit.
            "--idle-secs" => {
                idle_secs = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--idle-secs needs a number"))?
                        .parse::<u64>()
                        .map_err(|e| anyhow::anyhow!("--idle-secs: {e}"))?,
                );
            }
            // A node that IS the container has no second life: when the connection ends there is
            // nothing left to serve it, so it leaves instead of reconnecting (ADR-0003 §11).
            "--exit-on-disconnect" => exit_on_disconnect = true,
            other if other.starts_with('-') => {
                anyhow::bail!("unknown argument `{other}` (see `effector --help`)")
            }
            path if config_path.is_none() => config_path = Some(path.to_string()),
            other => anyhow::bail!("unexpected argument `{other}`"),
        }
    }

    let path = config_path.unwrap_or_else(|| "effector.json".into());
    let mut config = load(&path)?;
    // Command line beats the file: a launcher hands a node its dial target and its token, and
    // the file carries the shape (alias, carriers) the image ships with.
    if let Some(url) = control_plane {
        config.control_plane_url = url;
    }
    if token.is_some() && token_from_stdin {
        anyhow::bail!("--token and --token-stdin are one choice, not two");
    }
    if let Some(t) = token {
        config.token = t;
    }
    if token_from_stdin {
        let mut line = String::new();
        std::io::stdin().read_line(&mut line)?;
        config.token = take_token_line(&line).map_err(anyhow::Error::msg)?;
    }
    if let Some(secs) = idle_secs {
        config.lease.idle_secs = secs;
    }
    if exit_on_disconnect {
        config.lease.exit_on_disconnect = true;
    }

    let runtime = tokio::runtime::Runtime::new()?;
    if once {
        runtime.block_on(async {
            match effector_runtime::remote::register_once(&config).await {
                Ok(()) => {
                    println!(
                        "accepted: the control plane let this node in as `{}`",
                        config.capabilities.node_alias
                    );
                    Ok(())
                }
                Err(e) => Err(anyhow::anyhow!("{e:#}")),
            }
        })
    } else {
        runtime.block_on(effector_runtime::remote::run(config))
    }
}

const USAGE: &str = "\
effector — a node's dial-in agent

  effector [config.json]                 load the config and serve (default: ./effector.json)
  effector register [config.json]        one handshake against the control plane, then exit
  effector [config.json] --control-plane <ws://url> --token <token>
                                         launcher overrides (the container form, ADR-0003)
  effector [config.json] --control-plane <ws://url> --token-stdin [--idle-secs N] \
      [--exit-on-disconnect]
                                         the token arrives on stdin, one line — the form to use
                                         when a launcher spawns the node: argv is world-readable
                                         and the environment is inherited by the guest

A node's authority is the TOKEN it dials with. The control plane mints one per alias, the node
attaches it to the mount URL, and the connection that results authorizes every call that
follows — there is no challenge, no keypair and no `keygen`. Revoking a node means minting a
new token; a node that is a container is handed its token at launch and keeps nothing else.
";

/// One line of stdin is the token. A launcher that had nothing to say must not look like a
/// launcher that said nothing useful: both are a startup failure, named at the door.
fn take_token_line(line: &str) -> Result<String, String> {
    let token = line.trim();
    if token.is_empty() {
        return Err(
            "--token-stdin: stdin closed before a token arrived (the launcher writes it, one line)"
                .into(),
        );
    }
    Ok(token.to_string())
}

fn load(path: &str) -> anyhow::Result<EffectorConfig> {
    let raw = std::fs::read_to_string(path)?;
    serde_json::from_str(&raw).map_err(|e| anyhow::anyhow!("effector config {path}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::take_token_line;

    #[test]
    #[allow(clippy::unwrap_used, clippy::expect_used)]
    fn the_token_line_is_trimmed_and_empty_is_a_refusal() {
        assert_eq!(take_token_line("abc\n").unwrap(), "abc");
        assert_eq!(take_token_line("  abc  ").unwrap(), "abc");
        assert!(take_token_line("\n").unwrap_err().contains("stdin closed"));
        assert!(take_token_line("").unwrap_err().contains("stdin closed"));
    }
}