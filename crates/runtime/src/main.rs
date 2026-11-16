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
    if let Some(t) = token {
        config.token = t;
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

A node's authority is the TOKEN it dials with. The control plane mints one per alias, the node
attaches it to the mount URL, and the connection that results authorizes every call that
follows — there is no challenge, no keypair and no `keygen`. Revoking a node means minting a
new token; a node that is a container is handed its token at launch and keeps nothing else.
";

fn load(path: &str) -> anyhow::Result<EffectorConfig> {
    let raw = std::fs::read_to_string(path)?;
    serde_json::from_str(&raw).map_err(|e| anyhow::anyhow!("effector config {path}: {e}"))
}