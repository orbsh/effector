//! The effector binary: `keygen`, `register`, or (no subcommand) load config and serve.
//!
//! Subcommands are CLI form only — the library keeps the one entry point it had, so an
//! embedded deployment (aura's local mode) is unaffected by how a node operator drives this
//! binary. A config path as the first argument still works; `keygen` and `register` are
//! reserved words and are documented as such.

use effector_config::EffectorConfig;

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("keygen") => keygen(args.collect()),
        Some("register") => register(args.collect()),
        Some("-h" | "--help" | "help") => {
            print!("{USAGE}");
            Ok(())
        }
        other => serve(other),
    }
}

const USAGE: &str = "\
effector — a node's dial-in agent

  effector [config.json]              load the config and serve (default: ./effector.json)
  effector keygen [--out <file>]      make an ed25519 keypair; prints the public key
  effector register <config.json>     one handshake against the control plane, then exit

The control plane challenges every connection and verifies the signature against the public
key IT has registered for the alias; `register` is therefore how an operator learns what
verdict their node gets without leaving it running.
";

/// `keygen`: a private key (0600, never replaced) and the public key to register.
fn keygen(args: Vec<String>) -> anyhow::Result<()> {
    let mut out = None;
    let mut it = args.into_iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--out" | "-o" => {
                out = Some(it.next().ok_or_else(|| anyhow::anyhow!("--out needs a path"))?)
            }
            other => anyhow::bail!("keygen: unknown argument `{other}`"),
        }
    }
    // The default lives under XDG_CONFIG_HOME because a key is per-user state, not per-cwd
    // state: a key next to whatever directory the operator happened to be in is a key nobody
    // finds again.
    let path = match out {
        Some(p) => std::path::PathBuf::from(p),
        None => {
            let base = std::env::var_os("XDG_CONFIG_HOME")
                .map(std::path::PathBuf::from)
                .or_else(|| std::env::var_os("HOME").map(|h| std::path::PathBuf::from(h).join(".config")))
                .ok_or_else(|| anyhow::anyhow!("no HOME and no XDG_CONFIG_HOME: pass --out"))?;
            base.join("effector").join("node.key")
        }
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let public_key = effector_protocol::write_keypair(&path)
        .map_err(|e| anyhow::anyhow!("writing {}: {e}", path.display()))?;
    println!("private key  {}", path.display());
    println!("public key   {public_key}");
    println!("next         register the public key for this node's alias, then `effector register <config>`");
    Ok(())
}

/// `register`: one connection, one verdict, exit. Registration is a network FACT (the control
/// plane's registry) — serving is runtime state, and a restart re-earns trust per connection.
fn register(args: Vec<String>) -> anyhow::Result<()> {
    let path = args
        .into_iter()
        .next()
        .unwrap_or_else(|| "effector.json".into());
    let config = load(&path)?;
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        match effector_runtime::remote::register_once(&config).await {
            Ok(()) => {
                println!("registered: the control plane accepted this node's key for `{}`", config.capabilities.node_alias);
                Ok(())
            }
            Err(e) => Err(anyhow::anyhow!("{e:#}")),
        }
    })
}

fn serve(path: Option<&str>) -> anyhow::Result<()> {
    // Single-node simplicity: no discovery layer, the operator points the effector at its
    // control plane.
    let path = path.map(str::to_string).unwrap_or_else(|| "effector.json".into());
    let config = load(&path)?;
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(effector_runtime::remote::run(config))
}

fn load(path: &str) -> anyhow::Result<EffectorConfig> {
    let raw = std::fs::read_to_string(path)?;
    serde_json::from_str(&raw).map_err(|e| anyhow::anyhow!("effector config {path}: {e}"))
}
