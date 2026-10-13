//! The effector binary: load config, run the remote wrapper (Phase 3).

use effector_config::EffectorConfig;

fn main() -> anyhow::Result<()> {
    // Config path: argv[1] or ./effector.json. Single-node simplicity: no
    // discovery layer, the operator points the effector at its control plane.
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "effector.json".into());
    let raw = std::fs::read_to_string(&path)?;
    let config: EffectorConfig =
        serde_json::from_str(&raw).map_err(|e| anyhow::anyhow!("effector config {path}: {e}"))?;
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(effector_runtime::remote::run(config))
}
