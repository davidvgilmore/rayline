use anyhow::{Result, anyhow};
use rayline_mtrouter::C82Router;

fn main() -> Result<()> {
    let runtime = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow!("usage: verify_runtime <runtime-directory>"))?;
    let router = C82Router::load(runtime, "http://127.0.0.1:1", "offline-golden")?;
    println!(
        "{}",
        serde_json::to_string_pretty(&router.verify_head_golden()?)?
    );
    Ok(())
}
