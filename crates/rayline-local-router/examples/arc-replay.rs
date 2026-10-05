//! Replay private exact decide envelopes without storing them in this repository.
use anyhow::{Result, anyhow};
use rayline_local_router::RouterConfig;
use serde_json::Value;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        return Err(anyhow!("usage: arc-replay CONFIG.json REQUESTS.jsonl"));
    }
    let config: RouterConfig = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    let arc = config
        .arc
        .ok_or_else(|| anyhow!("config must contain arc"))?;
    for line in std::fs::read_to_string(&args[2])?
        .lines()
        .filter(|s| !s.trim().is_empty())
    {
        let request: Value = serde_json::from_str(line)?;
        let result = arc.decide(&request).await?;
        println!("{}", serde_json::to_string(&result)?);
    }
    Ok(())
}
