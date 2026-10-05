//! Start the real local router against an operator-managed ARC worker.
use anyhow::{Result, anyhow};
use rayline_local_router::{LocalRouterOptions, serve};

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        return Err(anyhow!("usage: arc-router CONFIG.json PORT"));
    }
    serve(LocalRouterOptions {
        config_path: Some(args[1].clone().into()),
        port: args[2].parse()?,
        ..Default::default()
    })
    .await
}
