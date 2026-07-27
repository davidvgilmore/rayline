use std::io::Read as _;

use anyhow::{Context as _, Result, anyhow};
use rayline_mtrouter::{C82Router, argmax_first};
use serde::Serialize;

#[derive(Serialize)]
struct ScoreReport {
    scores: Vec<f32>,
    selected_index: usize,
}

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let runtime = args
        .next()
        .ok_or_else(|| anyhow!("usage: score_embedding <runtime-directory>"))?;
    let previous_arm = args
        .next()
        .map(|value| {
            if value == "none" {
                Ok(None)
            } else {
                value
                    .parse::<usize>()
                    .map(Some)
                    .context("parse previous arm as a non-negative index or `none`")
            }
        })
        .transpose()?
        .flatten();
    let turn_index = args
        .next()
        .map(|value| value.parse::<u64>().context("parse turn index"))
        .transpose()?
        .unwrap_or(0);
    if args.next().is_some() {
        return Err(anyhow!(
            "usage: score_embedding <runtime-directory> [previous-arm|none] [turn-index]"
        ));
    }
    let router = C82Router::load(runtime, "http://127.0.0.1:1", "offline-native")?;
    let mut input = String::new();
    std::io::stdin()
        .read_to_string(&mut input)
        .context("read embedding from stdin")?;
    let embedding = input
        .split_whitespace()
        .map(str::parse::<f32>)
        .collect::<Result<Vec<_>, _>>()
        .context("parse whitespace-delimited embedding")?;
    let scores = router.score_embedding(&embedding, previous_arm, turn_index)?;
    let selected_index =
        argmax_first(&scores).ok_or_else(|| anyhow!("C82 head returned no scores"))?;
    println!(
        "{}",
        serde_json::to_string_pretty(&ScoreReport {
            scores,
            selected_index,
        })?
    );
    Ok(())
}
