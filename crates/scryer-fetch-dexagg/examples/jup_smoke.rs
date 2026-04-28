//! Live smoke test: hit Jupiter for 8 xStocks, print bid/ask/mid/spread.
//! Usage: `cargo run -p scryer-fetch-dexagg --example jup_smoke`
use scryer_fetch_dexagg::jupiter::{xstock_two_sided_mid_usdc, JupiterConfig, XSTOCK_MINTS};

#[tokio::main]
async fn main() {
    let cfg = JupiterConfig::default();
    let client = reqwest::Client::builder().build().unwrap();
    for (sym, _mint) in XSTOCK_MINTS {
        match xstock_two_sided_mid_usdc(&client, &cfg, sym, 1.0).await {
            Ok((bid, ask, mid)) => {
                let spread_bp = (ask / bid).ln() * 1e4;
                println!(
                    "{sym:7}  bid=${bid:8.3}  ask=${ask:8.3}  mid=${mid:8.3}  spread={spread_bp:6.1}bp"
                );
            }
            Err(e) => println!("{sym:7}  ERROR: {e}"),
        }
    }
}
