//! Minimum decision-time WebSocket BBO capture for stock perpetuals.
//!
//! Kraken Futures supplies backed xStock books; Bitget and OKX supply
//! synthetic books. All three collectors run concurrently.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use ordered_float::NotNan;
use scryer_schema::cex_stock_perp_bbo::v2::{Row, SCHEMA_VERSION};
use scryer_schema::meta::Meta;
use serde_json::{json, Value};
use thiserror::Error;
use tokio::time::{timeout_at, Instant};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const KRAKEN_URL: &str = "wss://futures.kraken.com/ws/v1";
const BITGET_URL: &str = "wss://ws.bitget.com/v2/ws/public";
const OKX_URL: &str = "wss://ws.okx.com:8443/ws/v5/public";

#[derive(Debug, Error)]
pub enum WsCaptureError {
    #[error("websocket transport: {0}")]
    Transport(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("malformed websocket payload: {0}")]
    Malformed(String),
}

#[derive(Debug, Default)]
pub struct CaptureResult {
    pub rows: Vec<Row>,
    pub errors: Vec<String>,
}

pub async fn capture(
    underliers: &[String],
    duration: Duration,
    session_id: &str,
    kraken: bool,
    bitget: bool,
    okx: bool,
) -> CaptureResult {
    let upper: Vec<String> = underliers.iter().map(|s| s.to_uppercase()).collect();
    let (kraken_rows, bitget_rows, okx_rows) = tokio::join!(
        enabled_capture(kraken, capture_kraken(&upper, duration, session_id)),
        enabled_capture(bitget, capture_bitget(&upper, duration, session_id)),
        enabled_capture(okx, capture_okx(&upper, duration, session_id)),
    );
    let mut out = CaptureResult::default();
    for (venue, result) in [
        ("kraken_futures", kraken_rows),
        ("bitget", bitget_rows),
        ("okx", okx_rows),
    ] {
        match result {
            Ok(mut rows) => out.rows.append(&mut rows),
            Err(e) => out.errors.push(format!("{venue}: {e}")),
        }
    }
    out
}

async fn enabled_capture<F>(enabled: bool, capture: F) -> Result<Vec<Row>, WsCaptureError>
where
    F: std::future::Future<Output = Result<Vec<Row>, WsCaptureError>>,
{
    if enabled {
        capture.await
    } else {
        Ok(Vec::new())
    }
}

fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_micros() as i64
}

fn parse_f64(v: Option<&Value>, field: &str) -> Result<f64, WsCaptureError> {
    let v = v.ok_or_else(|| WsCaptureError::Malformed(format!("missing {field}")))?;
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        .ok_or_else(|| WsCaptureError::Malformed(format!("invalid {field}: {v}")))
}

fn event_us_from_ms(v: Option<&Value>, fallback_us: i64) -> i64 {
    v.and_then(|x| x.as_i64().or_else(|| x.as_str()?.parse().ok()))
        .map_or(fallback_us, |ms| ms.saturating_mul(1_000))
}

fn base_row(
    exchange: &str,
    exchange_symbol: &str,
    underlier: &str,
    backing_kind: &str,
    session_id: &str,
    event_timestamp_us: i64,
    received_timestamp_us: i64,
    sequence_id: Option<i64>,
    update_kind: &str,
    bid: f64,
    ask: f64,
    bid_size: f64,
    ask_size: f64,
) -> Row {
    Row {
        exchange: exchange.into(),
        exchange_symbol: exchange_symbol.into(),
        underlier_symbol: underlier.into(),
        backing_kind: backing_kind.into(),
        session_id: session_id.into(),
        event_timestamp_us,
        received_timestamp_us,
        sequence_id,
        update_kind: update_kind.into(),
        bid,
        ask,
        bid_size,
        ask_size,
        contract_multiplier: None,
        tick_size: None,
        lot_size: None,
        trading_state: None,
        meta: Meta::new(
            SCHEMA_VERSION,
            received_timestamp_us.div_euclid(1_000_000),
            format!("{exchange}_ws"),
        ),
    }
}

async fn next_text<S>(
    ws: &mut tokio_tungstenite::WebSocketStream<S>,
    deadline: Instant,
) -> Result<Option<String>, WsCaptureError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let next = match timeout_at(deadline, ws.next()).await {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };
        match next {
            Some(Ok(Message::Text(text))) => return Ok(Some(text)),
            Some(Ok(Message::Ping(bytes))) => ws.send(Message::Pong(bytes)).await?,
            Some(Ok(Message::Close(_))) | None => return Ok(None),
            Some(Ok(_)) => {}
            Some(Err(e)) => return Err(e.into()),
        }
    }
}

#[derive(Default)]
struct KrakenBook {
    bids: BTreeMap<NotNan<f64>, f64>,
    asks: BTreeMap<NotNan<f64>, f64>,
}

impl KrakenBook {
    fn update(&mut self, side: &str, price: f64, qty: f64) {
        let Ok(price) = NotNan::new(price) else {
            return;
        };
        let levels = if side == "buy" {
            &mut self.bids
        } else {
            &mut self.asks
        };
        if qty == 0.0 {
            levels.remove(&price);
        } else {
            levels.insert(price, qty);
        }
    }

    fn bbo(&self) -> Option<(f64, f64, f64, f64)> {
        let (bid, bid_size) = self.bids.last_key_value()?;
        let (ask, ask_size) = self.asks.first_key_value()?;
        Some((bid.into_inner(), ask.into_inner(), *bid_size, *ask_size))
    }
}

async fn capture_kraken(
    underliers: &[String],
    duration: Duration,
    session_id: &str,
) -> Result<Vec<Row>, WsCaptureError> {
    let symbols: Vec<String> = underliers.iter().map(|s| format!("PF_{s}XUSD")).collect();
    let symbol_map: HashMap<String, String> = symbols
        .iter()
        .zip(underliers.iter())
        .map(|(a, b)| (a.clone(), b.clone()))
        .collect();
    let (mut ws, _) = connect_async(KRAKEN_URL).await?;
    ws.send(Message::Text(
        json!({"event":"subscribe","feed":"book","product_ids":symbols}).to_string(),
    ))
    .await?;
    let deadline = Instant::now() + duration;
    let mut books: HashMap<String, KrakenBook> = HashMap::new();
    let mut out = Vec::new();
    while let Some(text) = next_text(&mut ws, deadline).await? {
        let received = now_us();
        let v: Value =
            serde_json::from_str(&text).map_err(|e| WsCaptureError::Malformed(e.to_string()))?;
        let Some(feed) = v.get("feed").and_then(Value::as_str) else {
            continue;
        };
        if feed != "book_snapshot" && feed != "book" {
            continue;
        }
        let Some(symbol) = v.get("product_id").and_then(Value::as_str) else {
            continue;
        };
        let Some(underlier) = symbol_map.get(symbol) else {
            continue;
        };
        let book = books.entry(symbol.to_string()).or_default();
        if feed == "book_snapshot" {
            book.bids.clear();
            book.asks.clear();
            for (field, side) in [("bids", "buy"), ("asks", "sell")] {
                if let Some(levels) = v.get(field).and_then(Value::as_array) {
                    for level in levels {
                        if let (Ok(price), Ok(qty)) = (
                            parse_f64(level.get("price"), "price"),
                            parse_f64(level.get("qty"), "qty"),
                        ) {
                            book.update(side, price, qty);
                        }
                    }
                }
            }
        } else if let (Some(side), Ok(price), Ok(qty)) = (
            v.get("side").and_then(Value::as_str),
            parse_f64(v.get("price"), "price"),
            parse_f64(v.get("qty"), "qty"),
        ) {
            book.update(side, price, qty);
        }
        if let Some((bid, ask, bid_size, ask_size)) = book.bbo() {
            out.push(base_row(
                "kraken_futures",
                symbol,
                underlier,
                "backed_xstock",
                session_id,
                event_us_from_ms(v.get("timestamp"), received),
                received,
                v.get("seq").and_then(Value::as_i64),
                if feed == "book_snapshot" {
                    "snapshot"
                } else {
                    "update"
                },
                bid,
                ask,
                bid_size,
                ask_size,
            ));
        }
    }
    Ok(out)
}

async fn capture_bitget(
    underliers: &[String],
    duration: Duration,
    session_id: &str,
) -> Result<Vec<Row>, WsCaptureError> {
    let symbols: Vec<String> = underliers.iter().map(|s| format!("{s}USDT")).collect();
    let symbol_map: HashMap<String, String> = symbols
        .iter()
        .zip(underliers.iter())
        .map(|(a, b)| (a.clone(), b.clone()))
        .collect();
    let args: Vec<Value> = symbols
        .iter()
        .map(|s| json!({"instType":"USDT-FUTURES","channel":"ticker","instId":s}))
        .collect();
    let (mut ws, _) = connect_async(BITGET_URL).await?;
    ws.send(Message::Text(
        json!({"op":"subscribe","args":args}).to_string(),
    ))
    .await?;
    let deadline = Instant::now() + duration;
    let mut out = Vec::new();
    while let Some(text) = next_text(&mut ws, deadline).await? {
        if text == "pong" {
            continue;
        }
        let received = now_us();
        let v: Value =
            serde_json::from_str(&text).map_err(|e| WsCaptureError::Malformed(e.to_string()))?;
        let Some(data) = v.get("data").and_then(Value::as_array) else {
            continue;
        };
        for item in data {
            let symbol = item
                .get("instId")
                .or_else(|| v.pointer("/arg/instId"))
                .and_then(Value::as_str);
            let Some(symbol) = symbol else { continue };
            let Some(underlier) = symbol_map.get(symbol) else {
                continue;
            };
            let (bid, ask, bid_size, ask_size) = (
                parse_f64(item.get("bidPr"), "bidPr")?,
                parse_f64(item.get("askPr"), "askPr")?,
                parse_f64(item.get("bidSz"), "bidSz")?,
                parse_f64(item.get("askSz"), "askSz")?,
            );
            out.push(base_row(
                "bitget",
                symbol,
                underlier,
                "synthetic",
                session_id,
                event_us_from_ms(item.get("ts").or_else(|| v.get("ts")), received),
                received,
                None,
                "snapshot",
                bid,
                ask,
                bid_size,
                ask_size,
            ));
        }
    }
    Ok(out)
}

async fn capture_okx(
    underliers: &[String],
    duration: Duration,
    session_id: &str,
) -> Result<Vec<Row>, WsCaptureError> {
    let symbols: Vec<String> = underliers
        .iter()
        .map(|s| format!("{s}-USDT-SWAP"))
        .collect();
    let symbol_map: HashMap<String, String> = symbols
        .iter()
        .zip(underliers.iter())
        .map(|(a, b)| (a.clone(), b.clone()))
        .collect();
    let args: Vec<Value> = symbols
        .iter()
        .map(|s| json!({"channel":"bbo-tbt","instId":s}))
        .collect();
    let (mut ws, _) = connect_async(OKX_URL).await?;
    ws.send(Message::Text(
        json!({"op":"subscribe","args":args}).to_string(),
    ))
    .await?;
    let deadline = Instant::now() + duration;
    let mut out = Vec::new();
    while let Some(text) = next_text(&mut ws, deadline).await? {
        let received = now_us();
        let v: Value =
            serde_json::from_str(&text).map_err(|e| WsCaptureError::Malformed(e.to_string()))?;
        let Some(symbol) = v.pointer("/arg/instId").and_then(Value::as_str) else {
            continue;
        };
        let Some(underlier) = symbol_map.get(symbol) else {
            continue;
        };
        let Some(data) = v.get("data").and_then(Value::as_array) else {
            continue;
        };
        for item in data {
            let bid_level = item.pointer("/bids/0").and_then(Value::as_array);
            let ask_level = item.pointer("/asks/0").and_then(Value::as_array);
            let (Some(bid_level), Some(ask_level)) = (bid_level, ask_level) else {
                continue;
            };
            out.push(base_row(
                "okx",
                symbol,
                underlier,
                "synthetic",
                session_id,
                event_us_from_ms(item.get("ts"), received),
                received,
                item.get("seqId")
                    .and_then(|x| x.as_i64().or_else(|| x.as_str()?.parse().ok())),
                "snapshot",
                parse_f64(bid_level.first(), "bid")?,
                parse_f64(ask_level.first(), "ask")?,
                parse_f64(bid_level.get(1), "bid_size")?,
                parse_f64(ask_level.get(1), "ask_size")?,
            ));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kraken_book_applies_deltas() {
        let mut b = KrakenBook::default();
        b.update("buy", 100.0, 2.0);
        b.update("buy", 101.0, 3.0);
        b.update("sell", 102.0, 4.0);
        assert_eq!(b.bbo(), Some((101.0, 102.0, 3.0, 4.0)));
        b.update("buy", 101.0, 0.0);
        assert_eq!(b.bbo(), Some((100.0, 102.0, 2.0, 4.0)));
    }

    #[test]
    fn parses_string_and_numeric_prices() {
        assert_eq!(parse_f64(Some(&json!("12.5")), "x").unwrap(), 12.5);
        assert_eq!(parse_f64(Some(&json!(12.5)), "x").unwrap(), 12.5);
    }
}
