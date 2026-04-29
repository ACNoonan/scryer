//! OKX funding-rate-history client.
//!
//! Endpoint: `GET https://www.okx.com/api/v5/public/funding-rate-history`
//!
//! Public, no auth. Documented rate limit: 10 requests / 2 seconds per IP
//! per `instId`. We default to a 250ms inter-request delay which keeps
//! us well under that.
//!
//! Response shape (success):
//! ```json
//! {
//!   "code": "0",
//!   "msg": "",
//!   "data": [
//!     {
//!       "instId": "BTC-USDT-SWAP",
//!       "instType": "SWAP",
//!       "fundingRate": "0.0001",
//!       "realizedRate": "0.0001",
//!       "fundingTime": "1777392000000",
//!       "method": "current_period"
//!     },
//!     ...
//!   ]
//! }
//! ```
//!
//! `realizedRate` is the actual paid rate at `fundingTime`; we prefer it
//! over `fundingRate` (which can be a forecast for the in-progress
//! interval). When both are present they typically match for closed
//! periods.

use scryer_schema::cex_perp_funding_multi::v1::{Rate, SCHEMA_VERSION};
use scryer_schema::Meta;

use crate::{body_head, FetchError, PollConfig};

pub const DEFAULT_BASE_URL: &str = "https://www.okx.com";
pub const SOURCE_LABEL: &str = "okx:funding-rate-history";
/// OKX funding cadence: 8 hours.
pub const FUNDING_PERIOD_SECS: i32 = 28_800;

/// Fetch up to `limit` funding observations for `inst_id`.
///
/// `inst_id` is the OKX instrument code, e.g. `"BTC-USDT-SWAP"`.
/// `symbol` is the canonical short symbol carried into the row, e.g.
/// `"BTC"`. `limit` is capped at 100 by OKX; pagination over older
/// history uses the optional `before` (newer cursor) / `after` (older
/// cursor) ms-timestamp params.
///
/// `before_ms` / `after_ms` are forwarded verbatim when `Some`. For a
/// simple "give me the most recent N" call, leave them both `None`.
pub async fn fetch_funding(
    client: &reqwest::Client,
    cfg: &PollConfig,
    inst_id: &str,
    symbol: &str,
    limit: u32,
    before_ms: Option<i64>,
    after_ms: Option<i64>,
    fetched_at: i64,
) -> Result<Vec<Rate>, FetchError> {
    let url = format!(
        "{}/api/v5/public/funding-rate-history",
        DEFAULT_BASE_URL.trim_end_matches('/')
    );
    let limit_str = limit.to_string();
    let before_str = before_ms.map(|n| n.to_string());
    let after_str = after_ms.map(|n| n.to_string());
    let mut last_err: Option<FetchError> = None;
    for _attempt in 0..cfg.retry_max.max(1) {
        let mut q: Vec<(&str, &str)> =
            vec![("instId", inst_id), ("limit", limit_str.as_str())];
        if let Some(s) = before_str.as_deref() {
            q.push(("before", s));
        }
        if let Some(s) = after_str.as_deref() {
            q.push(("after", s));
        }
        let resp = client.get(&url).query(&q).send().await;
        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                last_err = Some(FetchError::Transport(e));
                tokio::time::sleep(cfg.retry_delay).await;
                continue;
            }
        };
        let status = resp.status().as_u16();
        let text = resp.text().await.map_err(FetchError::Transport)?;
        if status == 429 || status >= 500 {
            tracing::warn!(inst_id, status, "okx transient error; backing off");
            last_err = Some(FetchError::UpstreamStatus {
                status,
                body_head: body_head(&text),
            });
            tokio::time::sleep(cfg.retry_delay).await;
            continue;
        }
        if status >= 400 {
            return Err(FetchError::UpstreamStatus {
                status,
                body_head: body_head(&text),
            });
        }
        return parse_response(&text, inst_id, symbol, fetched_at);
    }
    Err(last_err.unwrap_or_else(|| {
        FetchError::UpstreamError(format!("okx retries exhausted for {inst_id}"))
    }))
}

/// Parse the OKX funding-rate-history JSON body into [`Rate`] rows.
/// Public for unit tests.
pub fn parse_response(
    body: &str,
    inst_id: &str,
    symbol: &str,
    fetched_at: i64,
) -> Result<Vec<Rate>, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    let code = v.get("code").and_then(|c| c.as_str()).unwrap_or("");
    if code != "0" {
        let msg = v
            .get("msg")
            .and_then(|m| m.as_str())
            .unwrap_or("(no msg)");
        return Err(FetchError::UpstreamError(format!(
            "okx code={code} msg={msg}"
        )));
    }
    let data = v
        .get("data")
        .and_then(|d| d.as_array())
        .cloned()
        .unwrap_or_default();
    let mut out: Vec<Rate> = Vec::with_capacity(data.len());
    for entry in data {
        let funding_time_ms = entry
            .get("fundingTime")
            .and_then(|t| t.as_str())
            .and_then(|s| s.parse::<i64>().ok());
        let funding_time_ms = match funding_time_ms {
            Some(t) => t,
            None => continue,
        };
        // Prefer realizedRate (actual paid) over fundingRate (forecast
        // for the in-progress interval).
        let rate_str = entry
            .get("realizedRate")
            .and_then(|r| r.as_str())
            .filter(|s| !s.is_empty())
            .or_else(|| entry.get("fundingRate").and_then(|r| r.as_str()));
        let rate = match rate_str.and_then(|s| s.parse::<f64>().ok()) {
            Some(r) => r,
            None => continue,
        };
        out.push(Rate {
            exchange: "okx".to_string(),
            symbol: symbol.to_string(),
            exchange_symbol: inst_id.to_string(),
            funding_ts: funding_time_ms / 1000,
            funding_rate: rate,
            mark_price: None,
            funding_period_secs: FUNDING_PERIOD_SECS,
            meta: Meta::new(SCHEMA_VERSION, fetched_at, SOURCE_LABEL),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_typical_response() {
        let body = r#"{
            "code": "0",
            "msg": "",
            "data": [
                {"instId":"BTC-USDT-SWAP","instType":"SWAP","fundingRate":"0.0001","realizedRate":"0.00012","fundingTime":"1777392000000","method":"current_period"},
                {"instId":"BTC-USDT-SWAP","instType":"SWAP","fundingRate":"0.0002","realizedRate":"0.00021","fundingTime":"1777363200000","method":"current_period"}
            ]
        }"#;
        let rows = parse_response(body, "BTC-USDT-SWAP", "BTC", 1_777_400_000).expect("parse");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].exchange, "okx");
        assert_eq!(rows[0].symbol, "BTC");
        assert_eq!(rows[0].exchange_symbol, "BTC-USDT-SWAP");
        assert_eq!(rows[0].funding_ts, 1_777_392_000);
        assert_eq!(rows[0].funding_rate, 0.00012);
        assert_eq!(rows[0].funding_period_secs, FUNDING_PERIOD_SECS);
        assert_eq!(rows[0].mark_price, None);
        assert_eq!(rows[0].meta.schema_version, SCHEMA_VERSION);
        assert_eq!(rows[0].meta.fetched_at, 1_777_400_000);
        assert_eq!(rows[0].meta.source, SOURCE_LABEL);
    }

    #[test]
    fn falls_back_to_funding_rate_when_realized_missing() {
        let body = r#"{
            "code": "0",
            "data": [
                {"instId":"BTC-USDT-SWAP","fundingRate":"0.00033","realizedRate":"","fundingTime":"1777392000000"}
            ]
        }"#;
        let rows = parse_response(body, "BTC-USDT-SWAP", "BTC", 1).expect("parse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].funding_rate, 0.00033);
    }

    #[test]
    fn surfaces_error_envelope() {
        let body = r#"{"code":"50011","msg":"Request too frequent","data":[]}"#;
        let err = parse_response(body, "BTC-USDT-SWAP", "BTC", 1).unwrap_err();
        assert!(matches!(err, FetchError::UpstreamError(_)));
    }

    #[test]
    fn skips_rows_with_unparseable_rate() {
        let body = r#"{
            "code": "0",
            "data": [
                {"instId":"BTC-USDT-SWAP","fundingRate":"not-a-number","fundingTime":"1777392000000"},
                {"instId":"BTC-USDT-SWAP","realizedRate":"0.0001","fundingTime":"1777363200000"}
            ]
        }"#;
        let rows = parse_response(body, "BTC-USDT-SWAP", "BTC", 1).expect("parse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].funding_ts, 1_777_363_200);
    }
}

// ============================================================
// stock-perp tape (item 45 / Phase 55)
// ============================================================

use scryer_schema::cex_stock_perp_tape::v1::{Tick, SCHEMA_VERSION as TAPE_SCHEMA_VERSION};

pub const TAPE_SOURCE_LABEL: &str = "okx:tickers+mark-price";

/// OKX stock-perp `instId` shape: `TSLA-USDT-SWAP`. Strip the suffix
/// to recover the canonical underlier.
pub fn underlier_from_inst_id(inst_id: &str) -> Option<String> {
    let s = inst_id.strip_suffix("-USDT-SWAP")?;
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

/// Fetch one OKX stock-perp tape tick for each `(underlier_symbol)`
/// in `underliers`. OKX exposes ticker (last/bid/ask/24h) and mark
/// price on separate endpoints; we make both calls per symbol and
/// merge into one [`Tick`].
pub async fn fetch_tape(
    client: &reqwest::Client,
    cfg: &PollConfig,
    underliers: &[String],
    fetched_at: i64,
) -> Result<Vec<Tick>, FetchError> {
    let mut out = Vec::with_capacity(underliers.len());
    for u in underliers {
        let inst_id = format!("{u}-USDT-SWAP");
        match fetch_one_tick(client, cfg, &inst_id, u, fetched_at).await {
            Ok(Some(t)) => out.push(t),
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(symbol = %u, error = %e, "okx tape fetch skipped");
            }
        }
        if cfg.rate_limit_delay > std::time::Duration::ZERO {
            tokio::time::sleep(cfg.rate_limit_delay).await;
        }
    }
    Ok(out)
}

async fn fetch_one_tick(
    client: &reqwest::Client,
    cfg: &PollConfig,
    inst_id: &str,
    underlier: &str,
    fetched_at: i64,
) -> Result<Option<Tick>, FetchError> {
    let ticker = okx_get(
        client,
        cfg,
        &format!(
            "{}/api/v5/market/ticker",
            DEFAULT_BASE_URL.trim_end_matches('/')
        ),
        &[("instId", inst_id)],
    )
    .await?;
    let mark = okx_get(
        client,
        cfg,
        &format!(
            "{}/api/v5/public/mark-price",
            DEFAULT_BASE_URL.trim_end_matches('/')
        ),
        &[("instType", "SWAP"), ("instId", inst_id)],
    )
    .await?;
    parse_tape_tick(&ticker, &mark, inst_id, underlier, fetched_at)
}

async fn okx_get(
    client: &reqwest::Client,
    cfg: &PollConfig,
    url: &str,
    query: &[(&str, &str)],
) -> Result<serde_json::Value, FetchError> {
    let mut last_err: Option<FetchError> = None;
    for _attempt in 0..cfg.retry_max.max(1) {
        let resp = client.get(url).query(query).send().await;
        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                last_err = Some(FetchError::Transport(e));
                tokio::time::sleep(cfg.retry_delay).await;
                continue;
            }
        };
        let status = resp.status().as_u16();
        let text = resp.text().await.map_err(FetchError::Transport)?;
        if status == 429 || status >= 500 {
            last_err = Some(FetchError::UpstreamStatus {
                status,
                body_head: body_head(&text),
            });
            tokio::time::sleep(cfg.retry_delay).await;
            continue;
        }
        if status >= 400 {
            return Err(FetchError::UpstreamStatus {
                status,
                body_head: body_head(&text),
            });
        }
        let v: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
        let code = v.get("code").and_then(|c| c.as_str()).unwrap_or("");
        if code != "0" {
            let msg = v.get("msg").and_then(|m| m.as_str()).unwrap_or("");
            return Err(FetchError::UpstreamError(format!(
                "okx code={code} msg={msg}"
            )));
        }
        return Ok(v);
    }
    Err(last_err.unwrap_or_else(|| FetchError::UpstreamError("okx retries exhausted".to_string())))
}

/// Merge one OKX `/market/ticker` and `/public/mark-price` response
/// into a single [`Tick`]. Public for unit tests.
pub fn parse_tape_tick(
    ticker_v: &serde_json::Value,
    mark_v: &serde_json::Value,
    inst_id: &str,
    underlier: &str,
    fetched_at: i64,
) -> Result<Option<Tick>, FetchError> {
    let t = ticker_v
        .get("data")
        .and_then(|d| d.as_array())
        .and_then(|a| a.first());
    let m = mark_v
        .get("data")
        .and_then(|d| d.as_array())
        .and_then(|a| a.first());
    let mark = match m
        .and_then(|x| x.get("markPx"))
        .and_then(|s| s.as_str())
        .and_then(|s| s.parse::<f64>().ok())
    {
        Some(p) => p,
        None => return Ok(None),
    };
    let last_price = t
        .and_then(|x| x.get("last"))
        .and_then(|s| s.as_str())
        .and_then(|s| s.parse::<f64>().ok());
    let bid = t
        .and_then(|x| x.get("bidPx"))
        .and_then(|s| s.as_str())
        .and_then(|s| s.parse::<f64>().ok());
    let ask = t
        .and_then(|x| x.get("askPx"))
        .and_then(|s| s.as_str())
        .and_then(|s| s.parse::<f64>().ok());
    let bid_size = t
        .and_then(|x| x.get("bidSz"))
        .and_then(|s| s.as_str())
        .and_then(|s| s.parse::<f64>().ok());
    let ask_size = t
        .and_then(|x| x.get("askSz"))
        .and_then(|s| s.as_str())
        .and_then(|s| s.parse::<f64>().ok());
    let vol_24h = t
        .and_then(|x| x.get("volCcy24h"))
        .and_then(|s| s.as_str())
        .and_then(|s| s.parse::<f64>().ok());
    Ok(Some(Tick {
        exchange: "okx".to_string(),
        exchange_symbol: inst_id.to_string(),
        underlier_symbol: underlier.to_string(),
        backing_kind: "synthetic".to_string(),
        ts: fetched_at,
        mark_price: mark,
        index_price: None,
        last_price,
        bid,
        ask,
        bid_size,
        ask_size,
        funding_rate: None,
        funding_prediction: None,
        open_interest: None,
        vol_24h,
        suspended: None,
        meta: scryer_schema::Meta::new(TAPE_SCHEMA_VERSION, fetched_at, TAPE_SOURCE_LABEL),
    }))
}

#[cfg(test)]
mod tape_tests {
    use super::*;

    #[test]
    fn underlier_from_inst_id_strips_suffix() {
        assert_eq!(underlier_from_inst_id("TSLA-USDT-SWAP"), Some("TSLA".to_string()));
        assert_eq!(underlier_from_inst_id("BTC-USDT-SWAP"), Some("BTC".to_string()));
        assert_eq!(underlier_from_inst_id("BTC-PERP"), None);
        assert_eq!(underlier_from_inst_id("-USDT-SWAP"), None);
    }

    #[test]
    fn parses_tape_tick_from_separate_responses() {
        let ticker = serde_json::from_str(r#"{"code":"0","msg":"","data":[
            {"instType":"SWAP","instId":"TSLA-USDT-SWAP","last":"377.64","askPx":"377.65","askSz":"2.86","bidPx":"377.64","bidSz":"29.79","vol24h":"18006.56","volCcy24h":"18006.56","ts":"1777431864210"}
        ]}"#).unwrap();
        let mark = serde_json::from_str(r#"{"code":"0","msg":"","data":[
            {"instId":"TSLA-USDT-SWAP","instType":"SWAP","markPx":"377.64","ts":"1777431865591"}
        ]}"#).unwrap();
        let tick = parse_tape_tick(&ticker, &mark, "TSLA-USDT-SWAP", "TSLA", 1_777_400_000)
            .expect("parse")
            .expect("non-empty");
        assert_eq!(tick.exchange, "okx");
        assert_eq!(tick.underlier_symbol, "TSLA");
        assert_eq!(tick.backing_kind, "synthetic");
        assert_eq!(tick.mark_price, 377.64);
        assert_eq!(tick.last_price, Some(377.64));
        assert_eq!(tick.bid, Some(377.64));
        assert_eq!(tick.ask, Some(377.65));
        assert_eq!(tick.index_price, None);
    }

    #[test]
    fn missing_mark_price_returns_none() {
        let ticker = serde_json::from_str(r#"{"code":"0","data":[]}"#).unwrap();
        let mark = serde_json::from_str(r#"{"code":"0","data":[]}"#).unwrap();
        let tick = parse_tape_tick(&ticker, &mark, "X", "X", 1).expect("parse");
        assert!(tick.is_none());
    }
}

// ============================================================
// 1m OHLCV (companion forward tape, item 45 §1.2 / Phase 56)
// ============================================================

use scryer_schema::cex_stock_perp_ohlcv::v1::{Bar, SCHEMA_VERSION as OHLCV_SCHEMA_VERSION};

pub const OHLCV_SOURCE_LABEL: &str = "okx:candles";

/// Fetch 1m OHLCV bars for one OKX stock-perp.
///
/// Endpoint: `GET /api/v5/market/candles?bar=1m&instId={SYM}&limit={N}`.
/// Returns up to 300 bars per call (most-recent first). For deeper
/// history use `before`/`after` cursors (deferred to v2).
pub async fn fetch_ohlcv(
    client: &reqwest::Client,
    cfg: &PollConfig,
    inst_id: &str,
    underlier: &str,
    limit: u32,
    fetched_at: i64,
) -> Result<Vec<Bar>, FetchError> {
    let url = format!(
        "{}/api/v5/market/candles",
        DEFAULT_BASE_URL.trim_end_matches('/')
    );
    let limit_str = limit.to_string();
    let mut last_err: Option<FetchError> = None;
    for _attempt in 0..cfg.retry_max.max(1) {
        let resp = client
            .get(&url)
            .query(&[
                ("instId", inst_id),
                ("bar", "1m"),
                ("limit", limit_str.as_str()),
            ])
            .send()
            .await;
        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                last_err = Some(FetchError::Transport(e));
                tokio::time::sleep(cfg.retry_delay).await;
                continue;
            }
        };
        let status = resp.status().as_u16();
        let text = resp.text().await.map_err(FetchError::Transport)?;
        if status == 429 || status >= 500 {
            last_err = Some(FetchError::UpstreamStatus {
                status,
                body_head: body_head(&text),
            });
            tokio::time::sleep(cfg.retry_delay).await;
            continue;
        }
        if status >= 400 {
            return Err(FetchError::UpstreamStatus {
                status,
                body_head: body_head(&text),
            });
        }
        return parse_ohlcv_response(&text, inst_id, underlier, fetched_at);
    }
    Err(last_err.unwrap_or_else(|| {
        FetchError::UpstreamError(format!("okx ohlcv retries exhausted for {inst_id}"))
    }))
}

/// Parse OKX `/market/candles` body. Tuple shape:
/// `[ts_ms_str, open, high, low, close, vol_base, vol_ccy_quote, vol_ccy_quote2, confirm]`.
/// Public for tests.
pub fn parse_ohlcv_response(
    body: &str,
    inst_id: &str,
    underlier: &str,
    fetched_at: i64,
) -> Result<Vec<Bar>, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    let code = v.get("code").and_then(|c| c.as_str()).unwrap_or("");
    if code != "0" {
        let msg = v.get("msg").and_then(|m| m.as_str()).unwrap_or("");
        return Err(FetchError::UpstreamError(format!(
            "okx code={code} msg={msg}"
        )));
    }
    let arr = v
        .get("data")
        .and_then(|d| d.as_array())
        .cloned()
        .unwrap_or_default();
    let mut out: Vec<Bar> = Vec::with_capacity(arr.len());
    for entry in arr {
        let tup = match entry.as_array() {
            Some(t) if t.len() >= 9 => t,
            _ => continue,
        };
        let ts_ms = match tup[0].as_str().and_then(|s| s.parse::<i64>().ok()) {
            Some(t) => t,
            None => continue,
        };
        let o = parse_str_f64_okx(&tup[1]);
        let h = parse_str_f64_okx(&tup[2]);
        let l = parse_str_f64_okx(&tup[3]);
        let c = parse_str_f64_okx(&tup[4]);
        let vol_base = parse_str_f64_okx(&tup[5]);
        // tup[7] is volCcyQuote (USD-quoted notional, OKX docs).
        let vol_quote = parse_str_f64_okx(&tup[7]);
        let (o, h, l, c, vol_base) = match (o, h, l, c, vol_base) {
            (Some(o), Some(h), Some(l), Some(c), Some(v)) => (o, h, l, c, v),
            _ => continue,
        };
        let bar_open_ts = ts_ms / 1000;
        out.push(Bar {
            exchange: "okx".to_string(),
            exchange_symbol: inst_id.to_string(),
            underlier_symbol: underlier.to_string(),
            backing_kind: "synthetic".to_string(),
            bar_open_ts,
            bar_close_ts: bar_open_ts + 60,
            open: o,
            high: h,
            low: l,
            close: c,
            volume_base: vol_base,
            volume_quote: vol_quote,
            trade_count: None,
            meta: scryer_schema::Meta::new(OHLCV_SCHEMA_VERSION, fetched_at, OHLCV_SOURCE_LABEL),
        });
    }
    Ok(out)
}

fn parse_str_f64_okx(v: &serde_json::Value) -> Option<f64> {
    v.as_str().and_then(|s| s.parse::<f64>().ok())
}

#[cfg(test)]
mod ohlcv_tests {
    use super::*;

    #[test]
    fn parses_typical_okx_candles() {
        // OKX returns most-recent-first.
        let body = r#"{"code":"0","msg":"","data":[
            ["1777433040000","378.31","378.31","378.31","378.31","0.03","0.03","11.3493","0"],
            ["1777432980000","378.2","378.3","378.2","378.26","6.07","6.07","2296.0212","1"]
        ]}"#;
        let rows =
            parse_ohlcv_response(body, "TSLA-USDT-SWAP", "TSLA", 1_777_400_000).expect("parse");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].exchange, "okx");
        assert_eq!(rows[0].underlier_symbol, "TSLA");
        assert_eq!(rows[0].backing_kind, "synthetic");
        assert_eq!(rows[0].bar_open_ts, 1_777_433_040);
        assert_eq!(rows[0].bar_close_ts, 1_777_433_100);
        assert_eq!(rows[0].open, 378.31);
        assert_eq!(rows[0].volume_base, 0.03);
        assert_eq!(rows[0].volume_quote, Some(11.3493));
    }

    #[test]
    fn surfaces_error_envelope() {
        let body = r#"{"code":"50011","msg":"throttle"}"#;
        let err = parse_ohlcv_response(body, "X", "X", 1).unwrap_err();
        assert!(matches!(err, FetchError::UpstreamError(_)));
    }

    #[test]
    fn skips_truncated_tuples() {
        let body = r#"{"code":"0","data":[
            ["1","2","3"],
            ["1777432980000","378.2","378.3","378.2","378.26","6.07","6.07","2296.0212","1"]
        ]}"#;
        let rows = parse_ohlcv_response(body, "X", "X", 1).expect("parse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].bar_open_ts, 1_777_432_980);
    }
}
