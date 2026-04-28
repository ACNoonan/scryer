//! Nasdaq trade-halts RSS feed.
//!
//! Endpoint: `https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts`
//!
//! Format: RSS 2.0 with a custom `ndaq:` namespace carrying typed
//! halt fields. Each `<item>` becomes one
//! [`scryer_schema::nasdaq_halts::v1::Halt`] row.

use scryer_schema::nasdaq_halts::v1::Halt;
use scryer_schema::Meta;

use crate::{parse_us_date_to_date32, FetchError};

pub const DEFAULT_FEED_URL: &str = "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts";

/// Parse the RSS body into [`Halt`] rows. Public so tests can drive
/// it directly with hardcoded XML strings.
pub fn parse_feed(body: &str, poll_unix_micros: i64, meta: &Meta) -> Result<Vec<Halt>, FetchError> {
    let mut reader = quick_xml::Reader::from_str(body);
    reader.config_mut().trim_text(true);

    let mut out: Vec<Halt> = Vec::new();
    let mut buf = Vec::new();
    let mut in_item = false;
    let mut current: Option<HaltAccumulator> = None;
    let mut current_tag: Option<String> = None;

    loop {
        match reader.read_event_into(&mut buf) {
            Err(e) => return Err(FetchError::XmlParse(format!("read: {e}"))),
            Ok(quick_xml::events::Event::Eof) => break,
            Ok(quick_xml::events::Event::Start(e)) => {
                let name = local_name(e.name().as_ref());
                if name == "item" {
                    in_item = true;
                    current = Some(HaltAccumulator::default());
                    continue;
                }
                if in_item {
                    current_tag = Some(name);
                }
            }
            Ok(quick_xml::events::Event::Empty(e)) => {
                // Self-closing tag like `<ndaq:ResumptionDate/>` —
                // emit empty value into the accumulator.
                if in_item {
                    let name = local_name(e.name().as_ref());
                    if let Some(acc) = current.as_mut() {
                        assign_field(acc, &name, "");
                    }
                }
            }
            Ok(quick_xml::events::Event::End(e)) => {
                let name = local_name(e.name().as_ref());
                if name == "item" {
                    if let Some(acc) = current.take() {
                        match acc.finalize(poll_unix_micros, meta) {
                            Ok(Some(row)) => out.push(row),
                            Ok(None) => {
                                tracing::debug!(
                                    "nasdaq halt item missing required fields, skipping"
                                );
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "nasdaq halt parse error");
                            }
                        }
                    }
                    in_item = false;
                    current_tag = None;
                    continue;
                }
                current_tag = None;
            }
            Ok(quick_xml::events::Event::Text(t)) => {
                if in_item {
                    let text = match t.unescape() {
                        Ok(s) => s.to_string(),
                        Err(_) => continue,
                    };
                    if let Some(tag) = current_tag.as_ref() {
                        if let Some(acc) = current.as_mut() {
                            assign_field(acc, tag, &text);
                        }
                    }
                }
            }
            Ok(quick_xml::events::Event::CData(t)) => {
                // CDATA body — Nasdaq sometimes wraps free-form text.
                if in_item {
                    let text = String::from_utf8_lossy(&t.into_inner()).to_string();
                    if let Some(tag) = current_tag.as_ref() {
                        if let Some(acc) = current.as_mut() {
                            assign_field(acc, tag, &text);
                        }
                    }
                }
            }
            _ => {}
        }
        buf.clear();
    }
    Ok(out)
}

#[derive(Debug, Default)]
struct HaltAccumulator {
    halt_date: String,
    halt_time: String,
    issue_symbol: String,
    issue_name: String,
    market_category: String,
    reason_code: String,
    pause_threshold_price: String,
    resumption_date: String,
    resumption_quote_time: String,
    resumption_trade_time: String,
}

fn assign_field(acc: &mut HaltAccumulator, tag: &str, text: &str) {
    let value = text.trim().to_string();
    if value.is_empty() {
        return;
    }
    match tag {
        "HaltDate" => acc.halt_date = value,
        "HaltTime" => acc.halt_time = value,
        "IssueSymbol" => acc.issue_symbol = value,
        "IssueName" => acc.issue_name = value,
        "MarketCategory" => acc.market_category = value,
        "ReasonCode" => acc.reason_code = value,
        "PauseThresholdPrice" => acc.pause_threshold_price = value,
        "ResumptionDate" => acc.resumption_date = value,
        "ResumptionQuoteTime" => acc.resumption_quote_time = value,
        "ResumptionTradeTime" => acc.resumption_trade_time = value,
        _ => {}
    }
}

/// Strip an `ns:tag` prefix to `tag` (UTF-8 lossy). Returns the
/// local-name as `String`.
fn local_name(qname: &[u8]) -> String {
    let s = std::str::from_utf8(qname).unwrap_or("");
    match s.find(':') {
        Some(idx) => s[idx + 1..].to_string(),
        None => s.to_string(),
    }
}

impl HaltAccumulator {
    fn finalize(self, poll_ts: i64, meta: &Meta) -> Result<Option<Halt>, FetchError> {
        // Required: HaltDate, HaltTime, IssueSymbol.
        if self.halt_date.is_empty() || self.halt_time.is_empty() || self.issue_symbol.is_empty() {
            return Ok(None);
        }
        let halt_date = parse_us_date_to_date32(&self.halt_date)?;
        let pause_threshold_price = self
            .pause_threshold_price
            .parse::<f64>()
            .ok()
            .filter(|v| !v.is_nan());
        let resumption_date = if self.resumption_date.is_empty() {
            None
        } else {
            parse_us_date_to_date32(&self.resumption_date).ok()
        };
        let resumption_quote_time = if self.resumption_quote_time.is_empty() {
            None
        } else {
            Some(self.resumption_quote_time.clone())
        };
        let resumption_trade_time = if self.resumption_trade_time.is_empty() {
            None
        } else {
            Some(self.resumption_trade_time.clone())
        };
        // raw_xml is left empty — the typed columns capture every
        // documented Nasdaq RSS field; future schema additions can
        // re-derive from a fresh poll if needed.
        Ok(Some(Halt {
            poll_ts,
            halt_date,
            halt_time: self.halt_time,
            underlying: self.issue_symbol,
            issue_name: self.issue_name,
            market_category: self.market_category,
            reason_code: self.reason_code,
            pause_threshold_price,
            resumption_date,
            resumption_quote_time,
            resumption_trade_time,
            raw_xml: String::new(),
            meta: meta.clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> Meta {
        Meta::new(
            scryer_schema::nasdaq_halts::v1::SCHEMA_VERSION,
            1_777_300_000,
            "nasdaq:rss",
        )
    }

    #[test]
    fn parses_two_active_halts() {
        let body = r#"<?xml version="1.0"?>
<rss xmlns:ndaq="http://www.nasdaqtrader.com/" version="2.0">
<channel>
<item>
  <ndaq:HaltDate>04/24/2026</ndaq:HaltDate>
  <ndaq:HaltTime>19:50:00.000</ndaq:HaltTime>
  <ndaq:IssueSymbol>AAPL</ndaq:IssueSymbol>
  <ndaq:IssueName>Apple Inc.</ndaq:IssueName>
  <ndaq:MarketCategory>NASDAQ</ndaq:MarketCategory>
  <ndaq:ReasonCode>T1</ndaq:ReasonCode>
  <ndaq:PauseThresholdPrice/>
  <ndaq:ResumptionDate/>
  <ndaq:ResumptionQuoteTime/>
  <ndaq:ResumptionTradeTime/>
</item>
<item>
  <ndaq:HaltDate>04/24/2026</ndaq:HaltDate>
  <ndaq:HaltTime>20:15:00.000</ndaq:HaltTime>
  <ndaq:IssueSymbol>NVDA</ndaq:IssueSymbol>
  <ndaq:IssueName>NVIDIA Corp.</ndaq:IssueName>
  <ndaq:MarketCategory>NASDAQ</ndaq:MarketCategory>
  <ndaq:ReasonCode>M</ndaq:ReasonCode>
  <ndaq:PauseThresholdPrice>123.45</ndaq:PauseThresholdPrice>
  <ndaq:ResumptionDate/>
  <ndaq:ResumptionQuoteTime/>
  <ndaq:ResumptionTradeTime/>
</item>
</channel></rss>"#;
        let rows = parse_feed(body, 1_777_300_000_000_000, &meta()).expect("parse");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].underlying, "AAPL");
        assert_eq!(rows[0].halt_date, 20_567); // 2026-04-24
        assert_eq!(rows[0].halt_time, "19:50:00.000");
        assert_eq!(rows[0].reason_code, "T1");
        assert_eq!(rows[0].pause_threshold_price, None);
        assert_eq!(rows[0].resumption_date, None);
        // Second halt with pause threshold.
        assert_eq!(rows[1].underlying, "NVDA");
        assert!((rows[1].pause_threshold_price.unwrap() - 123.45).abs() < 1e-9);
    }

    #[test]
    fn parses_resumed_halt() {
        let body = r#"<?xml version="1.0"?>
<rss xmlns:ndaq="http://www.nasdaqtrader.com/" version="2.0">
<channel>
<item>
  <ndaq:HaltDate>04/24/2026</ndaq:HaltDate>
  <ndaq:HaltTime>19:50:00.000</ndaq:HaltTime>
  <ndaq:IssueSymbol>AAPL</ndaq:IssueSymbol>
  <ndaq:IssueName>Apple Inc.</ndaq:IssueName>
  <ndaq:MarketCategory>NASDAQ</ndaq:MarketCategory>
  <ndaq:ReasonCode>T1</ndaq:ReasonCode>
  <ndaq:PauseThresholdPrice/>
  <ndaq:ResumptionDate>04/24/2026</ndaq:ResumptionDate>
  <ndaq:ResumptionQuoteTime>19:55:00.000</ndaq:ResumptionQuoteTime>
  <ndaq:ResumptionTradeTime>19:55:30.000</ndaq:ResumptionTradeTime>
</item>
</channel></rss>"#;
        let rows = parse_feed(body, 1_777_300_000_000_000, &meta()).expect("parse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].resumption_date, Some(20_567));
        assert_eq!(rows[0].resumption_quote_time.as_deref(), Some("19:55:00.000"));
        assert_eq!(rows[0].resumption_trade_time.as_deref(), Some("19:55:30.000"));
    }

    #[test]
    fn skips_item_missing_required_fields() {
        // No <ndaq:IssueSymbol> — drop the row.
        let body = r#"<?xml version="1.0"?>
<rss xmlns:ndaq="http://www.nasdaqtrader.com/" version="2.0">
<channel>
<item>
  <ndaq:HaltDate>04/24/2026</ndaq:HaltDate>
  <ndaq:HaltTime>19:50:00.000</ndaq:HaltTime>
  <ndaq:MarketCategory>NASDAQ</ndaq:MarketCategory>
</item>
</channel></rss>"#;
        let rows = parse_feed(body, 0, &meta()).expect("parse");
        assert!(rows.is_empty());
    }

    #[test]
    fn empty_feed_returns_zero_rows() {
        let body = r#"<?xml version="1.0"?>
<rss xmlns:ndaq="http://www.nasdaqtrader.com/" version="2.0">
<channel></channel></rss>"#;
        let rows = parse_feed(body, 0, &meta()).expect("parse");
        assert!(rows.is_empty());
    }
}
