//! Backed Finance corp-actions feed.
//!
//! Endpoint: `https://github.com/{REPO}/commits/main.atom` (default
//! repo: `backed-fi/backed-tokens-metadata`). Atom 1.0 format with
//! one `<entry>` per commit.
//!
//! Each commit becomes one [`scryer_schema::backed::v1::Action`] row.
//! The `action_type` field is heuristically classified from the
//! commit title via substring matching: "list" / "delist" / "rename"
//! / "distribution" / "unknown". The ticker registry
//! (`all_tickers_json`) is extracted by regex from title + content.

use std::collections::BTreeSet;

use scryer_schema::backed::v1::Action;
use scryer_schema::Meta;

use crate::{parse_iso_date_to_date32, FetchError};

pub const DEFAULT_REPO: &str = "backed-fi/backed-tokens-metadata";
pub const DEFAULT_BRANCH: &str = "main";

/// Build the GitHub commits Atom URL for the given repo + branch.
pub fn commits_atom_url(repo: &str, branch: &str) -> String {
    format!("https://github.com/{repo}/commits/{branch}.atom")
}

/// Parse an Atom feed body into [`Action`] rows. Public so tests can
/// drive it directly with hardcoded XML strings.
pub fn parse_feed(
    body: &str,
    repo: &str,
    detected_at_micros: i64,
    meta: &Meta,
) -> Result<Vec<Action>, FetchError> {
    let mut reader = quick_xml::Reader::from_str(body);
    reader.config_mut().trim_text(true);

    let mut out: Vec<Action> = Vec::new();
    let mut buf = Vec::new();
    let mut in_entry = false;
    let mut current: Option<EntryAccumulator> = None;
    let mut current_tag: Option<String> = None;

    loop {
        match reader.read_event_into(&mut buf) {
            Err(e) => return Err(FetchError::XmlParse(format!("read: {e}"))),
            Ok(quick_xml::events::Event::Eof) => break,
            Ok(quick_xml::events::Event::Start(e)) => {
                let name = local_name(e.name().as_ref());
                if name == "entry" {
                    in_entry = true;
                    current = Some(EntryAccumulator::default());
                    continue;
                }
                if in_entry && name == "link" {
                    capture_link(&e, current.as_mut());
                }
                if in_entry {
                    current_tag = Some(name);
                }
            }
            Ok(quick_xml::events::Event::Empty(e)) => {
                let name = local_name(e.name().as_ref());
                if in_entry && name == "link" {
                    capture_link(&e, current.as_mut());
                }
            }
            Ok(quick_xml::events::Event::End(e)) => {
                let name = local_name(e.name().as_ref());
                if name == "entry" {
                    if let Some(acc) = current.take() {
                        match acc.finalize(repo, detected_at_micros, meta) {
                            Ok(Some(row)) => out.push(row),
                            Ok(None) => {
                                tracing::debug!("backed atom entry missing required fields");
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "backed atom parse error");
                            }
                        }
                    }
                    in_entry = false;
                    current_tag = None;
                    continue;
                }
                current_tag = None;
            }
            Ok(quick_xml::events::Event::Text(t)) => {
                if in_entry {
                    let text = t.unescape().map(|s| s.to_string()).unwrap_or_default();
                    if let Some(tag) = current_tag.as_ref() {
                        if let Some(acc) = current.as_mut() {
                            assign_field(acc, tag, &text);
                        }
                    }
                }
            }
            Ok(quick_xml::events::Event::CData(t)) => {
                if in_entry {
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
struct EntryAccumulator {
    id: String,
    title: String,
    updated: String,
    content: String,
    link_href: String,
}

fn assign_field(acc: &mut EntryAccumulator, tag: &str, text: &str) {
    match tag {
        "id" => acc.id.push_str(text),
        "title" => acc.title.push_str(text),
        "updated" => acc.updated.push_str(text),
        "content" => acc.content.push_str(text),
        _ => {}
    }
}

/// Capture an Atom `<link>` element's `href` if `rel="alternate"` (or
/// `rel` absent — Atom default is `alternate`).
fn capture_link(e: &quick_xml::events::BytesStart<'_>, acc: Option<&mut EntryAccumulator>) {
    let mut rel_alternate = true;
    let mut href: Option<String> = None;
    for attr in e.attributes().flatten() {
        match attr.key.as_ref() {
            b"rel" => {
                let v = attr.unescape_value().unwrap_or_default();
                rel_alternate = v == "alternate";
            }
            b"href" => {
                href = Some(attr.unescape_value().unwrap_or_default().to_string());
            }
            _ => {}
        }
    }
    if rel_alternate {
        if let (Some(h), Some(a)) = (href, acc) {
            a.link_href = h;
        }
    }
}

fn local_name(qname: &[u8]) -> String {
    let s = std::str::from_utf8(qname).unwrap_or("");
    match s.find(':') {
        Some(idx) => s[idx + 1..].to_string(),
        None => s.to_string(),
    }
}

impl EntryAccumulator {
    fn finalize(
        self,
        repo: &str,
        detected_at: i64,
        meta: &Meta,
    ) -> Result<Option<Action>, FetchError> {
        // GitHub commit ID looks like `tag:github.com,2008:Grit::Commit/<sha>`
        let commit_sha = match extract_commit_sha(&self.id, &self.link_href) {
            Some(s) => s,
            None => return Ok(None),
        };
        if self.title.is_empty() || self.updated.is_empty() {
            return Ok(None);
        }
        // Atom <updated> is RFC3339; commit_date is the date portion.
        let commit_date = match self.updated.split('T').next() {
            Some(date_part) => parse_iso_date_to_date32(date_part)?,
            None => return Ok(None),
        };
        let title_clean = self.title.trim().to_string();
        let action_type = classify_action(&title_clean, &self.content);
        let tickers = extract_backed_tickers(&title_clean, &self.content);
        let all_tickers_json = serde_json::to_string(&tickers).unwrap_or_else(|_| "[]".to_string());
        let underlying = if tickers.len() == 1 {
            Some(tickers[0].clone())
        } else {
            None
        };
        let snippet = build_snippet(&title_clean);
        Ok(Some(Action {
            detected_at,
            repo: repo.to_string(),
            commit_sha,
            commit_date,
            commit_url: self.link_href,
            title: title_clean,
            underlying,
            all_tickers_json,
            action_type,
            snippet,
            meta: meta.clone(),
        }))
    }
}

/// Extract the commit SHA from the `<id>` (preferred) or, as fallback,
/// the trailing path segment of the `<link href>`.
pub fn extract_commit_sha(id: &str, link_href: &str) -> Option<String> {
    if let Some(idx) = id.rfind('/') {
        let candidate = &id[idx + 1..];
        if is_hex_sha(candidate) {
            return Some(candidate.to_string());
        }
    }
    if let Some(idx) = link_href.rfind('/') {
        let candidate = &link_href[idx + 1..];
        if is_hex_sha(candidate) {
            return Some(candidate.to_string());
        }
    }
    None
}

fn is_hex_sha(s: &str) -> bool {
    !s.is_empty()
        && s.len() >= 7
        && s.len() <= 40
        && s.chars().all(|c| c.is_ascii_hexdigit())
}

/// Categorize a commit by title content. Cheap substring matching;
/// false-positive risk is acceptable here (downstream consumers can
/// filter by `title` if needed).
pub fn classify_action(title: &str, content: &str) -> String {
    let combined = format!("{} {}", title, content).to_lowercase();
    if combined.contains("delist") || combined.contains("remove") {
        return "delist".into();
    }
    if combined.contains("rename") || combined.contains("symbol change") {
        return "rename".into();
    }
    if combined.contains("distribution") || combined.contains("dividend") {
        return "distribution".into();
    }
    if combined.contains("list") || combined.contains("add") {
        return "list".into();
    }
    "unknown".into()
}

/// Extract Backed-style tickers (`b{TICKER}`) from a string. Tickers
/// are 2-6 uppercase letters preceded by lowercase `b`. Returns
/// sorted, deduplicated results.
pub fn extract_backed_tickers(title: &str, content: &str) -> Vec<String> {
    let combined = format!("{} {}", title, content);
    let bytes = combined.as_bytes();
    let mut out: BTreeSet<String> = BTreeSet::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'b' {
            let prev_is_alnum = i > 0 && bytes[i - 1].is_ascii_alphanumeric();
            if !prev_is_alnum {
                let mut j = i + 1;
                while j < bytes.len() && bytes[j].is_ascii_uppercase() {
                    j += 1;
                }
                let len = j - (i + 1);
                if (2..=6).contains(&len) {
                    let next_is_alnum = j < bytes.len() && bytes[j].is_ascii_alphanumeric();
                    if !next_is_alnum {
                        let ticker = std::str::from_utf8(&bytes[i..j]).unwrap_or("").to_string();
                        out.insert(ticker);
                    }
                }
            }
        }
        i += 1;
    }
    out.into_iter().collect()
}

fn build_snippet(title: &str) -> String {
    if title.len() <= 140 {
        title.to_string()
    } else {
        format!("{}…", &title[..140])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> Meta {
        Meta::new(
            scryer_schema::backed::v1::SCHEMA_VERSION,
            1_777_300_000,
            "github:atom",
        )
    }

    #[test]
    fn classifies_action_types() {
        assert_eq!(classify_action("List bSPY", ""), "list");
        assert_eq!(classify_action("Delist bAAPL", ""), "delist");
        assert_eq!(classify_action("Rename bGOOG to bGOOGL", ""), "rename");
        assert_eq!(classify_action("Distribution event for bSPY", ""), "distribution");
        assert_eq!(classify_action("Refactor metadata schema", ""), "unknown");
    }

    #[test]
    fn extracts_backed_tickers_from_title() {
        let tickers = extract_backed_tickers("List bSPY and bQQQ", "");
        assert_eq!(tickers, vec!["bQQQ", "bSPY"]); // BTreeSet → sorted
    }

    #[test]
    fn ticker_extraction_skips_non_word_boundary_matches() {
        let tickers = extract_backed_tickers("abSPY", "");
        assert!(tickers.is_empty());
    }

    #[test]
    fn extract_commit_sha_from_id() {
        let id = "tag:github.com,2008:Grit::Commit/abc1234567890def";
        let sha = extract_commit_sha(id, "https://github.com/foo/bar/commit/somethingelse").unwrap();
        assert_eq!(sha, "abc1234567890def");
    }

    #[test]
    fn extract_commit_sha_fallback_to_link_href() {
        let id = "no-sha-here";
        let sha = extract_commit_sha(id, "https://github.com/foo/bar/commit/abcdef1234567890").unwrap();
        assert_eq!(sha, "abcdef1234567890");
    }

    #[test]
    fn parses_typical_atom_feed() {
        let body = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>tag:github.com,2008:Grit::Commit/abc1234567890def0123456789abcdef01234567</id>
    <link rel="alternate" type="text/html" href="https://github.com/backed-fi/backed-tokens-metadata/commit/abc1234567890def0123456789abcdef01234567"/>
    <title>List bSPY and bQQQ</title>
    <updated>2026-04-25T12:34:56Z</updated>
    <author><name>backed-bot</name></author>
    <content type="html">Adding bSPY and bQQQ tokens to the metadata.</content>
  </entry>
  <entry>
    <id>tag:github.com,2008:Grit::Commit/def4567890abc0123456789abcdef0123456abcd</id>
    <link rel="alternate" type="text/html" href="https://github.com/backed-fi/backed-tokens-metadata/commit/def4567890abc0123456789abcdef0123456abcd"/>
    <title>Rename bGOOG to bGOOGL</title>
    <updated>2026-04-26T08:00:00Z</updated>
    <content type="html">bGOOG renamed bGOOGL after the underlying ticker change.</content>
  </entry>
</feed>"#;
        let rows = parse_feed(
            body,
            "backed-fi/backed-tokens-metadata",
            1_777_300_000_000_000,
            &meta(),
        )
        .expect("parse");
        assert_eq!(rows.len(), 2);

        let first = &rows[0];
        assert_eq!(first.commit_sha, "abc1234567890def0123456789abcdef01234567");
        assert_eq!(first.repo, "backed-fi/backed-tokens-metadata");
        assert_eq!(first.title, "List bSPY and bQQQ");
        assert_eq!(first.action_type, "list");
        assert_eq!(first.commit_date, 20_568); // 2026-04-25
        assert_eq!(first.all_tickers_json, r#"["bQQQ","bSPY"]"#);
        assert!(first.underlying.is_none()); // 2 tickers → multi-ticker

        let second = &rows[1];
        assert_eq!(second.action_type, "rename");
        assert_eq!(second.all_tickers_json, r#"["bGOOG","bGOOGL"]"#);
    }

    #[test]
    fn underlying_set_for_single_ticker_commits() {
        let body = r#"<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>tag:github.com,2008:Grit::Commit/aaaaaaa0000000000000000000000000000000ab</id>
    <link rel="alternate" href="https://github.com/foo/bar/commit/aaaaaaa0000000000000000000000000000000ab"/>
    <title>Delist bAAPL</title>
    <updated>2026-04-26T00:00:00Z</updated>
    <content type="html">Removing bAAPL from the metadata registry.</content>
  </entry>
</feed>"#;
        let rows = parse_feed(body, "foo/bar", 0, &meta()).expect("parse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].underlying.as_deref(), Some("bAAPL"));
        assert_eq!(rows[0].action_type, "delist");
    }

    #[test]
    fn empty_feed_returns_zero_rows() {
        let body = r#"<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom"></feed>"#;
        let rows = parse_feed(body, "foo/bar", 0, &meta()).expect("parse");
        assert!(rows.is_empty());
    }
}
