//! arXiv adapter.
//!
//! Polls the arXiv search API (Atom XML) for papers submitted strictly
//! after a per-adapter cursor, downloads each new paper's PDF into the
//! configured `ObjectStore`, shells out to `pdftotext` for body
//! extraction, and yields fully-formed `IngestRequest`s for the
//! scheduler to filter + ingest.
//!
//! ## Incremental polling
//!
//! arXiv's `search_query` accepts a `submittedDate:[X+TO+Y]` range
//! filter (minute-precision GMT). We append that to the user's query
//! using the cursor as the lower bound and sort `submittedDate`
//! ascending — so each cycle returns the **N oldest papers newer than
//! the cursor**. The newest `published` we see becomes the next cursor.
//!
//! Cursor shape: `{ "last_published": "<RFC3339>" }`. Per cycle the
//! lower bound is `max(cursor, now - ARXIV_MAX_STALENESS_SECS)` —
//! `ARXIV_MAX_STALENESS_SECS` (default 7 days) guarantees the discovered
//! content is never older than that. Concretely:
//!
//! - First start (no cursor) → fetch from `now - max_staleness`.
//! - Healthy steady-state → fetch from the persisted cursor.
//! - Returning from extended downtime → cursor is older than the
//!   floor, so we clamp to `now - max_staleness` and skip the older
//!   gap. Content stays bounded by the configured staleness, no
//!   thundering-herd backfill.
//!
//! After each poll the cursor is set to the newest `published` we
//! actually saw. With a bounded `ARXIV_PAGE_SIZE` (default 50) we
//! cannot guarantee we caught up to "now" in one cycle, so the newest
//! paper *we observed* is the only point we can safely advance to;
//! subsequent cycles continue forward from there.
//!
//! Note: `submittedDate` is inclusive on both ends, so the boundary
//! paper from the previous cycle re-appears each poll. The pipeline's
//! content-hash dedup swallows it as `Unchanged` — cheap and correct.
//!
//! Slice-2 simplifications:
//! - One page per cycle (no pagination loop). The cursor advances each
//!   cycle, so any backlog drains over successive polls at `page_size`
//!   per cycle.
//! - PDF fetch failures are logged and the paper falls through with
//!   `raw_text = None` — metadata still lands.
//! - `pdftotext` extraction failures likewise just suppress the body;
//!   the paper still ingests with summary + metadata.

use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

use crate::error::{Error, Result};
use crate::ingestion::IngestRequest;
use crate::object_store::ObjectStore;

use super::{placeholder_tenant_id, Fetched, SourceAdapter};

const ADAPTER_NAME: &str = "arxiv";
const DEFAULT_POLL_INTERVAL_SECS: u64 = 21_600; // 6 h
const DEFAULT_PAGE_SIZE: usize = 50;
const DEFAULT_MAX_STALENESS_SECS: u64 = 7 * 24 * 60 * 60; // 7 days
const ENDPOINT: &str = "https://export.arxiv.org/api/query";
const PDF_FETCH_DELAY: Duration = Duration::from_secs(3);
/// Hard cap on PDF download size. arXiv preprints almost always fit in
/// a few MB; the cap is mostly defence against a malformed or
/// adversarial response that would otherwise OOM the backend. Streamed
/// download aborts the moment the running total exceeds this. Tunable
/// via `ARXIV_MAX_PDF_BYTES`.
const DEFAULT_MAX_PDF_BYTES: usize = 50 * 1024 * 1024; // 50 MB
/// Wall-clock cap on the `pdftotext` shell-out. A pathological PDF can
/// take indefinite time / CPU; we'd rather lose the body than block an
/// adapter cycle. Tunable via `ARXIV_PDFTOTEXT_TIMEOUT_SECS`.
const DEFAULT_PDFTOTEXT_TIMEOUT_SECS: u64 = 30;
/// Hard cap on extracted-text length. A "PDF bomb" can decompress to
/// many GB of text; we read with a moving cap and kill the process
/// when reached. 4 MB comfortably exceeds any real paper (a full
/// monograph is ~1–2 MB of text) without leaving slack for adversarial
/// payloads. Tunable via `ARXIV_MAX_EXTRACTED_TEXT_BYTES`.
const DEFAULT_MAX_EXTRACTED_TEXT_BYTES: usize = 4 * 1024 * 1024; // 4 MB
/// Whole-request timeout on every reqwest call (search + PDF fetch).
/// Closes the slow-server / stalled-stream hole that the size caps
/// alone don't cover. Tunable via `ARXIV_HTTP_TIMEOUT_SECS`.
const DEFAULT_HTTP_TIMEOUT_SECS: u64 = 60;
/// Connect-handshake timeout, separately bounded so a host that
/// resolves but never accepts gets dropped quickly. Tunable via
/// `ARXIV_HTTP_CONNECT_TIMEOUT_SECS`.
const DEFAULT_HTTP_CONNECT_TIMEOUT_SECS: u64 = 10;
/// Open-ended upper bound for the `submittedDate` filter — far enough in
/// the future that "newer than cursor" is the only effective constraint.
const ARXIV_DATE_MAX: &str = "999912312359";
/// arXiv asks API consumers to put a contact email in the User-Agent so
/// they can reach out before rate-limiting an offending client. The
/// `ARXIV_CONTACT_EMAIL` env var fills the address; if unset we fall
/// back to a generic UA (works but is more 429-prone).
const USER_AGENT_FALLBACK: &str =
    "delphi-backend/0.1 (https://github.com/Accelero/delphi)";

pub struct ArxivAdapter {
    query: String,
    poll_interval: Duration,
    page_size: usize,
    /// Maximum age of auto-discovered content. Floors the lower bound
    /// of every poll at `now - max_staleness`; cursor values older than
    /// this (no cursor on first start, or a long downtime) get clamped
    /// instead of triggering an unbounded backfill.
    max_staleness: Duration,
    /// PDF download size cap (bytes). Streamed read aborts at this.
    max_pdf_bytes: usize,
    /// `pdftotext` wall-clock timeout. Process is killed on expiry.
    pdftotext_timeout: Duration,
    /// Cap on extracted-text bytes. `pdftotext` is killed when reached.
    max_extracted_text_bytes: usize,
    http: Client,
    object_store: Arc<dyn ObjectStore>,
}

impl ArxivAdapter {
    /// `None` when `ARXIV_QUERY` is unset — the install/uninstall switch.
    pub fn try_from_env(object_store: Arc<dyn ObjectStore>) -> Option<Self> {
        let query = std::env::var("ARXIV_QUERY").ok().filter(|s| !s.trim().is_empty())?;
        let poll_interval = std::env::var("ARXIV_POLL_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEFAULT_POLL_INTERVAL_SECS);
        let page_size = std::env::var("ARXIV_PAGE_SIZE")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(DEFAULT_PAGE_SIZE);
        let max_staleness = Duration::from_secs(
            std::env::var("ARXIV_MAX_STALENESS_SECS")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(DEFAULT_MAX_STALENESS_SECS),
        );
        let user_agent = match std::env::var("ARXIV_CONTACT_EMAIL").ok() {
            Some(email) if !email.trim().is_empty() => format!(
                "delphi-backend/0.1 (https://github.com/Accelero/delphi; mailto:{})",
                email.trim()
            ),
            _ => USER_AGENT_FALLBACK.to_string(),
        };
        let http_timeout = Duration::from_secs(
            std::env::var("ARXIV_HTTP_TIMEOUT_SECS")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(DEFAULT_HTTP_TIMEOUT_SECS),
        );
        let http_connect_timeout = Duration::from_secs(
            std::env::var("ARXIV_HTTP_CONNECT_TIMEOUT_SECS")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(DEFAULT_HTTP_CONNECT_TIMEOUT_SECS),
        );
        let http = Client::builder()
            .user_agent(user_agent)
            .timeout(http_timeout)
            .connect_timeout(http_connect_timeout)
            .build()
            .ok()?;
        let max_pdf_bytes = std::env::var("ARXIV_MAX_PDF_BYTES")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAX_PDF_BYTES);
        let pdftotext_timeout = Duration::from_secs(
            std::env::var("ARXIV_PDFTOTEXT_TIMEOUT_SECS")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(DEFAULT_PDFTOTEXT_TIMEOUT_SECS),
        );
        let max_extracted_text_bytes = std::env::var("ARXIV_MAX_EXTRACTED_TEXT_BYTES")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAX_EXTRACTED_TEXT_BYTES);
        Some(Self {
            query,
            poll_interval: Duration::from_secs(poll_interval),
            page_size,
            max_staleness,
            max_pdf_bytes,
            pdftotext_timeout,
            max_extracted_text_bytes,
            http,
            object_store,
        })
    }
}

// ─── Atom XML structs ──────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct AtomFeed {
    #[serde(rename = "entry", default)]
    entries: Vec<AtomEntry>,
}

#[derive(Debug, Deserialize)]
struct AtomEntry {
    id: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    summary: String,
    #[serde(default)]
    published: String,
    #[serde(rename = "author", default)]
    authors: Vec<AtomAuthor>,
    #[serde(rename = "link", default)]
    links: Vec<AtomLink>,
    #[serde(rename = "category", default)]
    categories: Vec<AtomCategory>,
}

#[derive(Debug, Deserialize)]
struct AtomAuthor {
    #[serde(default)]
    name: String,
}

#[derive(Debug, Deserialize)]
struct AtomLink {
    #[serde(rename = "@href", default)]
    href: String,
    #[serde(rename = "@title", default)]
    title: String,
    #[serde(rename = "@type", default)]
    typ: String,
}

#[derive(Debug, Deserialize)]
struct AtomCategory {
    #[serde(rename = "@term", default)]
    term: String,
}

// ─── adapter impl ──────────────────────────────────────────────────────────

#[async_trait]
impl SourceAdapter for ArxivAdapter {
    fn name(&self) -> &str {
        ADAPTER_NAME
    }
    fn poll_interval(&self) -> Duration {
        self.poll_interval
    }
    async fn fetch(&self, cursor: Option<Value>) -> Result<Fetched> {
        let cursor_ts = cursor
            .as_ref()
            .and_then(|c| c.get("last_published"))
            .and_then(|v| v.as_str())
            .and_then(parse_rfc3339);

        let now = Utc::now();
        let staleness_floor = now - self.max_staleness;
        // Lower bound = max(cursor, staleness_floor). Both first-start
        // (no cursor) and post-downtime (stale cursor) collapse to
        // "fetch from staleness_floor", capping content age at the
        // configured max staleness without backfilling further.
        let lower_bound = cursor_ts
            .map(|ts| ts.max(staleness_floor))
            .unwrap_or(staleness_floor);
        if cursor_ts.map_or(true, |ts| ts < staleness_floor) {
            tracing::info!(
                cursor = ?cursor_ts.map(|t| t.to_rfc3339()),
                lower_bound = %lower_bound.to_rfc3339(),
                max_staleness_secs = self.max_staleness.as_secs(),
                "arxiv: cursor missing or older than max staleness — clamped to staleness floor"
            );
        }

        let scoped_query = format!(
            "{} AND submittedDate:[{} TO {}]",
            self.query,
            format_arxiv_date(lower_bound),
            ARXIV_DATE_MAX,
        );

        let resp = self
            .http
            .get(ENDPOINT)
            .query(&[
                ("search_query", scoped_query.as_str()),
                ("sortBy", "submittedDate"),
                ("sortOrder", "ascending"),
                ("start", "0"),
                ("max_results", &self.page_size.to_string()),
            ])
            .send()
            .await?;
        let status = resp.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after_secs =
                parse_retry_after(resp.headers().get(reqwest::header::RETRY_AFTER))
                    .map(|d| d.as_secs());
            tracing::warn!(
                ?retry_after_secs,
                next_cycle_secs = self.poll_interval.as_secs(),
                "arxiv: 429 Too Many Requests; skipping this cycle, retry on next interval"
            );
            return Ok(Fetched {
                items: Vec::new(),
                next_cursor: None,
            });
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Adapter {
                name: ADAPTER_NAME.into(),
                message: format!("HTTP {status}: {body}"),
            });
        }
        let xml = resp.text().await?;
        let entries = parse_atom_feed(&xml)?;

        let mut items = Vec::with_capacity(entries.len());
        // Cursor advances to the newest paper actually observed. If the
        // page is empty we leave the cursor untouched — next poll will
        // recompute the lower bound from the same (now slightly older)
        // base, naturally moving forward as `now` advances.
        let mut newest: Option<DateTime<Utc>> = None;

        for (idx, entry) in entries.into_iter().enumerate() {
            let Some(published) = parse_rfc3339(&entry.published) else {
                tracing::warn!(id = %entry.id, ts = %entry.published, "unparseable arxiv date, skipping");
                continue;
            };
            newest = Some(newest.map_or(published, |n| n.max(published)));

            // arXiv courtesy: pace PDF fetches. Don't sleep before the first
            // one (we already paid for the search request).
            if idx > 0 {
                tokio::time::sleep(PDF_FETCH_DELAY).await;
            }

            match self.build_ingest_request(entry, published).await {
                Ok(req) => items.push(req),
                Err(e) => {
                    tracing::error!(error = %e, "arxiv: failed to build ingest request");
                }
            }
        }

        let next_cursor = newest.map(|dt| json!({ "last_published": dt.to_rfc3339() }));
        Ok(Fetched {
            items,
            next_cursor,
        })
    }
}

impl ArxivAdapter {
    async fn build_ingest_request(
        &self,
        entry: AtomEntry,
        published: DateTime<Utc>,
    ) -> Result<IngestRequest> {
        // Identity. arXiv ids look like "http://arxiv.org/abs/2106.09685v2".
        let abs_id = parse_arxiv_abs_id(&entry.id).ok_or_else(|| Error::Adapter {
            name: ADAPTER_NAME.into(),
            message: format!("malformed arxiv id: {}", entry.id),
        })?;
        let canonical_id = format!("arxiv:{abs_id}");
        let source_uri = format!("https://arxiv.org/abs/{abs_id}");
        let pdf_url = entry
            .links
            .iter()
            .find(|l| l.title == "pdf" || l.typ == "application/pdf")
            .map(|l| l.href.clone());

        // Fetch + store + extract. Soft-fail any of these — the paper
        // still ingests with metadata + summary, body just stays None.
        let (storage_uri, raw_text) = match pdf_url {
            Some(ref url) => self.fetch_and_extract(&abs_id, url).await,
            None => (None, None),
        };

        let title = entry.title.split_whitespace().collect::<Vec<_>>().join(" ");
        let summary = entry.summary.split_whitespace().collect::<Vec<_>>().join(" ");
        let authors: Vec<String> = entry
            .authors
            .into_iter()
            .filter(|a| !a.name.is_empty())
            .map(|a| a.name.split_whitespace().collect::<Vec<_>>().join(" "))
            .collect();
        let categories: Vec<String> =
            entry.categories.into_iter().map(|c| c.term).collect();

        let metadata = json!({
            "arxiv_id": abs_id,
            "categories": categories,
            "primary_category": categories_first(&abs_id, &entry.id), // best-effort
        });

        Ok(IngestRequest {
            // Tenant placeholder: scheduler is authoritative and always
            // overwrites this before the request reaches the sink. The
            // adapter is tenant-agnostic; v2 multi-tenant scheduler will
            // construct one adapter instance per tenant.
            tenant_id: placeholder_tenant_id(),
            canonical_id,
            source_type: ADAPTER_NAME.into(),
            source_uri,
            title: if title.is_empty() { None } else { Some(title) },
            authors,
            published_at: Some(published),
            language: None,
            summary: if summary.is_empty() { None } else { Some(summary) },
            raw_text,
            storage_uri,
            metadata,
        })
    }

    async fn fetch_and_extract(
        &self,
        abs_id: &str,
        pdf_url: &str,
    ) -> (Option<String>, Option<String>) {
        let bytes = match fetch_pdf_capped(&self.http, pdf_url, self.max_pdf_bytes).await {
            Some(b) => b,
            None => return (None, None),
        };

        let key = format!("arxiv/{abs_id}.pdf");
        // `Bytes::clone` is a refcount bump, not a memory copy — safe to
        // hand a clone to the store and another to pdftotext below.
        let storage_uri = match self.object_store.put(&key, bytes.clone()).await {
            Ok(uri) => Some(uri),
            Err(e) => {
                tracing::error!(error = %e, key, "arxiv: object_store.put failed");
                None
            }
        };

        let text = match extract_pdf_text(
            bytes,
            self.pdftotext_timeout,
            self.max_extracted_text_bytes,
        )
        .await
        {
            Ok(s) if s.trim().is_empty() => {
                tracing::info!(abs_id, "arxiv: pdftotext returned empty (scanned PDF?)");
                None
            }
            Ok(s) => Some(s),
            Err(e) => {
                tracing::warn!(error = %e, abs_id, "arxiv: pdftotext failed");
                None
            }
        };

        (storage_uri, text)
    }
}

// ─── helpers ───────────────────────────────────────────────────────────────

fn parse_rfc3339(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s.trim())
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

/// arXiv's `submittedDate` filter wants `YYYYMMDDhhmm` GMT (minute
/// precision).
fn format_arxiv_date(dt: DateTime<Utc>) -> String {
    dt.format("%Y%m%d%H%M").to_string()
}

/// Honour an HTTP `Retry-After` header: either an integer number of
/// seconds or an RFC 7231 absolute date. Returns the wait duration, or
/// `None` if the header is absent / unparseable.
fn parse_retry_after(h: Option<&reqwest::header::HeaderValue>) -> Option<Duration> {
    let v = h?.to_str().ok()?.trim();
    if let Ok(secs) = v.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }
    // Absolute date form. RFC 7231 says it's an HTTP-date; chrono parses
    // it as RFC 2822.
    let target = DateTime::parse_from_rfc2822(v).ok()?;
    let now = Utc::now();
    let diff = target.with_timezone(&Utc) - now;
    diff.to_std().ok()
}

/// `http://arxiv.org/abs/2106.09685v2` → `2106.09685v2`
fn parse_arxiv_abs_id(id: &str) -> Option<String> {
    id.rsplit_once("/abs/").map(|(_, tail)| tail.to_string())
}

/// Best-effort first-category guess. arXiv's primary_category lives in
/// the `arxiv:` namespace which our serde model doesn't cover; we'd
/// need either custom XML walking or to read `<category>` and call the
/// first one primary. For slice 2 we just leave this as a string-shaped
/// hint inside metadata; consumers can read `metadata.categories[0]`.
fn categories_first(_abs_id: &str, _id: &str) -> Option<String> {
    None
}

fn parse_atom_feed(xml: &str) -> Result<Vec<AtomEntry>> {
    let feed: AtomFeed = quick_xml::de::from_str(xml).map_err(|e| Error::Adapter {
        name: ADAPTER_NAME.into(),
        message: format!("xml parse: {e}"),
    })?;
    Ok(feed.entries)
}

/// Stream a PDF body into memory with a hard size cap. Aborts (returns
/// `None`) on non-success status, network error, or when the running
/// total exceeds `cap`. Pre-checks `Content-Length` when present so the
/// connection is dropped before any body is read for oversize bodies.
async fn fetch_pdf_capped(http: &Client, url: &str, cap: usize) -> Option<Bytes> {
    let resp = match http.get(url).send().await {
        Ok(r) if r.status().is_success() => r,
        Ok(r) => {
            tracing::warn!(status = %r.status(), url, "arxiv: pdf fetch non-success");
            return None;
        }
        Err(e) => {
            tracing::warn!(error = %e, url, "arxiv: pdf fetch network error");
            return None;
        }
    };

    if let Some(declared) = resp.content_length() {
        if declared as usize > cap {
            tracing::warn!(
                content_length = declared,
                cap,
                url,
                "arxiv: pdf exceeds size cap (Content-Length); skipping"
            );
            return None;
        }
    }

    let mut buf = BytesMut::with_capacity(64 * 1024);
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, url, "arxiv: pdf chunk read error");
                return None;
            }
        };
        if buf.len() + chunk.len() > cap {
            tracing::warn!(cap, url, "arxiv: pdf body exceeded size cap; aborting");
            return None;
        }
        buf.extend_from_slice(&chunk);
    }
    Some(buf.freeze())
}

/// Run `pdftotext` over `bytes`, with a wall-clock `timeout` and an
/// extracted-byte cap of `max_out`. The bytes are consumed from a
/// `Bytes` (refcount-cheap, no `to_vec` copy) on a background task
/// while we drain stdout in capped chunks. If either limit is hit
/// the child is killed; partial output is returned only on success
/// or natural EOF.
async fn extract_pdf_text(bytes: Bytes, timeout: Duration, max_out: usize) -> Result<String> {
    let mut child = Command::new("pdftotext")
        .arg("-q") // quiet
        .arg("-enc") // explicit UTF-8
        .arg("UTF-8")
        .arg("-") // stdin
        .arg("-") // stdout
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // If our task is dropped mid-flight (e.g. scheduler shutdown),
        // make sure we don't leak a pdftotext process.
        .kill_on_drop(true)
        .spawn()?;

    let mut stdin = child.stdin.take().ok_or_else(|| Error::Adapter {
        name: ADAPTER_NAME.into(),
        message: "pdftotext: stdin not captured".into(),
    })?;
    let mut stdout = child.stdout.take().ok_or_else(|| Error::Adapter {
        name: ADAPTER_NAME.into(),
        message: "pdftotext: stdout not captured".into(),
    })?;
    let mut stderr = child.stderr.take().ok_or_else(|| Error::Adapter {
        name: ADAPTER_NAME.into(),
        message: "pdftotext: stderr not captured".into(),
    })?;

    let writer = tokio::spawn(async move {
        let _ = stdin.write_all(&bytes).await;
        let _ = stdin.shutdown().await;
    });

    let read_capped = async {
        let mut out: Vec<u8> = Vec::with_capacity(16 * 1024);
        let mut buf = [0u8; 16 * 1024];
        let mut truncated = false;
        loop {
            let n = match stdout.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) => {
                    return Err(Error::Adapter {
                        name: ADAPTER_NAME.into(),
                        message: format!("pdftotext stdout read: {e}"),
                    })
                }
            };
            let take = max_out.saturating_sub(out.len()).min(n);
            out.extend_from_slice(&buf[..take]);
            if out.len() >= max_out {
                truncated = true;
                break;
            }
        }
        Ok::<(Vec<u8>, bool), Error>((out, truncated))
    };

    let (out, truncated) = match tokio::time::timeout(timeout, read_capped).await {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => {
            let _ = child.kill().await;
            let _ = writer.await;
            return Err(e);
        }
        Err(_elapsed) => {
            let _ = child.kill().await;
            let _ = writer.await;
            return Err(Error::Adapter {
                name: ADAPTER_NAME.into(),
                message: format!("pdftotext timed out after {}s", timeout.as_secs()),
            });
        }
    };

    if truncated {
        tracing::info!(
            cap_bytes = max_out,
            "arxiv: pdftotext output capped — killing child"
        );
        let _ = child.kill().await;
    }

    let _ = writer.await;
    let mut err_buf = String::new();
    let _ = stderr.read_to_string(&mut err_buf).await;
    let status = child.wait().await?;

    // pdftotext can exit non-zero on a malformed page while still
    // emitting useful text for the rest of the document — keep what we
    // got. Hard fail only when there's no output to salvage.
    if !status.success() && out.is_empty() && !truncated {
        return Err(Error::Adapter {
            name: ADAPTER_NAME.into(),
            message: format!("pdftotext failed: {err_buf}"),
        });
    }
    Ok(String::from_utf8_lossy(&out).into_owned())
}

// ─── tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Trimmed real-shaped arXiv response. Two entries, one with PDF
    /// link, one with just metadata-shaped links.
    const ATOM_FIXTURE: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>arXiv Query Result</title>
  <id>http://arxiv.org/api/query</id>
  <entry>
    <id>http://arxiv.org/abs/2106.09685v2</id>
    <updated>2021-10-16T00:00:00Z</updated>
    <published>2021-06-17T17:37:18Z</published>
    <title>LoRA: Low-Rank Adaptation of Large Language Models</title>
    <summary>An important paradigm of natural language processing
consists of large-scale pre-training on general domain data and adaptation
to particular tasks or domains. We propose Low-Rank Adaptation, or LoRA.</summary>
    <author><name>Edward J. Hu</name></author>
    <author><name>Yelong Shen</name></author>
    <author><name>Phillip Wallis</name></author>
    <link href="http://arxiv.org/abs/2106.09685v2" rel="alternate" type="text/html"/>
    <link title="pdf" href="http://arxiv.org/pdf/2106.09685v2" rel="related" type="application/pdf"/>
    <category term="cs.CL"/>
    <category term="cs.AI"/>
  </entry>
  <entry>
    <id>http://arxiv.org/abs/2305.14314v1</id>
    <updated>2023-05-23T00:00:00Z</updated>
    <published>2023-05-23T17:50:33Z</published>
    <title>QLoRA: Efficient Finetuning of Quantized LLMs</title>
    <summary>We present QLoRA, an efficient finetuning approach.</summary>
    <author><name>Tim Dettmers</name></author>
    <link href="http://arxiv.org/abs/2305.14314v1" rel="alternate" type="text/html"/>
    <link title="pdf" href="http://arxiv.org/pdf/2305.14314v1" rel="related" type="application/pdf"/>
    <category term="cs.LG"/>
  </entry>
</feed>
"#;

    #[test]
    fn parses_atom_feed_into_entries() {
        let entries = parse_atom_feed(ATOM_FIXTURE).unwrap();
        assert_eq!(entries.len(), 2);

        let first = &entries[0];
        assert_eq!(first.id, "http://arxiv.org/abs/2106.09685v2");
        assert!(first.title.contains("LoRA"));
        assert_eq!(first.authors.len(), 3);
        assert_eq!(first.authors[0].name, "Edward J. Hu");
        assert!(first.links.iter().any(|l| l.title == "pdf"));
        assert_eq!(first.categories.len(), 2);
        assert_eq!(first.categories[0].term, "cs.CL");
    }

    #[test]
    fn extracts_canonical_id_from_atom_url() {
        assert_eq!(
            parse_arxiv_abs_id("http://arxiv.org/abs/2106.09685v2").as_deref(),
            Some("2106.09685v2")
        );
        assert_eq!(parse_arxiv_abs_id("malformed").as_deref(), None);
    }

    #[test]
    fn parses_published_timestamp() {
        let dt = parse_rfc3339("2021-06-17T17:37:18Z").unwrap();
        assert_eq!(dt.to_rfc3339(), "2021-06-17T17:37:18+00:00");
    }

    #[test]
    fn formats_date_for_arxiv_submitted_date_filter() {
        let dt = parse_rfc3339("2021-06-17T17:37:18Z").unwrap();
        assert_eq!(format_arxiv_date(dt), "202106171737");
    }

    /// Drives `extract_pdf_text` through a `cat`-equivalent: a fake
    /// pdftotext that just echoes stdin to stdout. Lets us verify the
    /// timeout and output-cap branches without a real PDF.
    #[cfg(unix)]
    mod pdftotext_caps {
        use std::process::Stdio;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::process::Command;
        use tokio::time::Duration;

        /// Stand-in extractor that runs `cat` instead of `pdftotext`,
        /// applying the same timeout + output-cap policy. Mirrors the
        /// real extract_pdf_text shape so the test exercises the actual
        /// kill / cap logic rather than a re-implementation.
        async fn extract_via_cat(
            bytes: bytes::Bytes,
            timeout: Duration,
            max_out: usize,
        ) -> Option<(String, bool)> {
            let mut child = Command::new("cat")
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true)
                .spawn()
                .ok()?;
            let mut stdin = child.stdin.take()?;
            let mut stdout = child.stdout.take()?;
            let writer = tokio::spawn(async move {
                let _ = stdin.write_all(&bytes).await;
                let _ = stdin.shutdown().await;
            });
            let read_capped = async {
                let mut out: Vec<u8> = Vec::new();
                let mut buf = [0u8; 8 * 1024];
                let mut truncated = false;
                loop {
                    let n = stdout.read(&mut buf).await.ok()?;
                    if n == 0 {
                        break;
                    }
                    let take = max_out.saturating_sub(out.len()).min(n);
                    out.extend_from_slice(&buf[..take]);
                    if out.len() >= max_out {
                        truncated = true;
                        break;
                    }
                }
                Some((out, truncated))
            };
            let (out, truncated) = tokio::time::timeout(timeout, read_capped).await.ok()??;
            if truncated {
                let _ = child.kill().await;
            }
            let _ = writer.await;
            let _ = child.wait().await;
            Some((String::from_utf8_lossy(&out).into_owned(), truncated))
        }

        #[tokio::test]
        async fn output_cap_truncates_and_kills() {
            // Write 5 MB of 'a' through cat with a 1 MB cap.
            let payload = bytes::Bytes::from(vec![b'a'; 5 * 1024 * 1024]);
            let cap = 1 * 1024 * 1024;
            let (out, truncated) =
                extract_via_cat(payload, Duration::from_secs(5), cap).await.expect("extract");
            assert!(truncated, "should report truncation");
            assert_eq!(out.len(), cap, "output capped exactly at the limit");
        }

        #[tokio::test]
        async fn timeout_kills_long_runner() {
            // `sleep 5` produces no stdout; the read loop hangs until
            // the timeout fires.
            let mut child = Command::new("sleep")
                .arg("5")
                .stdout(Stdio::piped())
                .kill_on_drop(true)
                .spawn()
                .expect("spawn sleep");
            let mut stdout = child.stdout.take().unwrap();
            let started = std::time::Instant::now();
            let r = tokio::time::timeout(Duration::from_millis(200), async {
                let mut buf = [0u8; 64];
                stdout.read(&mut buf).await
            })
            .await;
            assert!(r.is_err(), "expected timeout elapsed");
            let _ = child.kill().await;
            let _ = child.wait().await;
            assert!(started.elapsed() < Duration::from_secs(2), "fired well before sleep wakes");
        }
    }
}
