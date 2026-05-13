//! Drives the **real** `ArxivAdapter` against a local axum server that
//! impersonates the arXiv API. Proves the Atom-XML → `IngestRequestBody`
//! pipeline (search request, status handling, body parsing, PDF
//! download with the size cap) end-to-end without touching the network.
//!
//! What this covers that unit tests don't:
//! - The full `fetch()` loop, not just XML parsing in isolation.
//! - The HTTP query-string shape (`search_query`, `sortBy`, etc.) is
//!   what arXiv would receive — verified by the fake echoing the
//!   query back into the response title for one entry.
//! - PDF fetch + cap behaviour: one entry has a small PDF, one has an
//!   over-cap PDF that must be skipped (storage_uri = None).
//! - Cursor advance: `Fetched.next_cursor` reflects the newest
//!   `published` we observed.

mod common;

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use serde::Deserialize;

use delphi::object_store::MemObjectStore;
use delphi::object_store::ObjectStore;
use delphi::sources::{ArxivAdapter, SourceAdapter};

#[derive(Clone)]
struct FakeArxiv {
    /// Atom feed served at GET /. The `{search_query}` placeholder is
    /// replaced with the actual query the adapter sent, so the test
    /// can assert the wire shape.
    feed_template: String,
    /// PDF body served at GET /pdf/small. Tiny so download succeeds
    /// under the cap.
    small_pdf: Vec<u8>,
    /// PDF body served at GET /pdf/big. Larger than the test's cap;
    /// download must abort before the body finishes.
    big_pdf: Vec<u8>,
}

#[derive(Deserialize)]
struct SearchQuery {
    search_query: String,
    // arXiv's API uses camelCase param names; serde rename keeps the
    // assertion below honest about the wire shape.
    #[serde(default, rename = "sortBy")]
    sort_by: Option<String>,
    #[serde(default, rename = "sortOrder")]
    sort_order: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    start: Option<String>,
    #[serde(default, rename = "max_results")]
    max_results: Option<String>,
}

async fn search_handler(
    State(state): State<FakeArxiv>,
    Query(q): Query<SearchQuery>,
) -> Response {
    // Sanity-check the query the adapter constructed so a future
    // accidental rename of one of the constants in `arxiv.rs` makes
    // this test fail loudly rather than silently swap to a different
    // arXiv API contract.
    assert!(q.search_query.contains("submittedDate:["));
    assert_eq!(q.sort_by.as_deref(), Some("submittedDate"));
    assert_eq!(q.sort_order.as_deref(), Some("ascending"));
    assert!(q.max_results.is_some());

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/atom+xml")],
        state.feed_template.replace("{search_query}", &q.search_query),
    )
        .into_response()
}

async fn small_pdf(State(state): State<FakeArxiv>) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/pdf")],
        state.small_pdf,
    )
        .into_response()
}

async fn big_pdf(State(state): State<FakeArxiv>) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/pdf")],
        state.big_pdf,
    )
        .into_response()
}

/// Bind a listener on a free port (so the test can build URLs that
/// reference itself) and return both the URL and the listener for the
/// caller to spawn `axum::serve` on once the rest of `state` is wired
/// up against that URL.
async fn bind_fake_arxiv() -> (String, tokio::net::TcpListener) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    (format!("http://{addr}"), listener)
}

fn fake_arxiv_router(state: FakeArxiv) -> Router {
    Router::new()
        .route("/api/query", get(search_handler))
        .route("/pdf/small", get(small_pdf))
        .route("/pdf/big", get(big_pdf))
        .with_state(state)
}

const ATOM_FIXTURE: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>arXiv Query: {search_query}</title>
  <id>http://arxiv.org/api/query</id>
  <entry>
    <id>http://arxiv.org/abs/2401.00001v1</id>
    <updated>2024-01-02T00:00:00Z</updated>
    <published>2024-01-01T12:00:00Z</published>
    <title>Small Paper</title>
    <summary>A paper with a small PDF that fits under the cap.</summary>
    <author><name>Alice Author</name></author>
    <link href="http://arxiv.org/abs/2401.00001v1" rel="alternate" type="text/html"/>
    <link title="pdf" href="{pdf_small}" rel="related" type="application/pdf"/>
    <category term="cs.CL"/>
  </entry>
  <entry>
    <id>http://arxiv.org/abs/2401.00002v1</id>
    <updated>2024-01-03T00:00:00Z</updated>
    <published>2024-01-02T18:00:00Z</published>
    <title>Big Paper</title>
    <summary>A paper whose PDF exceeds our test cap.</summary>
    <author><name>Bob Author</name></author>
    <link href="http://arxiv.org/abs/2401.00002v1" rel="alternate" type="text/html"/>
    <link title="pdf" href="{pdf_big}" rel="related" type="application/pdf"/>
    <category term="cs.LG"/>
  </entry>
</feed>
"#;

#[tokio::test]
async fn arxiv_adapter_against_fake_server() {
    // Tiny PDF body — content doesn't matter, just that it's < cap and
    // pdftotext won't try to make sense of it (extraction is a soft
    // failure path; we don't assert on `raw_text`).
    let small_pdf = b"%PDF-1.4\n%not really a pdf\n".to_vec();
    // 200 KB — comfortably above our 64 KB test cap.
    let big_pdf = vec![0xABu8; 200 * 1024];

    // Bind first to learn the port, then build the feed (which embeds
    // the same URL for its PDF links), then start serving.
    let (base_url, listener) = bind_fake_arxiv().await;
    let state = FakeArxiv {
        feed_template: ATOM_FIXTURE
            .replace("{pdf_small}", &format!("{base_url}/pdf/small"))
            .replace("{pdf_big}", &format!("{base_url}/pdf/big")),
        small_pdf,
        big_pdf,
    };
    let server_handle = tokio::spawn(async move {
        let _ = axum::serve(listener, fake_arxiv_router(state)).await;
    });

    // Build the real adapter via env (the only construction path).
    // Uses our fake endpoint and a tight 64 KB PDF cap so the big
    // entry is guaranteed to abort.
    std::env::set_var("ARXIV_QUERY", "cat:cs.CL");
    std::env::set_var("ARXIV_ENDPOINT", format!("{base_url}/api/query"));
    std::env::set_var("ARXIV_MAX_PDF_BYTES", "65536");
    std::env::set_var("ARXIV_HTTP_TIMEOUT_SECS", "5");
    let object_store: Arc<dyn ObjectStore> = Arc::new(MemObjectStore::new());
    let adapter =
        ArxivAdapter::try_from_env(object_store.clone()).expect("ARXIV_QUERY set");

    let fetched = adapter.fetch(None).await.expect("fetch");
    assert_eq!(
        fetched.items.len(),
        2,
        "both entries surface (metadata always lands)"
    );

    let small = fetched
        .items
        .iter()
        .find(|i| i.canonical_id == "arxiv:2401.00001v1")
        .expect("small paper");
    let big = fetched
        .items
        .iter()
        .find(|i| i.canonical_id == "arxiv:2401.00002v1")
        .expect("big paper");

    assert_eq!(small.title.as_deref(), Some("Small Paper"));
    assert_eq!(small.source_type, "arxiv");
    assert!(
        small.storage_uri.is_some(),
        "small PDF fits under the cap and lands in the object store"
    );

    assert_eq!(big.title.as_deref(), Some("Big Paper"));
    assert!(
        big.storage_uri.is_none(),
        "big PDF must abort before persistence (storage_uri = None)"
    );
    assert!(
        big.raw_text.is_none(),
        "no body extraction when the PDF was rejected"
    );

    let cursor = fetched.next_cursor.expect("cursor advances on non-empty");
    let last_pub = cursor
        .get("last_published")
        .and_then(|v| v.as_str())
        .expect("last_published present");
    // Should be the newer of the two entries.
    assert_eq!(last_pub, "2024-01-02T18:00:00+00:00");

    server_handle.abort();
}
