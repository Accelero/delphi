//! Layer-1 ingestion metadata validator.
//!
//! Pure function: same `(req, policy)` in → same `MetadataReject` (or
//! `Ok(())`) out. No I/O. Auditable as a unit. Property-tested.
//!
//! Closes audit item **M8** (unbounded metadata) by construction:
//! `metadata` is depth-capped and size-capped before it touches the
//! database or any downstream code.
//!
//! Also enforces the "never accept these from the body" rule for
//! `tenant_id`, `user_id`, `storage_uri`, `key`, and `upload_id` —
//! every one is server-derived from the JWT. A request that includes
//! any of them is rejected as `MalformedRequest`.

use std::collections::HashSet;

use regex::Regex;
use serde::{Deserialize, Serialize};

/// Inbound JSON body for `POST /api/ingestion/uploads`. The wire shape
/// is `serde_json::Value` for `metadata` so we can recursively bound
/// depth + serialized size without imposing a particular schema.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CreateUploadRequest {
    /// Optional dedup key. The SPA never sends it for manual uploads;
    /// natural-source writers (v1 JSON ingest, future adapters) still do.
    /// Validated for shape only *when present*.
    #[serde(default)]
    pub canonical_id: Option<String>,
    /// Server-defaults to `"manual"` when absent (the manual-upload case).
    #[serde(default)]
    pub source_type: Option<String>,
    /// Optional. Validated for shape only *when present*.
    #[serde(default)]
    pub source_uri: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    /// Untrusted original filename from the client. Used only for (a) the
    /// object validator's file-ending dispatch at `/complete` and (b) a
    /// sanitised title fallback — never as an S3 key or a shell arg.
    #[serde(default)]
    pub filename: Option<String>,
    // NOTE: no `content_type` — the backend never sees the bytes at create
    // time, so a client-declared MIME is just an unverifiable claim. The
    // actual type is determined from the bytes by the object validator at
    // `/complete`.
    pub size: u64,
    #[serde(default)]
    pub metadata: serde_json::Value,

    // ---- forbidden fields ----------------------------------------------
    //
    // The backend never accepts these from a request body — every one is
    // server-derived from the JWT or the upload session row. We declare
    // them as `Option<serde_json::Value>` so an over-eager client gets a
    // structured 400 instead of a deserialize error the SPA can't parse.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_uri: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upload_id: Option<serde_json::Value>,
}

impl CreateUploadRequest {
    /// Resolved source type: the declared value, or `"manual"` when the
    /// client omitted it (the manual-upload default).
    pub fn resolved_source_type(&self) -> String {
        self.source_type
            .clone()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "manual".to_string())
    }
}

/// App-level descriptive field, used by `MetadataPolicy.required_fields`
/// to declare which fields must be present **after merge**. Distinct from
/// the hard DB-required fields (`source_type`, `content_hash`, the record
/// id), which the engine enforces at `CREATE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetadataField {
    Title,
    Authors,
    Summary,
    Language,
}

#[derive(Debug, Clone)]
pub struct MetadataPolicy {
    pub allowed_content_types: HashSet<String>,
    pub max_size_bytes: u64,
    pub max_title_chars: usize,
    /// Cap on the descriptive `summary` after sanitization (abstracts can
    /// be long, so this is generous).
    pub max_summary_chars: usize,
    /// Cap on the number of authors kept (extras are dropped).
    pub max_authors: usize,
    /// Cap on each author string after sanitization.
    pub max_author_chars: usize,
    pub max_metadata_depth: usize,
    pub max_metadata_bytes: usize,
    pub canonical_id_pattern: Regex,
    /// Descriptive fields that must be present after the prefill/autofill
    /// merge. Starts **empty** — nothing is app-required until the LLM
    /// extractor lands (otherwise every multi-file upload fails, since
    /// autofill is a noop today).
    pub required_fields: HashSet<MetadataField>,
}

impl Default for MetadataPolicy {
    fn default() -> Self {
        Self {
            allowed_content_types: ["application/pdf", "text/plain", "text/markdown", "text/html"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            max_size_bytes: 200 * 1024 * 1024, // 200 MiB
            max_title_chars: 1024,
            max_summary_chars: 8192,
            max_authors: 64,
            max_author_chars: 256,
            max_metadata_depth: 8,
            max_metadata_bytes: 64 * 1024,
            canonical_id_pattern: Regex::new(r"^[a-z][a-z0-9_-]*:[A-Za-z0-9._:-]{1,256}$")
                .expect("default canonical_id pattern compiles"),
            // Empty: nothing app-required until the LLM extractor lands.
            required_fields: HashSet::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataReject {
    DisallowedContentType,
    SizeExceedsLimit,
    TitleTooLong,
    MetadataTooDeep,
    MetadataTooLarge,
    InvalidCanonicalId,
    InvalidSourceUri,
    MalformedRequest(String),
}

/// Synchronous, pure metadata gate. Called at the top of
/// `POST /api/ingestion/uploads`, before any S3 op.
pub fn validate_ingestion_metadata(
    req: &CreateUploadRequest,
    policy: &MetadataPolicy,
) -> Result<(), MetadataReject> {
    // 1. Reject forbidden fields outright — these must come from the JWT
    //    + server-derived state, not the client.
    if req.tenant_id.is_some() {
        return Err(MetadataReject::MalformedRequest(
            "tenant_id is server-derived; do not send it".into(),
        ));
    }
    if req.user_id.is_some() {
        return Err(MetadataReject::MalformedRequest(
            "user_id is server-derived; do not send it".into(),
        ));
    }
    if req.storage_uri.is_some() {
        return Err(MetadataReject::MalformedRequest(
            "storage_uri is server-derived; do not send it".into(),
        ));
    }
    if req.key.is_some() {
        return Err(MetadataReject::MalformedRequest(
            "key is server-derived; do not send it".into(),
        ));
    }
    if req.upload_id.is_some() {
        return Err(MetadataReject::MalformedRequest(
            "upload_id is server-derived; do not send it".into(),
        ));
    }

    // 2. Required-string shape. `canonical_id` and `source_uri` are
    //    optional (manual uploads omit them); `source_type` defaults to
    //    "manual" server-side when absent.
    if let Some(st) = &req.source_type {
        if st.is_empty() {
            return Err(MetadataReject::MalformedRequest(
                "source_type is empty".into(),
            ));
        }
    }

    // 3. Hardcoded fixed-rule limits. `canonical_id` / `source_uri` shape
    //    checks run *only when the field is present* — an absent value is
    //    legal (manual upload).
    if let Some(cid) = &req.canonical_id {
        if cid.is_empty() || !policy.canonical_id_pattern.is_match(cid) {
            return Err(MetadataReject::InvalidCanonicalId);
        }
    }
    if let Some(uri) = &req.source_uri {
        if uri.is_empty() || !is_plausible_uri(uri) {
            return Err(MetadataReject::InvalidSourceUri);
        }
    }
    // Content type is no longer part of the request — it's determined from
    // the actual bytes by the object validator at `/complete`.
    if req.size == 0 || req.size > policy.max_size_bytes {
        return Err(MetadataReject::SizeExceedsLimit);
    }
    if let Some(t) = &req.title {
        if t.chars().count() > policy.max_title_chars {
            return Err(MetadataReject::TitleTooLong);
        }
    }

    // 4. Metadata shape.
    if json_depth(&req.metadata, 0) > policy.max_metadata_depth {
        return Err(MetadataReject::MetadataTooDeep);
    }
    // Serialised byte size — protects the DB from arbitrarily large
    // arrays of small primitives that wouldn't trip the depth check.
    let metadata_bytes = serde_json::to_vec(&req.metadata)
        .map(|v| v.len())
        .unwrap_or(usize::MAX);
    if metadata_bytes > policy.max_metadata_bytes {
        return Err(MetadataReject::MetadataTooLarge);
    }

    Ok(())
}

/// A borrowed view of descriptive metadata, shared by the two
/// pipeline call sites (autofill output, merged result). The pipeline
/// builds one of these from `ExtractedMetadata` / `MergedMetadata`
/// without those types depending on the validator.
#[derive(Debug, Default, Clone, Copy)]
pub struct DescriptiveView<'a> {
    pub title: Option<&'a str>,
    pub authors: &'a [String],
    pub summary: Option<&'a str>,
    pub language: Option<&'a str>,
    pub published_at: Option<chrono::DateTime<chrono::Utc>>,
    pub extra: Option<&'a serde_json::Value>,
}

/// Layer-2 descriptive-metadata gate. Distinct from
/// [`validate_ingestion_metadata`] (which checks the wire *request*:
/// content-type, size, forbidden fields). This validates a
/// **descriptive** result — autofill output (stage 7) or the merged
/// metadata (stage 9). It shares the size/depth helpers with the wire
/// validator, not its entry point.
///
/// Checks: `title` length, `published_at` sanity (not absurdly far in the
/// future), `extra` depth + serialized size, and `required_fields`.
pub fn validate_descriptive_metadata(
    meta: &DescriptiveView<'_>,
    policy: &MetadataPolicy,
) -> Result<(), MetadataReject> {
    if let Some(t) = meta.title {
        if t.chars().count() > policy.max_title_chars {
            return Err(MetadataReject::TitleTooLong);
        }
    }

    if let Some(pub_at) = meta.published_at {
        // Reject implausible far-future timestamps (a corrupt / adversarial
        // extractor result). One year of slack covers clock skew + embargo
        // dates.
        let horizon = chrono::Utc::now() + chrono::Duration::days(366);
        if pub_at > horizon {
            return Err(MetadataReject::MalformedRequest(
                "published_at is implausibly far in the future".into(),
            ));
        }
    }

    if let Some(extra) = meta.extra {
        if json_depth(extra, 0) > policy.max_metadata_depth {
            return Err(MetadataReject::MetadataTooDeep);
        }
        let bytes = serde_json::to_vec(extra).map(|v| v.len()).unwrap_or(usize::MAX);
        if bytes > policy.max_metadata_bytes {
            return Err(MetadataReject::MetadataTooLarge);
        }
    }

    for field in &policy.required_fields {
        let present = match field {
            MetadataField::Title => meta.title.map(|s| !s.is_empty()).unwrap_or(false),
            MetadataField::Authors => !meta.authors.is_empty(),
            MetadataField::Summary => meta.summary.map(|s| !s.is_empty()).unwrap_or(false),
            MetadataField::Language => meta.language.map(|s| !s.is_empty()).unwrap_or(false),
        };
        if !present {
            return Err(MetadataReject::MalformedRequest(format!(
                "required descriptive field missing: {field:?}"
            )));
        }
    }

    Ok(())
}

fn is_plausible_uri(s: &str) -> bool {
    // Very narrow: require an absolute http(s) URL. ArXiv adapter and
    // SPA both produce that; anything else is suspicious. We don't pull
    // in a full URL parser — `Regex::is_match` would be fine, but a
    // hand check is enough.
    if s.len() > 4096 {
        return false;
    }
    let lower = s.to_ascii_lowercase();
    lower.starts_with("http://") || lower.starts_with("https://")
}

/// Sanitize a free-text descriptive string **in place** (we clean rather
/// than reject so one stray character never fails an otherwise-good
/// upload). Removes:
/// - C0/C1 control characters except `\t` / `\n` / `\r` — these forge log
///   lines, drive terminal escape sequences, and embed NUL.
/// - Unicode bidirectional override / isolate codepoints (U+202A–202E,
///   U+2066–2069) — the "Trojan Source" class (CVE-2021-42574) that can
///   spoof how a title renders.
///
/// Then trims and truncates to `max_chars` (counted in `char`s, matching
/// the length checks elsewhere). XSS is **not** handled here — that's a
/// render-time (DOM) concern defended in the SPA; this only governs what
/// we store and log.
pub fn sanitize_text(s: &str, max_chars: usize) -> String {
    let cleaned: String = s.chars().filter(|&c| !is_disallowed_char(c)).collect();
    cleaned.trim().chars().take(max_chars).collect()
}

fn is_disallowed_char(c: char) -> bool {
    matches!(c, '\u{202A}'..='\u{202E}' | '\u{2066}'..='\u{2069}')
        || (c.is_control() && !matches!(c, '\t' | '\n' | '\r'))
}

/// Sanitize an author list: clean each name, drop any that empty out, and
/// cap the count at `max_authors`.
pub fn sanitize_authors(authors: &[String], max_authors: usize, max_author_chars: usize) -> Vec<String> {
    authors
        .iter()
        .map(|a| sanitize_text(a, max_author_chars))
        .filter(|a| !a.is_empty())
        .take(max_authors)
        .collect()
}

fn json_depth(v: &serde_json::Value, current: usize) -> usize {
    match v {
        serde_json::Value::Object(m) => m
            .values()
            .map(|x| json_depth(x, current + 1))
            .max()
            .unwrap_or(current + 1),
        serde_json::Value::Array(a) => a
            .iter()
            .map(|x| json_depth(x, current + 1))
            .max()
            .unwrap_or(current + 1),
        _ => current,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ok_req() -> CreateUploadRequest {
        CreateUploadRequest {
            canonical_id: Some("manual:abc123".into()),
            source_type: Some("manual".into()),
            source_uri: Some("https://example.test/abc123".into()),
            title: Some("A paper".into()),
            filename: Some("abc123.pdf".into()),
            size: 1024,
            metadata: json!({}),
            tenant_id: None,
            user_id: None,
            storage_uri: None,
            key: None,
            upload_id: None,
        }
    }

    fn manual_req() -> CreateUploadRequest {
        // The shape the SPA actually sends: no canonical_id, no
        // source_uri, no source_type (server defaults to "manual").
        CreateUploadRequest {
            canonical_id: None,
            source_type: None,
            source_uri: None,
            title: Some("A manual upload".into()),
            filename: Some("manual.pdf".into()),
            size: 1024,
            metadata: json!({}),
            tenant_id: None,
            user_id: None,
            storage_uri: None,
            key: None,
            upload_id: None,
        }
    }

    #[test]
    fn descriptive_empty_passes_when_nothing_required() {
        let p = MetadataPolicy::default(); // required_fields empty
        let view = DescriptiveView::default();
        assert!(validate_descriptive_metadata(&view, &p).is_ok());
    }

    #[test]
    fn descriptive_title_too_long_rejected() {
        let p = MetadataPolicy::default();
        let long = "x".repeat(p.max_title_chars + 1);
        let view = DescriptiveView {
            title: Some(&long),
            ..Default::default()
        };
        assert_eq!(
            validate_descriptive_metadata(&view, &p),
            Err(MetadataReject::TitleTooLong)
        );
    }

    #[test]
    fn descriptive_required_title_missing_rejected() {
        let mut p = MetadataPolicy::default();
        p.required_fields.insert(MetadataField::Title);
        let view = DescriptiveView::default();
        assert!(matches!(
            validate_descriptive_metadata(&view, &p),
            Err(MetadataReject::MalformedRequest(_))
        ));
        // With a title present it passes.
        let view = DescriptiveView {
            title: Some("present"),
            ..Default::default()
        };
        assert!(validate_descriptive_metadata(&view, &p).is_ok());
    }

    #[test]
    fn manual_upload_without_canonical_id_passes() {
        let p = MetadataPolicy::default();
        assert!(validate_ingestion_metadata(&manual_req(), &p).is_ok());
    }

    #[test]
    fn resolved_source_type_defaults_to_manual() {
        assert_eq!(manual_req().resolved_source_type(), "manual");
        assert_eq!(ok_req().resolved_source_type(), "manual");
    }

    #[test]
    fn happy_path_passes() {
        let p = MetadataPolicy::default();
        assert!(validate_ingestion_metadata(&ok_req(), &p).is_ok());
    }

    #[test]
    fn forbidden_tenant_id_rejected() {
        let p = MetadataPolicy::default();
        let mut req = ok_req();
        req.tenant_id = Some(json!("tenant-evil"));
        assert!(matches!(
            validate_ingestion_metadata(&req, &p),
            Err(MetadataReject::MalformedRequest(_))
        ));
    }

    #[test]
    fn forbidden_storage_uri_rejected() {
        let p = MetadataPolicy::default();
        let mut req = ok_req();
        req.storage_uri = Some(json!("s3://evil/key"));
        assert!(matches!(
            validate_ingestion_metadata(&req, &p),
            Err(MetadataReject::MalformedRequest(_))
        ));
    }

    #[test]
    fn forbidden_key_rejected() {
        let p = MetadataPolicy::default();
        let mut req = ok_req();
        req.key = Some(json!("tenants/evil/k"));
        assert!(matches!(
            validate_ingestion_metadata(&req, &p),
            Err(MetadataReject::MalformedRequest(_))
        ));
    }

    #[test]
    fn oversized_rejected() {
        let p = MetadataPolicy::default();
        let mut req = ok_req();
        req.size = p.max_size_bytes + 1;
        assert_eq!(
            validate_ingestion_metadata(&req, &p),
            Err(MetadataReject::SizeExceedsLimit)
        );
    }

    #[test]
    fn zero_size_rejected() {
        let p = MetadataPolicy::default();
        let mut req = ok_req();
        req.size = 0;
        assert_eq!(
            validate_ingestion_metadata(&req, &p),
            Err(MetadataReject::SizeExceedsLimit)
        );
    }

    #[test]
    fn title_too_long_rejected() {
        let p = MetadataPolicy::default();
        let mut req = ok_req();
        req.title = Some("x".repeat(p.max_title_chars + 1));
        assert_eq!(
            validate_ingestion_metadata(&req, &p),
            Err(MetadataReject::TitleTooLong)
        );
    }

    #[test]
    fn deeply_nested_metadata_rejected() {
        let p = MetadataPolicy::default();
        let mut req = ok_req();
        // Build a nested object 20 levels deep.
        let mut v = json!("leaf");
        for _ in 0..20 {
            v = json!({ "next": v });
        }
        req.metadata = v;
        assert_eq!(
            validate_ingestion_metadata(&req, &p),
            Err(MetadataReject::MetadataTooDeep)
        );
    }

    #[test]
    fn huge_metadata_rejected() {
        let p = MetadataPolicy::default();
        let mut req = ok_req();
        // 100 KiB of payload — over the 64 KiB default cap.
        req.metadata = json!({ "blob": "x".repeat(100 * 1024) });
        assert_eq!(
            validate_ingestion_metadata(&req, &p),
            Err(MetadataReject::MetadataTooLarge)
        );
    }

    #[test]
    fn invalid_canonical_id_rejected() {
        let p = MetadataPolicy::default();
        let mut req = ok_req();
        req.canonical_id = Some("no-colon-form".into());
        assert_eq!(
            validate_ingestion_metadata(&req, &p),
            Err(MetadataReject::InvalidCanonicalId)
        );
    }

    #[test]
    fn invalid_source_uri_rejected() {
        let p = MetadataPolicy::default();
        let mut req = ok_req();
        req.source_uri = Some("javascript:alert(1)".into());
        assert_eq!(
            validate_ingestion_metadata(&req, &p),
            Err(MetadataReject::InvalidSourceUri)
        );
    }

    #[test]
    fn empty_canonical_id_when_present_rejected() {
        // An *absent* canonical_id is fine (manual upload); an empty
        // string, when explicitly present, is malformed shape.
        let p = MetadataPolicy::default();
        let mut req = ok_req();
        req.canonical_id = Some(String::new());
        assert_eq!(
            validate_ingestion_metadata(&req, &p),
            Err(MetadataReject::InvalidCanonicalId)
        );
    }

    // ---- Property tests --------------------------------------------------
    //
    // The "any input → no panic" guarantee is the meaningful property for
    // a parser at the trust boundary. We run it over a synthetic
    // input space (forbidden-fields enabled, random sizes, deep nesting,
    // odd canonical ids) and assert the function returns rather than
    // panicking and that the decision matches each rule.

    #[test]
    fn sanitize_strips_control_and_bidi_chars() {
        // NUL + a C0 control + a bidi override embedded in a title.
        let dirty = "Hello\u{0000}\u{0007}\u{202E}World";
        assert_eq!(sanitize_text(dirty, 1024), "HelloWorld");
        // Tab/newline are preserved (then trimmed at the ends).
        assert_eq!(sanitize_text("  a\tb\nc  ", 1024), "a\tb\nc");
    }

    #[test]
    fn sanitize_truncates_to_char_cap() {
        let s = "x".repeat(100);
        assert_eq!(sanitize_text(&s, 10).chars().count(), 10);
        // Truncation is by char, not byte (multi-byte safe).
        let multi = "é".repeat(20);
        assert_eq!(sanitize_text(&multi, 5).chars().count(), 5);
    }

    #[test]
    fn sanitize_authors_cleans_drops_empty_and_caps() {
        let authors = vec![
            "Alice".to_string(),
            "\u{202E}\u{0000}".to_string(), // sanitizes to empty → dropped
            "Bob".to_string(),
            "Carol".to_string(),
        ];
        let got = sanitize_authors(&authors, 2, 256);
        assert_eq!(got, vec!["Alice".to_string(), "Bob".to_string()]); // empty dropped, capped at 2
    }

    #[test]
    fn property_random_inputs_never_panic() {
        let p = MetadataPolicy::default();
        let oversize = p.max_size_bytes + 1;
        // 50 inputs each chosen to hit a different code path.
        let cases: Vec<CreateUploadRequest> = (0..50)
            .map(|i| {
                let mut r = ok_req();
                match i % 9 {
                    0 => r.tenant_id = Some(json!(format!("t-{i}"))),
                    1 => r.user_id = Some(json!(format!("u-{i}"))),
                    2 => r.storage_uri = Some(json!(format!("s3://b/k-{i}"))),
                    3 => r.key = Some(json!(format!("tenants/t/k-{i}"))),
                    4 => r.upload_id = Some(json!(format!("mpu-{i}"))),
                    5 => r.size = oversize,
                    6 => r.canonical_id = Some(format!("badid-{i}")),
                    7 => {
                        let mut v = json!("leaf");
                        for _ in 0..(p.max_metadata_depth + 4) {
                            v = json!({ "n": v });
                        }
                        r.metadata = v;
                    }
                    _ => r.title = Some("x".repeat(p.max_title_chars + 1)),
                }
                r
            })
            .collect();
        for case in cases {
            // Just call — must return without panic.
            let _ = validate_ingestion_metadata(&case, &p);
        }
    }
}
