//! The `/complete` completion pipeline — the post-upload workflow as code.
//!
//! The whole point of this module: the post-upload sequence must be
//! **legible top-to-bottom**. The `/complete` handler stays thin (CAS the
//! state, call [`run_completion`], map the result to HTTP). The ordered
//! stages live in one function whose body *is* the workflow. Each stage is
//! a named call, not inlined logic, so the sequence stays the
//! documentation.
//!
//! Stages 4 and 9 are the fatal-reject points (wipe S3 + log rejection).
//! Stages 5, 6, 7 degrade gracefully — the bytes are already committed and
//! the user can edit metadata later, so an extraction/autofill hiccup must
//! never lose a good upload.

use std::sync::Arc;

use crate::auth::AuthContext;
use crate::object_store::ObjectStore;
use crate::storage::{AuthedDb, Content, DocId, Document, Storage, SystemDb, UploadSession};

use super::autofill::{
    merge_metadata, DocumentPrefill, ExtractionContext, MetadataExtractor, MergedMetadata,
};
use super::text_extract::extract_text;
use super::validation::{
    validate_descriptive_metadata, validate_uploaded_object, DescriptiveView, MetadataPolicy,
    ObjectPolicy, ObjectReject,
};

/// Everything the ordered stages need. Carries **both** DB handles:
/// `AuthedDb` for the CAS/commit (engine PERMISSIONS scope to the caller)
/// and `SystemDb` for the rejection write (the `ingestion_rejection` table
/// denies user-session writes, so rejections go through the system path).
pub struct CompletionCtx<'a> {
    pub object_store: &'a dyn ObjectStore,
    pub authed_db: &'a AuthedDb,
    pub system_db: &'a Arc<SystemDb>,
    pub auth: &'a AuthContext,
    pub session: &'a UploadSession,
    pub extractor: &'a dyn MetadataExtractor,
    pub policy: &'a MetadataPolicy,
    pub object_policy: &'a ObjectPolicy,
    pub prefill: &'a DocumentPrefill,
    /// Bucket the canonical `storage_uri` is rendered against
    /// (`UploadsConfig.bucket`).
    pub bucket: &'a str,
}

/// Terminal outcome of the pipeline. The handler maps each to HTTP.
#[derive(Debug)]
pub enum CompletionError {
    /// Stage 4 — the committed object failed validation. Wipe S3 + log.
    ObjectRejected(ObjectReject),
    /// Stage 9 — merged descriptive metadata failed the final gate.
    /// Wipe S3 + log. Reason is a stable short code.
    MetadataRejected(String),
    /// Stage 10 — a write to the DB failed after the bytes were good.
    /// The session/object become orphans the cleaner reaps; the SPA can
    /// recover via the status poll.
    CommitFailed(String),
    /// Stage 10 — `canonical_id` collided with an existing row.
    CanonicalIdConflict { existing_doc_id: String },
}

/// Ordered post-upload ingestion stages. The order is load-bearing; read
/// it top-to-bottom.
pub async fn run_completion(ctx: &CompletionCtx<'_>) -> Result<DocId, CompletionError> {
    // 4. Bytes are committed + the object is sound.
    let validated = validate_uploaded_object(
        &ctx.session.s3_key,
        ctx.session.declared_size as u64,
        &ctx.session.declared_content_type,
        ctx.object_store,
        ctx.object_policy,
    )
    .await
    .map_err(CompletionError::ObjectRejected)?;

    // 5. Extract raw text (LLM needs something to read; also persisted).
    //    Extraction failure ⇒ empty text, non-fatal.
    let content = extract_text(
        ctx.object_store,
        &ctx.session.s3_key,
        &ctx.session.declared_content_type,
        ctx.object_policy.pdf_max_input_bytes,
    )
    .await
    .unwrap_or_else(|e| {
        tracing::warn!(error = %e, key = %ctx.session.s3_key, "text extraction failed; continuing with empty text");
        Content {
            text: String::new(),
            format: "text".into(),
            extractor: "none".into(),
        }
    });

    // 6. Autofill from text + prefill (deferred LLM; noop today).
    //    Autofill failure ⇒ prefill-only, non-fatal.
    let autofilled = ctx
        .extractor
        .extract(&ExtractionContext {
            text: &content.text,
            prefill: ctx.prefill,
        })
        .await
        .unwrap_or_default();

    // 7. Validate the (untrusted) autofill output before merge. Invalid
    //    autofill is dropped, not fatal.
    let autofilled = {
        let view = DescriptiveView {
            title: autofilled.title.as_deref(),
            authors: &autofilled.authors,
            summary: autofilled.summary.as_deref(),
            language: autofilled.language.as_deref(),
            published_at: autofilled.published_at,
            extra: Some(&autofilled.extra),
        };
        if validate_descriptive_metadata(&view, ctx.policy).is_ok() {
            autofilled
        } else {
            tracing::warn!(key = %ctx.session.s3_key, "autofill output failed validation; dropping");
            Default::default()
        }
    };

    // 8. Merge: prefill wins; unset optional fields stay unset.
    let merged = merge_metadata(ctx.prefill, &autofilled);

    // 9. Final gate: app-required fields present + shape valid.
    {
        let view = DescriptiveView {
            title: merged.title.as_deref(),
            authors: &merged.authors,
            summary: merged.summary.as_deref(),
            language: merged.language.as_deref(),
            published_at: merged.published_at,
            extra: Some(&merged.extra),
        };
        validate_descriptive_metadata(&view, ctx.policy)
            .map_err(|r| CompletionError::MetadataRejected(format!("{r:?}")))?;
    }

    // 10. Commit: document:<doc_id> (+ document_content) + session delete.
    commit(ctx, &validated.etag, &merged, content).await
}

async fn commit(
    ctx: &CompletionCtx<'_>,
    etag: &str,
    merged: &MergedMetadata,
    content: Content,
) -> Result<DocId, CompletionError> {
    let doc = Document {
        id: None,
        tenant_id: None,
        // Manual uploads carry no canonical_id (identity is the record id).
        // The session's canonical_id (set only for natural-source writers)
        // is preserved when present.
        canonical_id: ctx.session.canonical_id.clone(),
        source_type: ctx.session.source_type.clone(),
        source_uri: ctx.session.source_uri.clone(),
        storage_uri: Some(crate::object_store::storage_uri_for_key(
            ctx.bucket,
            &ctx.session.s3_key,
        )),
        title: merged.title.clone(),
        authors: merged.authors.clone(),
        published_at: merged.published_at,
        ingested_at: None,
        language: merged.language.clone(),
        summary: merged.summary.clone(),
        paper_embedding: None,
        paper_embedding_model: None,
        // ETag is opaque/informational, not a stable content hash (S3
        // multipart ETags are md5-of-md5s-N). Manual uploads are not
        // deduped on it.
        content_hash: etag.trim_matches('"').to_string(),
        version: 1,
        metadata: ensure_object(merged.extra.clone()),
    };

    let dedup = crate::storage::dedup_key(&ctx.auth.tenant_id, doc.canonical_id.as_deref());
    match ctx
        .authed_db
        .commit_upload(&ctx.session.doc_id, &doc, &content, dedup.as_deref())
        .await
    {
        Ok(id) => Ok(id),
        Err(crate::error::Error::CanonicalIdConflict { existing_doc_id }) => {
            Err(CompletionError::CanonicalIdConflict { existing_doc_id })
        }
        Err(e) => Err(CompletionError::CommitFailed(e.to_string())),
    }
}

/// Surreal's `metadata` column is `FLEXIBLE TYPE object`; coerce non-object
/// values to an empty object so the commit tolerates odd extractor output.
fn ensure_object(v: serde_json::Value) -> serde_json::Value {
    if v.is_object() {
        v
    } else {
        serde_json::Value::Object(serde_json::Map::new())
    }
}
