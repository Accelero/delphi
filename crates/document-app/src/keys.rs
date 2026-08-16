//! Object keys and event ids.
//!
//! Both are **permanent contracts**. Changing the key function orphans every
//! existing blob; changing the event id construction silently disables
//! `Nats-Msg-Id` dedupe, because a redelivery would produce a different id.

use sha2::{Digest, Sha256};

/// `tenants/<tenant_id>/blobs/<upload_id>/original`
///
/// A pure function of `(tenant_id, upload_id)`. No URL is ever persisted; the
/// projection's `current_blob` holds the `upload_id` and the key is rederived.
///
/// `upload_id` is a ULID minted with `Ulid::new()` — 48 bits of timestamp plus
/// 80 bits of randomness — so the path is not guessable. Never put a filename,
/// title, or any user-supplied string in it.
pub fn object_key(tenant_id: &str, upload_id: &str) -> String {
    format!("tenants/{tenant_id}/blobs/{upload_id}/original")
}

// There is deliberately no inverse of `object_key`. Nothing ever needs to read
// a tenant or an upload id back out of a key: the only code that did was the
// blob sweeper, which listed the bucket and asked "whose is this?". Blobs are
// kept now, so every key in the system is reached from a record that already
// knows both halves.

/// Deterministic id for the event a completed upload produces.
///
/// Derived from `(tenant_id, upload_id, kind)` so that every redelivery of the
/// same work item produces the same id and JetStream deduplicates it.
fn event_id(tenant_id: &str, upload_id: &str, kind: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(tenant_id.as_bytes());
    hasher.update(b"|");
    hasher.update(upload_id.as_bytes());
    hasher.update(b"|");
    hasher.update(kind.as_bytes());
    hex::encode(&hasher.finalize()[..16])
}

pub fn created_event_id(tenant_id: &str, upload_id: &str) -> String {
    event_id(tenant_id, upload_id, "created")
}

pub fn blob_validated_event_id(tenant_id: &str, upload_id: &str) -> String {
    event_id(tenant_id, upload_id, "blob_validated")
}

/// `upload-completed:<tenant>:<upload_id>` — the work item's `Nats-Msg-Id`.
///
/// Derived from the upload id alone, so a second `/complete` with a different
/// parts list is deduped and ignored: first part list wins.
pub fn upload_completed_command_id(tenant_id: &str, upload_id: &str) -> String {
    format!("upload-completed:{tenant_id}:{upload_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_object_key_scheme_is_pinned() {
        // Changing this orphans every blob already in storage.
        assert_eq!(
            object_key("acme", "01JZ8QM2"),
            "tenants/acme/blobs/01JZ8QM2/original"
        );
    }

    #[test]
    fn event_ids_are_deterministic_and_distinct_per_kind() {
        assert_eq!(
            created_event_id("acme", "u1"),
            created_event_id("acme", "u1")
        );
        assert_ne!(
            created_event_id("acme", "u1"),
            blob_validated_event_id("acme", "u1")
        );
        assert_ne!(
            created_event_id("acme", "u1"),
            created_event_id("acme", "u2")
        );
        assert_ne!(
            created_event_id("acme", "u1"),
            created_event_id("other", "u1")
        );
    }

    #[test]
    fn the_tenant_upload_split_is_unambiguous() {
        // Without the separator, ("ab", "c") and ("a", "bc") would collide.
        assert_ne!(created_event_id("ab", "c"), created_event_id("a", "bc"));
    }

    #[test]
    fn command_ids_ignore_the_parts_list() {
        assert_eq!(
            upload_completed_command_id("acme", "u1"),
            "upload-completed:acme:u1"
        );
    }
}
