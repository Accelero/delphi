//! The document listing's keyset cursor.
//!
//! `updated_at` alone is **not** a key. Two documents can share a timestamp —
//! a batch import, or two uploads accepted in the same microsecond — and a
//! strict `updated_at < $cursor` then skips every row that ties with the last
//! row of the previous page. The tiebreaker is `document_id`, which is unique
//! within a tenant, so `(updated_at, document_id)` totally orders the listing.
//!
//! The wire form is opaque on purpose: it is a resumption token, not an API for
//! addressing a point in time, and keeping it opaque is what lets the ordering
//! key change later without breaking clients.

use chrono::{DateTime, Utc};

/// Where the previous page stopped. Rows *strictly after* this in
/// `(updated_at DESC, document_id DESC)` order form the next page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentCursor {
    pub updated_at: DateTime<Utc>,
    pub document_id: String,
}

impl DocumentCursor {
    /// `hex(<micros>:<document_id>)`. Hex rather than base64 only because it is
    /// already a dependency and needs no URL-safe alphabet decision.
    ///
    /// Microseconds, not millis: `timestamptz` stores microseconds, and a
    /// truncated cursor would sit *inside* a row's timestamp and either repeat
    /// or skip it.
    pub fn encode(&self) -> String {
        hex::encode(format!(
            "{}:{}",
            self.updated_at.timestamp_micros(),
            self.document_id
        ))
    }

    /// `None` for anything that is not a cursor this code emitted. Callers turn
    /// that into a `400`; silently treating a corrupt cursor as "start from the
    /// beginning" would loop a paging client forever.
    pub fn decode(value: &str) -> Option<Self> {
        let text = String::from_utf8(hex::decode(value).ok()?).ok()?;
        let (micros, document_id) = text.split_once(':')?;
        if document_id.is_empty() {
            return None;
        }
        Some(Self {
            updated_at: DateTime::from_timestamp_micros(micros.parse().ok()?)?,
            document_id: document_id.to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cursor_round_trips_with_microsecond_fidelity() {
        let cursor = DocumentCursor {
            updated_at: DateTime::from_timestamp_micros(1_755_000_000_123_456).expect("time"),
            document_id: "01JZ8QK9".to_owned(),
        };
        assert_eq!(DocumentCursor::decode(&cursor.encode()), Some(cursor));
    }

    #[test]
    fn a_document_id_containing_a_colon_survives_the_split() {
        // `split_once` takes the FIRST colon, so only the timestamp is claimed.
        let cursor = DocumentCursor {
            updated_at: DateTime::from_timestamp_micros(1).expect("time"),
            document_id: "weird:id:here".to_owned(),
        };
        assert_eq!(DocumentCursor::decode(&cursor.encode()), Some(cursor));
    }

    #[test]
    fn junk_is_rejected_rather_than_read_as_the_beginning() {
        for value in [
            "",
            "not hex!!",
            &hex::encode("no-colon"),
            &hex::encode("notanumber:doc-1"),
            &hex::encode("123:"),
        ] {
            assert_eq!(DocumentCursor::decode(value), None, "{value:?} should fail");
        }
    }
}
