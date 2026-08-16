use thiserror::Error;

use crate::events::MetadataPatch;

pub const MAX_TITLE_CHARS: usize = 512;
pub const MAX_TAGS: usize = 64;
pub const MAX_TAG_CHARS: usize = 64;
pub const MAX_DESCRIPTION_CHARS: usize = 8192;
/// Serialized cap. The metadata rides inside the `UploadCompleted` command, so
/// an unbounded blob here eats the NATS `max_payload` budget the parts list
/// also needs.
pub const MAX_METADATA_BYTES: usize = 32 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ValidationError {
    #[error("title must be between 1 and {MAX_TITLE_CHARS} characters")]
    Title,
    #[error("at most {MAX_TAGS} tags are allowed")]
    TagCount,
    #[error("each tag must be between 1 and {MAX_TAG_CHARS} characters with no control characters")]
    Tag,
    #[error("description must be at most {MAX_DESCRIPTION_CHARS} characters")]
    Description,
    #[error("metadata must be a JSON object")]
    MetadataShape,
    #[error("metadata must serialize to at most {MAX_METADATA_BYTES} bytes")]
    MetadataSize,
}

/// The one validation entry point for every write path.
///
/// Deliberately a runtime check over plain types rather than newtypes with
/// fallible constructors: `MetadataPatch` must round-trip through serde from
/// both HTTP and NATS, and a newtype would have to be reconstructed on the way
/// in anyway. Calling this at every entry point is what keeps the projection
/// un-poisonable — the fold itself never rejects anything.
pub fn validate_metadata_patch(patch: &MetadataPatch) -> Result<(), ValidationError> {
    if let Some(title) = &patch.title {
        let trimmed = title.trim();
        let len = trimmed.chars().count();
        if len == 0 || len > MAX_TITLE_CHARS {
            return Err(ValidationError::Title);
        }
    }

    if let Some(tags) = &patch.tags {
        if tags.len() > MAX_TAGS {
            return Err(ValidationError::TagCount);
        }
        for tag in tags {
            let trimmed = tag.trim();
            let len = trimmed.chars().count();
            if len == 0 || len > MAX_TAG_CHARS {
                return Err(ValidationError::Tag);
            }
            if trimmed.chars().any(char::is_control) {
                return Err(ValidationError::Tag);
            }
        }
    }

    if let Some(description) = &patch.description {
        if description.chars().count() > MAX_DESCRIPTION_CHARS {
            return Err(ValidationError::Description);
        }
    }

    if let Some(metadata) = &patch.metadata {
        if !metadata.is_object() {
            return Err(ValidationError::MetadataShape);
        }
        let serialized = serde_json::to_vec(metadata).map_err(|_| ValidationError::MetadataShape)?;
        if serialized.len() > MAX_METADATA_BYTES {
            return Err(ValidationError::MetadataSize);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn patch() -> MetadataPatch {
        MetadataPatch::default()
    }

    #[test]
    fn an_empty_patch_is_valid() {
        assert_eq!(validate_metadata_patch(&patch()), Ok(()));
    }

    #[test]
    fn title_bounds_are_enforced_after_trimming() {
        let blank = MetadataPatch {
            title: Some("   ".to_owned()),
            ..patch()
        };
        assert_eq!(validate_metadata_patch(&blank), Err(ValidationError::Title));

        let long = MetadataPatch {
            title: Some("a".repeat(MAX_TITLE_CHARS + 1)),
            ..patch()
        };
        assert_eq!(validate_metadata_patch(&long), Err(ValidationError::Title));

        let ok = MetadataPatch {
            title: Some("a".repeat(MAX_TITLE_CHARS)),
            ..patch()
        };
        assert_eq!(validate_metadata_patch(&ok), Ok(()));
    }

    #[test]
    fn tag_count_and_tag_shape_are_enforced() {
        let too_many = MetadataPatch {
            tags: Some(vec!["t".to_owned(); MAX_TAGS + 1]),
            ..patch()
        };
        assert_eq!(
            validate_metadata_patch(&too_many),
            Err(ValidationError::TagCount)
        );

        let empty_tag = MetadataPatch {
            tags: Some(vec![" ".to_owned()]),
            ..patch()
        };
        assert_eq!(validate_metadata_patch(&empty_tag), Err(ValidationError::Tag));

        let long_tag = MetadataPatch {
            tags: Some(vec!["t".repeat(MAX_TAG_CHARS + 1)]),
            ..patch()
        };
        assert_eq!(validate_metadata_patch(&long_tag), Err(ValidationError::Tag));
    }

    #[test]
    fn control_characters_in_a_tag_are_rejected() {
        let sneaky = MetadataPatch {
            tags: Some(vec!["fin\u{0}ance".to_owned()]),
            ..patch()
        };
        assert_eq!(validate_metadata_patch(&sneaky), Err(ValidationError::Tag));

        let newline = MetadataPatch {
            tags: Some(vec!["fin\nance".to_owned()]),
            ..patch()
        };
        assert_eq!(validate_metadata_patch(&newline), Err(ValidationError::Tag));
    }

    #[test]
    fn description_length_is_enforced() {
        let long = MetadataPatch {
            description: Some("d".repeat(MAX_DESCRIPTION_CHARS + 1)),
            ..patch()
        };
        assert_eq!(
            validate_metadata_patch(&long),
            Err(ValidationError::Description)
        );
    }

    #[test]
    fn metadata_must_be_a_bounded_json_object() {
        let not_object = MetadataPatch {
            metadata: Some(serde_json::json!([1, 2, 3])),
            ..patch()
        };
        assert_eq!(
            validate_metadata_patch(&not_object),
            Err(ValidationError::MetadataShape)
        );

        let huge = MetadataPatch {
            metadata: Some(serde_json::json!({ "blob": "x".repeat(MAX_METADATA_BYTES) })),
            ..patch()
        };
        assert_eq!(
            validate_metadata_patch(&huge),
            Err(ValidationError::MetadataSize)
        );

        let ok = MetadataPatch {
            metadata: Some(serde_json::json!({ "source": "email" })),
            ..patch()
        };
        assert_eq!(validate_metadata_patch(&ok), Ok(()));
    }

    #[test]
    fn multibyte_titles_are_counted_in_characters_not_bytes() {
        // 512 four-byte characters is 2048 bytes but exactly at the char limit.
        let ok = MetadataPatch {
            title: Some("😀".repeat(MAX_TITLE_CHARS)),
            ..patch()
        };
        assert_eq!(validate_metadata_patch(&ok), Ok(()));
    }
}
