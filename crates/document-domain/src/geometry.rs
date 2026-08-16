use thiserror::Error;

/// S3's hard cap on a single object.
pub const MAX_OBJECT_BYTES: u64 = 5 * 1024 * 1024 * 1024 * 1024;
/// S3's floor for every part but the last.
pub const MIN_PART_BYTES: u64 = 5 * 1024 * 1024;
/// S3's cap on parts per multipart upload. Enforced server-side: part 10 001
/// is a `400 InvalidArgument`, verified against live storage by
/// `storage_refuses_a_part_number_above_the_ten_thousand_cap`.
pub const MAX_PARTS: u64 = 10_000;
/// S3's cap on a single part.
pub const MAX_PART_BYTES: u64 = 5 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum GeometryError {
    #[error("file size must be greater than zero")]
    Empty,
    #[error("configured part size {part_size} must be between {MIN_PART_BYTES} and {MAX_PART_BYTES} bytes")]
    InvalidPartSize { part_size: u64 },
    #[error("file size {size} exceeds the {MAX_OBJECT_BYTES} byte object limit")]
    TooLarge { size: u64 },
    #[error("part size must be greater than zero")]
    ZeroPartSize,
    #[error("file size {size} at part size {part_size} needs {parts} parts, over the {MAX_PARTS} limit")]
    TooManyParts {
        size: u64,
        part_size: u64,
        parts: u64,
    },
}

/// Part size is server-owned: the client MUST slice at exactly this value.
///
/// ```text
/// part_size = max(target, ceil(file_size / MAX_PARTS))
/// ```
///
/// The `max` is what makes **part count scale before part size**. `target` —
/// the deployment's configured part size — governs until the file is large
/// enough that honouring it would need more than [`MAX_PARTS`] parts; only then
/// does the second term take over and grow the part instead.
///
/// That ordering is deliberate and is the opposite of the intuitive one.
/// Growing parts for large files sounds efficient, but retry cost is highest
/// exactly where it would hurt: long uploads are the ones most likely to meet a
/// network blip, and a bigger part is a bigger unit to lose to one.
///
/// The result is `<= MAX_PARTS` parts for every input. If `target` wins it is
/// already `>= file_size / MAX_PARTS`; if the second term wins it *is* that
/// value. Nothing a caller passes can break the cap.
///
/// The 5 MiB floor is deliberately not a term in the formula — a `target` below
/// it is a configuration error, refused here rather than quietly raised, so a
/// deployment cannot believe it is using a part size it is not.
pub fn part_size_bytes(file_size: u64, target: u64) -> Result<u64, GeometryError> {
    if !(MIN_PART_BYTES..=MAX_PART_BYTES).contains(&target) {
        return Err(GeometryError::InvalidPartSize { part_size: target });
    }
    if file_size == 0 {
        return Err(GeometryError::Empty);
    }
    if file_size > MAX_OBJECT_BYTES {
        return Err(GeometryError::TooLarge { size: file_size });
    }
    Ok(target.max(file_size.div_ceil(MAX_PARTS)))
}

/// How large a file may be before `target` stops being honoured.
///
/// Above this, [`part_size_bytes`] grows the part to hold the count at
/// [`MAX_PARTS`]. Configuration uses this to tell an operator that their
/// configured part size will not apply to the largest files they allow.
pub fn largest_size_honouring(target: u64) -> u64 {
    target.saturating_mul(MAX_PARTS)
}

/// `ceil(size / part_size)`, stated with `ceil` everywhere because that is the
/// invariant browser uploaders clamp against. A part size produced by
/// [`part_size_bytes`] can never make this exceed [`MAX_PARTS`], so the client's
/// own clamp can never fire and change the geometry underneath us.
pub fn part_count(file_size: u64, part_size: u64) -> Result<u16, GeometryError> {
    if file_size == 0 {
        return Err(GeometryError::Empty);
    }
    if part_size == 0 {
        return Err(GeometryError::ZeroPartSize);
    }
    let parts = file_size.div_ceil(part_size);
    if parts > MAX_PARTS {
        return Err(GeometryError::TooManyParts {
            size: file_size,
            part_size,
            parts,
        });
    }
    u16::try_from(parts).map_err(|_| GeometryError::TooManyParts {
        size: file_size,
        part_size,
        parts,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What `docker-compose.t2.yml` configures. Nothing in this crate defaults
    /// to it — the deployment supplies it — but the properties below are the
    /// ones that must hold for a sane value.
    const TARGET: u64 = 20 * 1024 * 1024;

    #[test]
    fn small_files_use_the_configured_part_size() {
        assert_eq!(part_size_bytes(1, TARGET).expect("size"), TARGET);
        assert_eq!(part_count(1, TARGET).expect("count"), 1);
    }

    #[test]
    fn the_configured_size_holds_until_the_part_cap_binds() {
        let boundary = largest_size_honouring(TARGET);
        assert_eq!(boundary, TARGET * MAX_PARTS);
        assert_eq!(part_size_bytes(boundary, TARGET).expect("size"), TARGET);
        assert_eq!(
            part_count(boundary, TARGET).expect("count"),
            MAX_PARTS as u16
        );

        // One byte past it, the count is pinned and the part grows instead.
        let over = boundary + 1;
        let size = part_size_bytes(over, TARGET).expect("size");
        assert!(size > TARGET);
        assert_eq!(u64::from(part_count(over, size).expect("count")), MAX_PARTS);
    }

    #[test]
    fn count_scales_before_size() {
        // The whole ordering, in one place: below the boundary only the count
        // moves; above it only the size does.
        let mut previous_count = 0;
        for multiple in [1_u64, 10, 100, 1_000] {
            let size = TARGET * multiple;
            assert_eq!(part_size_bytes(size, TARGET).expect("size"), TARGET);
            let count = part_count(size, TARGET).expect("count");
            assert!(u64::from(count) > previous_count, "count must grow");
            previous_count = u64::from(count);
        }
    }

    #[test]
    fn no_input_can_produce_more_than_ten_thousand_parts() {
        for target in [MIN_PART_BYTES, TARGET, 64 * 1024 * 1024] {
            for size in [
                1,
                target - 1,
                target,
                target + 1,
                largest_size_honouring(target),
                largest_size_honouring(target) + 1,
                MAX_OBJECT_BYTES - 1,
                MAX_OBJECT_BYTES,
            ] {
                let part_size = part_size_bytes(size, target).expect("size");
                let count = part_count(size, part_size).expect("count");
                assert!(
                    u64::from(count) <= MAX_PARTS,
                    "size {size} at target {target} produced {count} parts"
                );
                assert!(
                    part_size <= MAX_PART_BYTES,
                    "size {size} at target {target} produced a {part_size} byte part"
                );
            }
        }
    }

    #[test]
    fn a_part_size_outside_s3s_limits_is_refused_rather_than_clamped() {
        // Quietly raising it would leave a deployment believing it uses a part
        // size it does not.
        for target in [0, 1, MIN_PART_BYTES - 1, MAX_PART_BYTES + 1] {
            assert_eq!(
                part_size_bytes(1024, target),
                Err(GeometryError::InvalidPartSize { part_size: target })
            );
        }
        assert!(part_size_bytes(1024, MIN_PART_BYTES).is_ok());
        assert!(part_size_bytes(1024, MAX_PART_BYTES).is_ok());
    }

    #[test]
    fn zero_and_oversized_files_are_rejected() {
        assert_eq!(part_size_bytes(0, TARGET), Err(GeometryError::Empty));
        assert!(matches!(
            part_size_bytes(MAX_OBJECT_BYTES + 1, TARGET),
            Err(GeometryError::TooLarge { .. })
        ));
    }

    #[test]
    fn ceil_is_used_consistently_so_a_client_clamp_can_never_fire() {
        // Odd sizes are where a floor/ceil mismatch would show up.
        for size in [
            TARGET * MAX_PARTS + 1,
            TARGET * MAX_PARTS + 12_345,
            MAX_OBJECT_BYTES - 1,
        ] {
            let part_size = part_size_bytes(size, TARGET).expect("size");
            let count = part_count(size, part_size).expect("count");
            assert!(u64::from(count) <= MAX_PARTS);
            assert_eq!(u64::from(count), size.div_ceil(part_size));
        }
    }
}
