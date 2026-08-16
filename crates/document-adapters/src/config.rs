//! Configuration.
//!
//! Every new variable is `DELPHI_DOCUMENT_<NAME>` and **required**: missing
//! configuration fails at startup rather than silently defaulting into a
//! surprising production behaviour. (Older code uses unprefixed
//! `INGEST_UPLOAD_*` with defaults; do not copy that.)

use std::time::Duration;

use delphi_document_app::UploadPolicy;
use delphi_document_domain::{
    largest_size_honouring, MAX_OBJECT_BYTES, MAX_PARTS, MAX_PART_BYTES, MIN_PART_BYTES,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("{0} is required")]
    Missing(&'static str),
    #[error("{name} must be {expected}, got {value:?}")]
    Invalid {
        name: &'static str,
        expected: &'static str,
        value: String,
    },
}

fn var(name: &'static str) -> Result<String, ConfigError> {
    std::env::var(name).map_err(|_| ConfigError::Missing(name))
}

fn parse_u64(name: &'static str) -> Result<u64, ConfigError> {
    let raw = var(name)?;
    raw.trim().parse().map_err(|_| ConfigError::Invalid {
        name,
        expected: "a non-negative integer",
        value: raw,
    })
}

fn parse_secs(name: &'static str) -> Result<Duration, ConfigError> {
    Ok(Duration::from_secs(parse_u64(name)?))
}

fn parse_usize(name: &'static str) -> Result<usize, ConfigError> {
    Ok(parse_u64(name)? as usize)
}

fn parse_bool(name: &'static str) -> Result<bool, ConfigError> {
    let raw = var(name)?;
    match raw.trim() {
        "1" | "true" | "yes" => Ok(true),
        "0" | "false" | "no" => Ok(false),
        _ => Err(ConfigError::Invalid {
            name,
            expected: "a boolean",
            value: raw,
        }),
    }
}

#[derive(Debug, Clone)]
pub struct S3Config {
    pub endpoint_internal: String,
    /// Where the *browser* reaches the bucket. Presigned URLs are signed
    /// against this host, so it must be the one the client actually uses.
    pub endpoint_public: String,
    pub region: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub force_path_style: bool,
}

impl S3Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Ok(Self {
            endpoint_internal: var("DELPHI_DOCUMENT_S3_ENDPOINT_INTERNAL")?,
            endpoint_public: var("DELPHI_DOCUMENT_S3_ENDPOINT_PUBLIC")?,
            region: var("DELPHI_DOCUMENT_S3_REGION")?,
            bucket: var("DELPHI_DOCUMENT_S3_BUCKET")?,
            access_key_id: var("DELPHI_DOCUMENT_S3_ACCESS_KEY_ID")?,
            secret_access_key: var("DELPHI_DOCUMENT_S3_SECRET_ACCESS_KEY")?,
            force_path_style: parse_bool("DELPHI_DOCUMENT_S3_FORCE_PATH_STYLE")?,
        })
    }
}

/// The one value that decides how long an upload exists.
///
/// It is read by exactly two things, and both of them **enforce** it: the KV
/// bucket's `max_age`, declared here, and the age at which the object-storage
/// reaper aborts an incomplete multipart. Neither service reads it, because
/// neither service does anything with it — see [`crate::connect_jetstream`].
#[derive(Debug, Clone)]
pub struct TopologyConfig {
    pub upload_ttl: Duration,
}

impl TopologyConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Ok(Self {
            upload_ttl: parse_secs("DELPHI_DOCUMENT_UPLOAD_TTL_SECS")?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub ack_wait: Duration,
    pub max_deliver: u32,
    pub max_ack_pending: usize,
    /// How many work items one instance finishes at once. Bounded separately
    /// from `max_ack_pending` because that is JetStream's cap on *unacked*
    /// items, while this is the number of objects this process is willing to
    /// stream past a scanner simultaneously.
    pub work_concurrency: usize,
    pub projector_lock_id: i64,
    pub projector_election_interval: Duration,
    pub projection_batch: usize,
}

impl WorkerConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let max_deliver = parse_u64("DELPHI_DOCUMENT_MAX_DELIVER")?;
        if max_deliver == 0 {
            // Unlimited redelivery would mean an upload that never succeeds is
            // also never cleaned up: the last delivery is what converts a
            // transient failure into a rejection, which aborts the multipart
            // and deletes the object.
            return Err(ConfigError::Invalid {
                name: "DELPHI_DOCUMENT_MAX_DELIVER",
                expected: "finite and at least 1",
                value: "0".to_owned(),
            });
        }
        let max_ack_pending = parse_usize("DELPHI_DOCUMENT_MAX_ACK_PENDING")?;
        let work_concurrency = parse_usize("DELPHI_DOCUMENT_WORK_CONCURRENCY")?;
        if work_concurrency == 0 || work_concurrency > max_ack_pending {
            // Above `max_ack_pending` the extra slots can never be filled:
            // JetStream stops delivering, so the number would only mislead.
            return Err(ConfigError::Invalid {
                name: "DELPHI_DOCUMENT_WORK_CONCURRENCY",
                expected: "between 1 and DELPHI_DOCUMENT_MAX_ACK_PENDING",
                value: work_concurrency.to_string(),
            });
        }
        Ok(Self {
            ack_wait: parse_secs("DELPHI_DOCUMENT_ACK_WAIT_SECS")?,
            max_deliver: max_deliver as u32,
            max_ack_pending,
            work_concurrency,
            projector_lock_id: parse_u64("DELPHI_DOCUMENT_PROJECTOR_LOCK_ID")? as i64,
            projector_election_interval: parse_secs(
                "DELPHI_DOCUMENT_PROJECTOR_ELECTION_SECS",
            )?,
            projection_batch: parse_usize("DELPHI_DOCUMENT_PROJECTION_BATCH")?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ApiConfig {
    /// Presigned part URL expiry.
    ///
    /// Bounded absolutely rather than against the upload window. A part URL is
    /// a bearer capability that anyone holding it can PUT arbitrary bytes
    /// through, so what makes a long one bad is that someone can sit on it —
    /// not that it might outlive the upload, which merely makes it useless.
    /// The old relative check also forced every service to know the upload TTL
    /// just to run it.
    pub part_url_ttl: Duration,
    /// Largest declarable file. Anything over it is refused at preflight,
    /// before a multipart is opened.
    pub max_upload_bytes: u64,
    /// The part size to slice at while the part cap is not binding.
    pub part_size_bytes: u64,
}

/// The longest a presigned part URL may live. A client signs each part
/// immediately before uploading it, so anything approaching this is already a
/// misconfiguration.
const MAX_PART_URL_TTL: Duration = Duration::from_secs(3600);

impl ApiConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let max_upload_bytes = parse_u64("DELPHI_DOCUMENT_MAX_UPLOAD_BYTES")?;
        if max_upload_bytes == 0 || max_upload_bytes > MAX_OBJECT_BYTES {
            return Err(ConfigError::Invalid {
                name: "DELPHI_DOCUMENT_MAX_UPLOAD_BYTES",
                expected: "between 1 and S3's 5 TiB object limit",
                value: max_upload_bytes.to_string(),
            });
        }
        let part_size_bytes = parse_u64("DELPHI_DOCUMENT_PART_SIZE_BYTES")?;
        if !(MIN_PART_BYTES..=MAX_PART_BYTES).contains(&part_size_bytes) {
            // Below S3's floor every non-final part is rejected, so no
            // multi-part upload could ever complete. Refuse rather than raise
            // it quietly: a deployment must not believe it uses a part size it
            // does not.
            return Err(ConfigError::Invalid {
                name: "DELPHI_DOCUMENT_PART_SIZE_BYTES",
                expected: "between S3's 5 MiB part floor and its 5 GiB part cap",
                value: part_size_bytes.to_string(),
            });
        }

        // Not fatal — the geometry stays correct either way, because it grows
        // the part size to hold the count at MAX_PARTS. But above this size the
        // configured part size silently stops applying, and an operator who
        // raised the upload cap should be told rather than discover it from a
        // surprising `part_size_bytes` in a preflight response.
        let honoured_to = largest_size_honouring(part_size_bytes);
        if max_upload_bytes > honoured_to {
            tracing::error!(
                part_size_bytes,
                max_upload_bytes,
                honoured_up_to_bytes = honoured_to,
                parts_at_max = max_upload_bytes.div_ceil(part_size_bytes),
                max_parts = MAX_PARTS,
                "DELPHI_DOCUMENT_PART_SIZE_BYTES cannot be honoured for the largest \
                 allowed upload: it would need more than {MAX_PARTS} parts, so files \
                 above the honoured size will be sliced into larger parts instead. \
                 Lower DELPHI_DOCUMENT_MAX_UPLOAD_BYTES or raise the part size to \
                 make the configured value apply everywhere."
            );
        }

        let part_url_ttl = parse_secs("DELPHI_DOCUMENT_PART_URL_TTL_SECS")?;
        if part_url_ttl > MAX_PART_URL_TTL {
            return Err(ConfigError::Invalid {
                name: "DELPHI_DOCUMENT_PART_URL_TTL_SECS",
                expected: "at most 3600",
                value: part_url_ttl.as_secs().to_string(),
            });
        }
        Ok(Self {
            part_url_ttl,
            max_upload_bytes,
            part_size_bytes,
        })
    }
}

pub fn upload_policy(api: &ApiConfig) -> UploadPolicy {
    UploadPolicy {
        part_url_ttl: api.part_url_ttl,
        max_upload_bytes: api.max_upload_bytes,
        part_size_bytes: api.part_size_bytes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every variable `ApiConfig::from_env` reads, at values that pass.
    /// Individual tests override one to prove it is checked.
    fn valid_api_env() -> Vec<(&'static str, String)> {
        vec![
            ("DELPHI_DOCUMENT_MAX_UPLOAD_BYTES", "34359738368".to_owned()),
            ("DELPHI_DOCUMENT_PART_SIZE_BYTES", "20971520".to_owned()),
            ("DELPHI_DOCUMENT_PART_URL_TTL_SECS", "300".to_owned()),
        ]
    }

    /// `set_var` is process-global, so these tests cannot run concurrently
    /// with each other — one would observe another's overrides. Serialising
    /// here rather than requiring `--test-threads=1`, which would slow the
    /// whole suite for four tests.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn with_env(overrides: &[(&str, &str)], test: impl FnOnce()) {
        // A panicking test poisons the lock; the guard is still valid, and
        // failing every later test on the first failure would hide the rest.
        let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
        let mut env = valid_api_env();
        for (key, value) in overrides {
            match env.iter_mut().find(|(name, _)| name == key) {
                Some(entry) => entry.1 = (*value).to_owned(),
                None => env.push((
                    Box::leak((*key).to_owned().into_boxed_str()),
                    (*value).to_owned(),
                )),
            }
        }
        let previous: Vec<_> = env
            .iter()
            .map(|(key, _)| (*key, std::env::var(key).ok()))
            .collect();
        for (key, value) in &env {
            unsafe { std::env::set_var(key, value) };
        }
        test();
        for (key, value) in previous {
            match value {
                Some(value) => unsafe { std::env::set_var(key, value) },
                None => unsafe { std::env::remove_var(key) },
            }
        }
    }

    #[test]
    fn the_shipped_configuration_is_accepted() {
        with_env(&[], || {
            let config = ApiConfig::from_env().expect("compose values must parse");
            assert_eq!(config.part_size_bytes, 20 * 1024 * 1024);
        });
    }

    #[test]
    fn a_part_url_that_outlives_its_usefulness_is_refused() {
        // Absolute, not relative to the upload window: a part URL is a bearer
        // capability, and the hazard is someone holding it, not it outliving
        // the upload.
        with_env(&[("DELPHI_DOCUMENT_PART_URL_TTL_SECS", "86400")], || {
            assert!(matches!(
                ApiConfig::from_env(),
                Err(ConfigError::Invalid {
                    name: "DELPHI_DOCUMENT_PART_URL_TTL_SECS",
                    ..
                })
            ));
        });
    }

    #[test]
    fn a_part_size_below_s3s_floor_is_refused() {
        // Every non-final part would be rejected by storage, so no multi-part
        // upload could ever complete.
        with_env(&[("DELPHI_DOCUMENT_PART_SIZE_BYTES", "1048576")], || {
            assert!(matches!(
                ApiConfig::from_env(),
                Err(ConfigError::Invalid {
                    name: "DELPHI_DOCUMENT_PART_SIZE_BYTES",
                    ..
                })
            ));
        });
    }

    #[test]
    fn an_upload_cap_that_outgrows_the_part_size_is_accepted_but_logged() {
        // Not fatal: the geometry grows the part instead, so uploads still
        // work. The operator is told because their configured part size has
        // silently stopped applying to the largest files they allow.
        with_env(
            &[
                ("DELPHI_DOCUMENT_PART_SIZE_BYTES", "5242880"),
                // 5 MiB x 10 000 = 50 GiB honoured; this is well past it.
                ("DELPHI_DOCUMENT_MAX_UPLOAD_BYTES", "549755813888"),
            ],
            || {
                let config = ApiConfig::from_env().expect("still a usable configuration");
                assert!(config.max_upload_bytes > largest_size_honouring(config.part_size_bytes));
            },
        );
    }

    #[test]
    fn a_zero_max_deliver_is_still_refused() {
        // The last line of defence now that the work stream never ages an item
        // out: `max_deliver` is the *only* bound on a work item's behaviour, and
        // the final delivery is the only thing that converts a stuck upload
        // into a rejection — which is the only thing that deletes its bytes.
        with_env(&[("DELPHI_DOCUMENT_MAX_DELIVER", "0")], || {
            assert!(matches!(
                WorkerConfig::from_env(),
                Err(ConfigError::Invalid {
                    name: "DELPHI_DOCUMENT_MAX_DELIVER",
                    ..
                })
            ));
        });
    }
}
