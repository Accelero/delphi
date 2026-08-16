use sha2::{Digest, Sha256};

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// The `"sha256:<hex>"` form stored on events and in the projection.
pub fn checksum(sha256_hex: &str) -> String {
    format!("sha256:{sha256_hex}")
}
