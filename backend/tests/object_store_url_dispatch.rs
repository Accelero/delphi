//! `OBJECT_STORE_URL` → impl dispatch.

use tempfile::TempDir;

use delphi::error::Error;
use delphi::object_store::from_url;

#[test]
fn file_url_constructs_local_fs() {
    let tmp = TempDir::new().unwrap();
    let url = format!("file://{}", tmp.path().display());
    let store = from_url(&url).expect("file:// should construct");
    // We don't expose the impl type; round-tripping a put through the
    // trait is already covered in object_store_local_fs.rs. Here we
    // just want to know construction succeeded.
    drop(store);
}

#[test]
fn s3_url_returns_not_implemented() {
    let r = from_url("s3://my-bucket/prefix");
    let e = r.err().expect("s3 URL must error in slice 2");
    assert!(matches!(e, Error::NotImplemented(_)), "got {e}");
}

#[test]
fn no_scheme_treats_as_filesystem_path() {
    let tmp = TempDir::new().unwrap();
    // No leading scheme — bare path.
    let url = tmp.path().to_string_lossy().to_string();
    let store = from_url(&url).expect("bare path should construct LocalFs");
    drop(store);
}
