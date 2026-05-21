//! `OBJECT_STORE_URL` → impl dispatch. S3-only after the LocalFs removal.

use delphi::error::Error;
use delphi::object_store::from_url;

#[test]
fn non_s3_url_rejected() {
    // The old `file://` local-FS form is gone — only `s3://` is valid.
    let r = from_url("file:///tmp/whatever");
    let e = r.err().expect("file:// must be rejected");
    assert!(matches!(e, Error::InvalidConfig(_)), "got {e}");
}

#[test]
fn bare_path_rejected() {
    let r = from_url("/var/lib/delphi/originals");
    let e = r.err().expect("bare path must be rejected");
    assert!(matches!(e, Error::InvalidConfig(_)), "got {e}");
}
