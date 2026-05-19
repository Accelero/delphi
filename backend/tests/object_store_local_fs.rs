//! `LocalFsObjectStore` end-to-end: put / get / delete / exists +
//! atomic-write hygiene.

use bytes::Bytes;
use tempfile::TempDir;

use delphi::object_store::{LocalFsObjectStore, ObjectStore};

#[tokio::test]
async fn put_then_get_roundtrip() {
    let tmp = TempDir::new().unwrap();
    let store = LocalFsObjectStore::new(tmp.path()).unwrap();

    let url = store
        .put("originals/doc-1.pdf", Bytes::from_static(b"PDF-bytes"))
        .await
        .unwrap();
    assert!(url.starts_with("file://"));
    assert!(url.ends_with("originals/doc-1.pdf"));

    let got = store.get("originals/doc-1.pdf").await.unwrap();
    assert_eq!(&got[..], b"PDF-bytes");
}

#[tokio::test]
async fn put_overwrites_existing_key() {
    let tmp = TempDir::new().unwrap();
    let store = LocalFsObjectStore::new(tmp.path()).unwrap();

    store
        .put("k", Bytes::from_static(b"first"))
        .await
        .unwrap();
    store
        .put("k", Bytes::from_static(b"second"))
        .await
        .unwrap();

    let got = store.get("k").await.unwrap();
    assert_eq!(&got[..], b"second");
}

#[tokio::test]
async fn exists_and_delete() {
    let tmp = TempDir::new().unwrap();
    let store = LocalFsObjectStore::new(tmp.path()).unwrap();

    assert!(!store.exists("missing").await.unwrap());

    store
        .put("here", Bytes::from_static(b"hi"))
        .await
        .unwrap();
    assert!(store.exists("here").await.unwrap());

    store.delete("here").await.unwrap();
    assert!(!store.exists("here").await.unwrap());
    // Deleting a missing key is a no-op (idempotent).
    store.delete("here").await.unwrap();
}

#[tokio::test]
async fn atomic_write_does_not_leak_tmp() {
    let tmp = TempDir::new().unwrap();
    let store = LocalFsObjectStore::new(tmp.path()).unwrap();

    store
        .put("nested/path/file.pdf", Bytes::from_static(b"x"))
        .await
        .unwrap();

    // No `.tmp` sibling left after a successful put.
    let dir = tmp.path().join("nested/path");
    let entries: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().into_string().unwrap())
        .collect();
    assert_eq!(entries, vec!["file.pdf".to_string()]);
}

#[tokio::test]
async fn rejects_keys_with_traversal() {
    let tmp = TempDir::new().unwrap();
    let store = LocalFsObjectStore::new(tmp.path()).unwrap();

    let r = store.put("../escape", Bytes::from_static(b"nope")).await;
    assert!(r.is_err(), "expected rejection of traversal key");
}
