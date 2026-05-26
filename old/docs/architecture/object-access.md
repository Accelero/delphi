# Object Access & Delivery — direct-to-storage minting

Status: implemented (S3-presigned minter only). How clients read and
write object-storage bytes. Sister to [`ingestion.md`](./ingestion.md);
supersedes the old Traefik `/s3` byte-path design.

## Decision

The backend **mints a short-lived, scoped URL**; the client talks to the
object store **directly** (no proxy, no Traefik in the byte path). This
holds for **both directions** — upload (presigned `PUT`) and download
(presigned `GET`). MinIO is exposed on its own endpoint, not behind
Traefik.

The minting mechanism sits behind a **swappable seam** so a CDN-grade
minter (signed cookies / edge-validated claims tokens / STS) can drop in
later **without changing callers or the frontend**. We implement only the
**S3 presigned** minter now.

Invariant preserved in every mode: the **bucket is never anonymously
readable** — access is always gated by a credential the backend issued.

## The seam (the swap point)

`backend/src/object_store/` gains an access-minting abstraction, distinct
from `ObjectStore` (which stays for the backend's *own* server-side R/W:
validation read-back, text extraction, commit, cleaner).

```rust
pub enum AccessOp {
    Download,
    UploadPart { upload_id: String, part_number: u16 },
}

pub struct AccessGrant {
    pub url: String,                 // client fetches/PUTs here directly
    pub method: Method,              // GET | PUT
    pub headers: Vec<(String, String)>, // usually empty for presigned
    pub expires_at: DateTime<Utc>,
}

#[async_trait]
pub trait AccessMinter: Send + Sync {
    /// Mint a client-usable handle for `op` on `key`. The caller has
    /// ALREADY made the authorization decision (tenant/doc check).
    async fn mint(&self, key: &str, op: AccessOp, ttl: Duration) -> Result<AccessGrant>;
}
```

Wired into `AppState` as `Arc<dyn AccessMinter>`. **Every mode reduces to
"hand the client a URL + method + expiry,"** which is exactly why the seam
accommodates presigned / proxy / CDN / STS without caller changes.

### Implemented now: `S3PresignAccess`

Wraps the existing `aws-sdk-s3` client (the `public`-endpoint client used
for presigning, per [`ingestion.md`](./ingestion.md)):

- `Download` → presign `get_object(bucket, key)` for `ttl`.
- `UploadPart { upload_id, part_number }` → presign `upload_part(...)`
  (the current `S3ObjectStore::presign_upload_part` logic moves/feeds in
  here).

Scoped to the exact key + operation, short TTL, signed against the public
endpoint so the URL carries the browser-facing host.

### Deferred drop-ins (NOT built — documented so the seam stays honest)

Same trait, swapped by deployment config; no caller/frontend change:

- **`CdnAccess`** — mints a CDN signed cookie/URL or an
  asymmetric-signed, claims-scoped token an edge function validates
  (Lambda@Edge / Cloudflare Worker); origin locked by OAC so the browser
  never holds an S3 credential. Adds `jti`/single-use for near-live
  revocation.
- **`StsAccess`** — exchanges the IdP JWT at the store's STS endpoint
  (MinIO `AssumeRoleWithWebIdentity`) for prefix-scoped temp creds.
- **`ProxyAccess`** — returns a same-origin backend/edge URL that streams
  (max security, infra in byte path) for a future "small/locked-down"
  deployment mode.

We do not implement these now; the trait + `AppState` injection are the
only forward-investment.

## Backend wiring

- **Upload sign-part** (`/api/ingestion/uploads/:id/sign-part`): route
  through `access.mint(key, UploadPart{..}, upload_part_ttl)` instead of
  calling `ObjectStore::presign_upload_part` directly. Returned URL shape
  is identical today; the indirection is the swap point.
- **Download** — replace the byte-streaming proxy. Today
  `GET /api/documents/:key/file` reads the object via
  `object_store.get_by_url` and streams it. New:
  `GET /api/documents/:key/view-url` →
  1. tenant-scoped `get_document` (the **authz decision**, unchanged),
  2. `access.mint(storage_key, Download, download_ttl)`,
  3. return `{ url, expires_at }`.
  Remove the streaming `/file` handler. `ObjectStore::get_by_url` likely
  becomes unused → delete it.
- **Range support concern evaporates:** PDF.js range requests now hit S3
  directly (S3/MinIO honor `Range`); the backend no longer needs to proxy
  ranges.

## Frontend

- **Download/view:** `PdfViewer` (and any "open original" surface) fetches
  `/api/documents/:key/view-url`, then hands the returned `url` to
  react-pdf / PDF.js, which fetches bytes **directly from MinIO** (with
  range requests). One small change; the component already takes a URL.
- **Upload:** unchanged — Uppy's `signPart` already consumes a URL; that
  URL now comes from the seam.
- The frontend stays **handle-based** ("ask backend how to fetch/upload,
  then use the handle"), so swapping the minter later touches no frontend
  code.
- **CORS:** MinIO's built-in default already echoes the origin, allows
  `PUT`/`GET`/`HEAD`, and exposes `ETag` + `Accept-Ranges`/`Content-Range`
  (verified) — covers both direct upload and ranged PDF view.

## Infra — MinIO out of Traefik (tier-2)

- `ops/traefik/dynamic/routes.yml`: **remove the `/s3` router and the
  `minio` service.** (This also kills the broken unstripped-prefix routing
  that made MinIO read `s3` as the bucket → `InvalidBucketName`.)
- `docker-compose.full.yml`: publish MinIO directly —
  `ports: ["9000:9000", "9001:9001"]` — and set
  `DELPHI_INGEST_S3_ENDPOINT_PUBLIC=http://localhost:9000` (was
  `http://localhost/s3`). `DELPHI_INGEST_S3_ENDPOINT_INTERNAL=http://minio:9000`
  unchanged.
- Tier-1 already exposes `:9000` and presigns against `localhost:9000` —
  unchanged. Both tiers now identical in shape: browser → MinIO directly.
- Bucket stays private (`mc anonymous set none`); `minio-init` unchanged.
- **Prod note:** with a managed store (Hetzner/B2/AWS) the browser hits
  the provider endpoint directly anyway — Traefik was never in that path
  in prod. This change makes dev match prod.

## Config

| Var | Default | Notes |
|---|---|---|
| `DELPHI_INGEST_S3_ENDPOINT_PUBLIC` | `http://localhost:9000` (both tiers) | embedded in minted URLs |
| `INGEST_UPLOAD_PART_URL_TTL_SECS` | 900 | existing |
| `INGEST_DOWNLOAD_URL_TTL_SECS` | **120** (new) | download is confidentiality-sensitive → short |

## Security posture

- **Authz decision stays server-side**: the view-url / sign-part endpoints
  run the tenant/doc check *before* minting. The seam only changes byte
  *transport*, never the access decision.
- Minted URLs are **scoped (exact key + op), short-lived, over a private
  bucket** (HTTPS in prod). Upload = write-only `PUT` (worst case on leak:
  corrupt one in-flight part, still gated by `/complete` JWT + validator);
  download = read `GET` for one object within the TTL.
- Download is the confidentiality-sensitive direction → keep
  `INGEST_DOWNLOAD_URL_TTL_SECS` short. Residual risk is a bounded
  bearer-URL window; the future `CdnAccess`/`StsAccess` minter tightens it
  (no client S3 cred / `jti` revocation / prefix-scoped temp creds) as a
  pure seam swap.
- No open object link in any mode (private bucket invariant).

## Tests

- **Unit:** `S3PresignAccess::mint` returns a URL on the public endpoint,
  correct method, `expires_at ≈ now + ttl`, distinct for download vs
  upload-part.
- **Integration:** `/documents/:key/view-url` returns a URL that actually
  `GET`s the object (MinIO testcontainer / in-process shim); tenant
  isolation — caller can't mint for another tenant's doc.
- **Frontend:** `PdfViewer` fetches `view-url` then renders from the
  returned URL (MSW-mock the endpoint).
- **E2E (tier-2):** upload a small PDF, then open it in the viewer — both
  go browser↔MinIO directly.

## Implementation order

1. **Infra:** drop the `/s3` Traefik router + `minio` service; expose
   MinIO `:9000` in `full.yml`; set `DELPHI_INGEST_S3_ENDPOINT_PUBLIC`. (Fixes
   t2 immediately.)
2. **Seam:** add `AccessMinter` + `AccessGrant` + `S3PresignAccess`;
   inject into `AppState`.
3. **Upload:** route `sign-part` through the seam (no behavior change).
4. **Download:** add `GET /documents/:key/view-url`; remove the streaming
   `/file` handler + `ObjectStore::get_by_url`.
5. **Frontend:** switch `PdfViewer`/open-original to `view-url` + direct
   fetch.
6. **Config + docs:** add `INGEST_DOWNLOAD_URL_TTL_SECS`; update
   `ingestion.md` (delivery section) and `.env.example`.
7. **Tests + e2e.**

## Out of scope

- The `CdnAccess` / `StsAccess` / `ProxyAccess` minters (future, seam is
  ready).
- Document-delete → object-delete and a production orphan cleaner.
