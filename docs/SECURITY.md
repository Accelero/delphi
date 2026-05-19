# Delphi — Security Notes

Living document for security-relevant decisions, threat-model notes,
and defences that are deferred to specific deployment shapes rather
than shipped in every build.

Sister doc to [`ARCH.md`](./ARCH.md) (architecture), [`AUDIT.md`](./AUDIT.md)
(point-in-time findings), and [`INFRA-BACKLOG.md`](./INFRA-BACKLOG.md)
(things pushed to operations). Anything here should explain *why* a
defence is shaped the way it is, not just *what* it does.

---

## Ingestion API trust model

The document ingestion API (`/api/ingestion/*`) is first-class and
treated as **untrusted on every call**. The same surface is hit by:

- The SPA (any authenticated user).
- In-process source adapters running under a service identity.
- Future custom adapters and external integrations.

There is no privileged client path. Identity is the JWT, scope is the
tenant claim, and every validation that protects the system has to be
on the server side of that boundary. The new ingestion endpoints
(`/api/ingestion/uploads*`) require the `ingester` role in the JWT.
Role hierarchy is configured in Keycloak via composite roles — `owner`
is a composite that includes `ingester` — so the backend only ever
checks for the leaf capability (`ingester`) and never has to know
about the hierarchy. Adding higher-tier roles is a realm config
change, not a code change.

### Layered validation

Defences are arranged in layers; each layer assumes the previous one
may have been bypassed.

1. **At `create`.** Declarative checks against client-supplied
   metadata: content-type allowlist, size cap, metadata schema /
   depth / size, `canonical_id` shape.
2. **In-flight.** `CreateMultipartUpload` records the declared
   `Content-Type` on the object's metadata. S3 does **not** verify
   that the uploaded bytes match the declared type, and SigV4 does
   not support a per-part `Content-Length` ceiling (only exact
   match, which breaks the natural smaller last part). Byte-level
   enforcement of declared content-type and overall size therefore
   happens at `/complete` in the validator (layer 3 + 4), not at
   S3. See [`architecture/ingestion-v2.md`](architecture/ingestion-v2.md)
   ("What S3 actually enforces") for the corrected model.
3. **At `complete`.** Backend `HEAD`s the object; rejects if the
   actual size doesn't match the declared size, or the content-type
   on the object doesn't match what was signed for.
4. **Async validation.** Magic-byte sniffing (`infer`/`tree_magic`)
   plus a bounded format parse (PDF: timeout-and-cap, UTF-8 decode
   for text). Mismatch or parse failure → reject.
5. **At consumption.** Every consumer treats stored artefacts as
   untrusted: extractors are sandboxed (timeout, memory cap,
   output-size cap, `kill_on_drop`); rendered text is sanitised
   before DOM insertion; URLs are scheme-allowlisted on render.

### Rejection policy: delete, do not quarantine

When async validation rejects a document, the document row and the S3
object are **deleted immediately** in one transaction. There is no
`quarantined` or persistent `failed` state on the `document` table.

Rationale: keeping rejected rows in the same table as serveable rows
makes every read path responsible for filtering them out. Forgetting
the filter once leaks rejected content into a list, search result, or
RAG retrieval. Removing the row entirely makes the leak structurally
impossible — there is nothing to forget about.

Diagnostic information about the rejection (tenant, doc_id, sniffed
type, reason, timestamp) goes to structured logs, not to a queryable
table. The originating client sees the rejection through a short-TTL
status endpoint and/or an SSE event.

If a future deployment needs durable forensics on rejected content
(regulatory or threat-intel reasons), introduce a dedicated
`ingestion_audit_log` table written only by the validator and read
only by an admin surface. Do not reuse the `document` table.

### Antivirus / malware scanning — deferred to production

ClamAV (or equivalent) is **not** part of the development and tier-2
e2e stacks. The MVP validator does magic-byte + format-parse only;
that catches lying-about-type and corruption, but not known malware
signatures.

For deployments where untrusted users upload files (multi-tenant SaaS,
public-facing single-tenant), an AV scanner runs as a sidecar in the
validation pipeline:

- ClamAV `INSTREAM` socket, scanned between `uploaded` and `ready`
  states.
- Sidecar container in the production compose / k8s manifests; not in
  `docker-compose.yml` or `docker-compose.full.yml`.
- Operational cost: ~500 MB RAM resident + a few seconds CPU per file.
  Signature database refresh on a cron.

Single-user / private deployments may skip AV entirely; the operator
is the only uploader.

This is the deployment-time defence; the dev stack stays slim.

---

## How to use this document

- New trust-model decisions, defence-in-depth choices, or "we explicitly
  do *not* defend against X because Y" notes belong here.
- Point-in-time findings (a specific vulnerable line of code) belong in
  [`AUDIT.md`](./AUDIT.md) with an ID.
- Operational defences deferred to infrastructure (rate limits, body
  caps at the proxy) belong in [`INFRA-BACKLOG.md`](./INFRA-BACKLOG.md).
- This file is for the *why* — context that survives across audits and
  refactors.
