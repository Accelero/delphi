# Delphi — Audit Backlog

Single audit file for remaining findings and deferred operational
defences. Historical resolved findings from the old audit were removed;
the code/tests are now the record for closed work.

## Contents

- [Active findings](#active-findings)
- [Deferred to infrastructure](#deferred-to-infrastructure)
- [Planned away](#planned-away)
- [Closed high-risk areas](#closed-high-risk-areas)

## Active findings

- [ ] **M5.** `api/chat.rs` uses `Response::builder().body(...).unwrap()`.
  Static values make this safe today, but typed response constructors would
  avoid a future panic if headers become dynamic.
- [ ] **M6.** `error::Error` can format internal SurrealDB errors. API
  handlers currently return generic strings, but a future direct
  `IntoResponse` could leak query fragments. Keep API error mapping
  explicit.
- [ ] **M9.** Frontend source links need an explicit `http:`/`https:`
  allowlist before more source adapters are enabled.
- [~] **M10.** Feed SSE reconnect state is mostly repaired by periodic
  backend reconnects, but the UI still does not surface connection state.
- [ ] **M14.** Tier-2 logout still shows Keycloak's confirmation screen
  because the end-session redirect lacks `id_token_hint`.
- [ ] **M12.** No instant permission/revocation path. Users retain access
  until the next oauth2-proxy refresh/JWT expiry. Viable edge-only paths:
  shorter access-token TTLs, or replacing the edge with a policy-aware
  proxy/IAP.
- [ ] **L1.** Production builds still link the in-memory SurrealDB engine
  through default features. Gate it to dev/test if binary size or attack
  surface matters.
- [ ] **L3.** A real `DELPHI_PROVIDER_MINIMAX_API_KEY` existed in local
  gitignored `.env`. Rotate it if it was ever a live credential.
- [ ] **L4.** `DELPHI_SERVER_BIND_ADDR` defaults to `0.0.0.0:8081`.
  Local non-Docker runs should default to loopback and require explicit
  `0.0.0.0`.
- [ ] **L6.** Production guard keys only on `DELPHI_ENV=production`.
  Use an allowlist of dev environments so staging cannot boot with dev
  auth by accident.
- [ ] **L7.** Normalize the IdP `tenant_id` claim before provisioning.
  Schema slugs are lowercase; mixed-case claims can create unintended
  tenants.
- [ ] **L10.** Compose-created `data/` can become root-owned and awkward
  for local host runs. Set compose `user:` or document the ownership.
- [ ] **L12.** Discovery-feed "new" highlighting should clear on user
  engagement, not viewport dwell.
- [ ] **S2.** `api/mod.rs::serve` is the composition root and still knows
  about every concrete factory. A future `compose.rs` would keep `api/`
  focused on HTTP routing.
- [ ] **N2.** First-login tenant auto-provisioning is accepted for current
  Tier-2 dev and SaaS shape, but deployments must treat IdP tenant-claim
  issuance as tenant-admission control.
- [ ] **N4.** oauth2-proxy remains pinned to `v7.4.0`. Re-evaluate v7.6+
  when alpha-config session/cookie/server fields are clearer.

## Deferred to infrastructure

- [ ] **I1 / former H3. Per-route body size limits.** Enforce at the
  reverse proxy so oversized requests never reach the backend. Suggested
  Tier-2 ceilings: `/api/chat` <= 64 KB, legacy
  `/api/ingestion/documents` <= 5 MB, global default otherwise.
- [ ] **I2 / former L2. Per-user chat rate limits.** Enforce at the proxy
  or API gateway, keyed by a stable identity exposed after forward-auth.
  Suggested starting point: 60 requests/minute sustained, burst 10.

Single-user/private deployments may skip I1/I2 when the operator is the
only user.

## Planned away

- **Legacy JSON ingestion transactional gaps (former M7/M8).** The
  direct-upload path has an ordered completion transaction and validation
  gates. The remaining legacy JSON path is scheduled to be retired or
  routed through the upload path by
  [`architecture/ingestion-unify-on-upload.md`](./architecture/ingestion-unify-on-upload.md),
  then moved onto the NATS ingest work backbone in
  [`architecture/scaling-nats.md`](./architecture/scaling-nats.md).
- **Process-local live eventing limits.** Current chat/feed live updates
  are correct for one backend replica. Cross-replica fan-out and work
  queues are owned by the NATS plan, not by ad hoc Redis/app-specific
  fixes.

## Closed high-risk areas

These used to dominate the audit and are now closed by implementation and
tests:

- Engine-enforced tenant isolation on domain tables.
- Full JWT path with backend signature validation and SurrealDB
  `app_session` validation.
- Cross-tenant feed/event leakage.
- Tenant-scoped dedup.
- Production guard against default SurrealDB credentials.
- Request DB pool logout-on-drop.
- Chat persistence and v4 server-authoritative streaming.
