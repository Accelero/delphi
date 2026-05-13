# Delphi — Infrastructure Backlog

Things deferred to operations / infrastructure rather than implemented
in-app, with rationale. Tracked here so they don't disappear, and so
deployments know what defences they're expected to provide that the
backend deliberately does **not** enforce itself.

The guiding principle, per [`ARCH.md`](./ARCH.md): the backend is
written as if single-user, with tenancy and identity pushed to the
edges. Some operational defences belong at the same edges — at the
reverse proxy, IdP, or platform — rather than as in-app middleware.
Single-user / private deployments may skip them entirely; multi-tenant
SaaS must implement them at the relevant layer.

Items are listed by ID for cross-reference from [`AUDIT.md`](./AUDIT.md).

---

## I1. Per-route body size limits (was AUDIT H3)

**Concern.** axum's `DefaultBodyLimit` of 2 MB applies to every route.
`/api/ingestion/documents` accepts arbitrary `raw_text` + `metadata`;
`/api/chat` accepts arbitrary message history. A pathological caller
can fill the upstream LLM's context window or persist 2 MB blobs at
will.

**Where it belongs.** Reverse proxy. Stateless byte counting is what
proxies do well; doing it at the edge means an oversized request
never reaches the backend, never opens a connection to the LLM
provider, never spends backend CPU on parsing.

- **Tier-2 stack**: Traefik `buffering.maxRequestBodyBytes`
  middleware, attached per-route via labels. Suggested ceilings:
  `/api/chat` ≤ 64 KB, `/api/ingestion/documents` ≤ 5 MB, others use
  the global 2 MB default.
- **Single-user / private**: defaults are fine; the operator is the
  attacker.
- **In-backend fallback**: not currently implemented. If we ever
  decide we want defence-in-depth (proxy + backend), `axum::extract::DefaultBodyLimit::max(N)`
  is the per-route layer to add.

**Why not in the backend now.** Adding it in-app duplicates the
proxy's job and creates configuration drift between layers (limits
mismatch). For a single-backend-instance setup the proxy layer is
strictly better.

---

## I2. Per-user rate limiting on `/api/chat` (was AUDIT L2)

**Concern.** No rate limit on chat. One authenticated user can spam
`/api/chat`, run up upstream LLM spend, and drown the log volume.

**Where it belongs.** Reverse proxy or API gateway, keyed on a stable
per-user identifier.

- **Tier-2 stack with Traefik**: requires identity to be visible at
  the proxy layer. Two paths:
  1. Configure oauth2-proxy with `set_xauthrequest = true` so it
     appends `X-Auth-Request-User` after forward-auth completes; then
     a Traefik `rateLimit` middleware with
     `sourceCriterion.requestHeaderName: X-Auth-Request-User`.
     Middleware order must be `forwardAuth → rateLimit → backend`.
  2. Cookie-keyed via `sourceCriterion.requestHeaderName: Cookie`
     (cheaper, less correct — same user across browsers / sign-in
     cycles counts separately).
- **Single-user / private**: skip; the operator is the only user.
- **Cloud / external service**: Cloudflare, AWS API Gateway, or a
  dedicated `envoy ratelimit` service can do per-user keying with
  shared state if we ever scale to multiple backend replicas.

**Why not in the backend now.** Per-user rate limiting needs the
user's stable id, which the backend has but the proxy doesn't —
*unless* we wire oauth2-proxy header injection (above). Once that's
in place, the proxy is the better layer (catches the request before
the backend processes it).

The original audit item proposed `tower-governor` in-process. That's
a viable alternative if we ever want defence-in-depth, but defaulting
to "do it at the edge in tier-2, skip in single-user" matches the
project's "push to the edges" principle.

**Operational suggested defaults**: 60 req/min sustained, burst 10.
Tune against actual chat usage once we have telemetry.

---

## How to use this list

- New deferred-to-infra concerns: append a new `I<n>` section, link
  it from the relevant audit item with `_Deferred to infra: see
  [INFRA-BACKLOG.md#i<n>](INFRA-BACKLOG.md#i<n>)._`
- When an item lands at the proxy layer, mark with `[x]` and note
  the merging PR / config change.
- Items here are **not** dropped. They're a contract with operations,
  even when this repo doesn't implement them.
