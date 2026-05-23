# Delphi — Architecture

This document describes how Delphi is built. For *what* it does, see
[SPEC.md](./SPEC.md).

## Guiding principles

- **Build with libraries, not from scratch.** We move fast with coding agents
  and lean on well-maintained components for anything non-differentiating
  (auth, proxy, DB, LLM plumbing, UI primitives).
- **Push tenancy and identity to the edges.** The application code is
  written as if it were single-user. Tenancy and authentication are imposed
  by infrastructure around it.
- **One codebase, two deployment shapes.** The same binary runs as a
  single-user private install or as a multi-tenant SaaS. Only the
  surrounding infrastructure changes.
- **Standard claims, standard interfaces.** The only contract Delphi imposes
  on its environment is a small set of HTTP headers carrying identity. Any
  OIDC provider and any compliant proxy can satisfy it.

## High-level topology

```
 Browser ──cookie──▶ Traefik (BFF)  ──headers──▶ Delphi backend ──▶ SurrealDB
                          │
                          ├── OIDC provider (login, claims)
                          └── Redis (oauth2-proxy session store)
```

- **Frontend.** React SPA. No SSR. Talks to the backend over JSON HTTP.
- **Reverse proxy (Traefik).** Terminates TLS, performs the full
  Backend-for-Frontend auth dance, and forwards authenticated requests to
  the backend with identity headers attached.
- **Backend.** Rust HTTP service. Stateless w.r.t. user identity — every
  request carries its own identity context in headers.
- **Database.** SurrealDB. Multi-model: relational tables, documents,
  vector index, and graph — all in one engine, with built-in record-level
  access control.
- **OIDC provider.** External. Owns user accounts, login UI, MFA, password
  reset, and issues JWTs with the claims the backend needs.
- **Redis.** Server-side session store for oauth2-proxy: each browser
  cookie is an opaque ticket that resolves to an encrypted session
  payload (access + refresh token, claims) keyed by ticket. Survives
  proxy restarts. Not consulted as a blacklist today — see "Logout &
  instant invalidation" below.

## Auth & session model (BFF)

Delphi uses a strict Backend-for-Frontend pattern. The browser never sees a
JWT.

1. Unauthenticated request → Traefik redirects to the OIDC provider.
2. OIDC provider authenticates the user and returns a JWT to Traefik.
3. Traefik stores the JWT server-side (keyed by an opaque session id) and
   sets an `HttpOnly`, `Secure`, `SameSite=Lax` session cookie on the
   browser.
4. On each subsequent request, Traefik:
   - resolves the cookie to the stored JWT,
   - validates the JWT,
   - extracts claims and forwards them as request headers to the backend.

The backend **never** sees or validates the JWT. It trusts the proxy.

### Identity headers (proxy → backend)

The proxy injects a small, standard set of headers derived from JWT claims.
The backend reads these once at the request boundary, behind a single
`ClaimsExtractor` trait, and turns them into a typed `AuthContext`.

Required headers (request fails 401 if any is absent):

- `X-Auth-User-Id` — stable subject identifier (`sub`).
- `X-Auth-Issuer` — issuer URL (`iss`). Together with `User-Id` forms the
  `app_user` primary key — needed so the backend can disambiguate users
  across IdPs. Single-IdP deployments will see one fixed value here.
- `X-Auth-Email` — primary email; informational, used for display/audit.

Optional headers:

- `X-Auth-Name` — preferred display name (`preferred_username` / `name`).
- `X-Auth-Tenant-Id` — tenant the request belongs to. Absent or unknown
  → falls back to the configured `DELPHI_AUTH_DEFAULT_TENANT`.
- `X-Auth-Roles` — comma-separated role list. Parsed into
  `AuthContext.roles` for future per-action authorisation; no
  endpoint consults it today.

The trust boundary is the `ClaimsExtractor` trait. Today the only
implementation reads the headers above and trusts them (the backend must
not be reachable except through the proxy). Adding a second implementation
that validates a `Bearer` JWT inside the backend (defence-in-depth) is a
drop-in: same trait, no caller code changes.

In single-user deployments the proxy can inject a fixed identity, or the
backend's dev mode can synthesise one. **The dev mode is a strict subset of
the production path:** a tiny middleware (compiled in only with the
`dev-auth` cargo feature) writes the same `X-Auth-*` headers, then the
production identity middleware runs unchanged — same extractor, same
upsert, same `AuthContext` reaches handlers. The only thing that changes
between dev and prod is the source of the headers.

### Logout & session lifetime

Logout hits oauth2-proxy's `/oauth2/sign_out`, which clears the BFF
cookie and deletes the matching session in Redis. To also terminate the
IdP-side SSO session, sign-out passes Keycloak's RP-initiated logout
URL as the post-logout redirect (`?rd=…/protocol/openid-connect/logout`),
so the browser visits Keycloak with its own cookies and the SSO session
is killed too. Without that step, the SPA's next request silently
re-authenticates against the still-valid SSO session.

Natural expiry is the only other invalidation path today: oauth2-proxy
cookie TTL (25 min), `cookie_refresh` (20 min — proxy refreshes the
access token transparently before this), Keycloak access-token
lifespan (30 min), idle SSO timeout (30 min), max SSO lifespan (10 h).
The values are tuned so refresh always happens before expiry; users
don't see the seams.

### Instant permission updates (deferred)

There is **no instant revocation today**. After an admin disables a
user or removes a role in Keycloak, the user's BFF session keeps
working until the next token refresh (≤20 min) or JWT expiry
(≤30 min). Stale-access window: up to ~20 min worst case.

**Critical constraint:** "auth lives at the edge" (Guiding
Principles) rules out any solution that would teach the backend
about revocation. No backend-side blacklist check, no backend-side
introspection — `JwtClaimsExtractor` validates a JWT and stops
there. That leaves exactly two options that respect the
architecture:

- **Shorter access-token TTL.** Pure IdP + proxy config change;
  backend is unaware. The "instant" SLO becomes "≤ TTL." Cheapest
  fix, no code, no swap. Mature deployments routinely use minute-
  scale TTLs for exactly this reason.
- **Replace the edge** with one that supports per-request policy
  evaluation natively (e.g. **Pomerium**, **Ory Oathkeeper**, or a
  managed IAP — Cloudflare Access, Google IAP). Revocation is
  enforced at the proxy / policy-decision-point; backend contract is
  unchanged.

upstream oauth2-proxy ships **no** native blacklist hook, no
per-request revocation check, and no back-channel logout endpoint
(see `oauth2-proxy/oauth2-proxy#1224`, `#1684`, both open as of
May 2026), which is why option (a) — staying on oauth2-proxy and
adding revocation at the edge — is not on the menu without forking
the proxy.

Treated as advanced functionality and deferred. Tracked in
[`AUDIT.md`](AUDIT.md) as `M12`.

### User administration

Out-of-band. An external admin panel manages users, tenants, and roles
directly against the OIDC provider. Delphi has no sign-up flow, no
password-reset flow, and no role-editing UI.

## Multi-tenancy & access control

- **Tenancy is a first-class column.** Every domain record carries a
  `tenant_id`.
- **Enforcement lives in the database.** SurrealDB record-level access
  rules scope every read and write to the caller's `tenant_id`. The backend
  cannot accidentally leak across tenants because the database itself
  refuses cross-tenant queries.
- **Roles are application-level and composed in the IdP.** The JWT
  `roles` claim is parsed onto `AuthContext.roles`. The only gate
  today is on the ingestion endpoints (`/api/ingestion/uploads*`),
  which require the leaf `ingester` role. Hierarchy is configured
  via Keycloak composite roles — `owner` includes `ingester`, so
  the backend never needs to know about the hierarchy itself.
  Adding more capability-style roles is a realm config change plus
  one handler line.

## Storage (SurrealDB)

A single SurrealDB cluster hosts every persistence concern Delphi needs:

- **Tabular metadata.** Documents, sources, filters, feeds, users-per-tenant
  caches, etc.
- **Document content.** Full text and structured content alongside metadata.
- **Vector index.** Embeddings for RAG retrieval, queried with
  SurrealDB's vector search.
- **Graph.** Relationships for the future knowledge-management layer.

Choosing SurrealDB collapses what would otherwise be a polyglot stack
(Postgres + pgvector + a graph store) into one operational unit, which keeps
the deployment story simple for both single-user and SaaS modes.

Decision rationale, rejected alternatives, and the schema rules of thumb:
[`architecture/storage-backend.md`](architecture/storage-backend.md). The
authoritative schema lives in `backend/schema.surql`.

## Backend (Rust)

- **HTTP framework.** Axum-style handler + tower middleware stack.
- **Identity middleware.** Reads claims via a `ClaimsExtractor` trait,
  upserts the `app_user` / `membership` rows, and injects a typed
  `AuthContext` into every handler. Requests without a complete identity
  are rejected at this layer. Today's only extractor parses `X-Auth-*`
  headers; a second one validating JWTs in-process is a drop-in.
- **Storage module.** Owns the SurrealDB client. Public interface exposes
  domain operations only; SurrealDB types do not leak across the module
  boundary.
- **LLM module.** Thin abstraction over chat/embedding/tool-use providers
  (built on `rig`). The rest of the codebase depends on this abstraction,
  not on a specific provider SDK.
- **Source-adapter module.** Defines the adapter trait (poll schedule,
  fetch, normalise to a common document shape) and hosts concrete adapters
  (Semantic Scholar first). Adapters run on a scheduler inside the backend
  process; they hand documents to the ingestion pipeline.
- **Ingestion pipeline.** Filter (semantic gate) → embed → persist → notify.
  Each stage is independently testable. The canonical `Pipeline` is wrapped
  in middleware-style `IngestSink` decorators (e.g. `NotifyingSink` for
  Discovery-feed fan-out) so cross-cutting concerns compose without
  changing callers. Document ingestion (upload + direct-to-S3 + metadata
  extraction) is specified in [`specs/ingestion.md`](specs/ingestion.md);
  its architecture is in
  [`architecture/ingestion.md`](architecture/ingestion.md), with the
  forward plan in
  [`architecture/ingestion-roadmap.md`](architecture/ingestion-roadmap.md).
- **API.** JSON HTTP for the SPA. Endpoints are organised by pillar:
  `discovery/*`, `corpus/*`, `chat/*`, eventually `knowledge/*`. The
  Discovery surface ships first — cursor-paginated feed, per-user read
  state, and an SSE stream that pushes new accepted documents to clients.
  Details: [`architecture/discovery-feed.md`](architecture/discovery-feed.md).
- **Chat.** POST `/messages` (fire-and-forget) + per-conversation SSE
  stream every tab subscribes to + conversation-scoped `/stop`. The SSE
  stream is the single source of truth: every tab sees the same events
  in the same order, including late joiners (replay) and the originating
  tab (no special path). Requirements:
  [`specs/chat.md`](specs/chat.md). Architecture and the multi-tab
  state machine: [`architecture/chat.md`](architecture/chat.md).

Module boundaries follow the project rules in `.claude/CLAUDE.md`: each
module exposes a public interface (`mod.rs`); cross-module access goes only
through that interface.

## Frontend (React SPA)

- **Routing/state.** SPA with client-side routing. No SSR.
- **Auth.** The SPA assumes it is already authenticated — if a request
  401s, it lets the proxy handle the redirect to the OIDC provider.
- **Production serving.** Vite builds a static bundle (`dist/`) at
  image-build time. A two-stage `frontend/Dockerfile` copies the
  bundle into a `caddy:2-alpine` image that serves it on `:80` with
  SPA fallback (`try_files {path} /index.html`), zstd/gzip
  compression, and `immutable` cache headers on hashed assets. The
  resulting `delphi-frontend` image sits behind Traefik like any
  other upstream — Traefik handles TLS, routing, and the BFF chain;
  Caddy just serves files. Tier-1 dev keeps the Vite dev server for
  HMR; Tier-2 runs the production Caddy image so e2e validates the
  actual bytes that ship.
- **Chat surface.** Reusable component used for both corpus-RAG chat and
  per-document analysis chat. Markdown + reasoning rendering, citations
  inline. EventSource-driven; the same component renders identically on
  every tab subscribed to a conversation. See
  [`architecture/chat.md`](architecture/chat.md).
- **Discovery feed.** Reverse-chronological infinite scroll over the
  user's corpus. Cursor pagination via TanStack Query's `useInfiniteQuery`,
  live-prepend via SSE, native `overflow-anchor` for scroll preservation,
  IntersectionObserver-driven "newness" fade, optimistic mark-read.
  Details: [`architecture/discovery-feed.md`](architecture/discovery-feed.md).
- **Theme.** Token-based theming so the same surface drops into different
  product areas.

## Deployment

Two reference stacks ship in the repo, each backed by a compose file:

- **Tier 1 — `docker-compose.yml`.** Fast inner-loop dev: SurrealDB +
  backend (with the `dev-auth` cargo feature) + Vite frontend. The
  backend's dev injector writes `X-Auth-*` headers itself, so the
  production identity middleware runs unchanged.
- **Tier 2 — `docker-compose.full.yml`.** Full prod-shape stack: Traefik
  (entry point + forward-auth) + Keycloak (OIDC IdP) + oauth2-proxy
  (BFF) + Redis (session store) + SurrealDB + backend (built without
  `dev-auth`) + **frontend served by Caddy from a built bundle**
  (`frontend/Dockerfile`). Browser only sees the Traefik origin;
  the backend receives an IdP-issued JWT as `Authorization: Bearer`.
  Both backend and frontend are built once in PR-CI and the same
  bytes are promoted to GHCR on merge — the e2e suite tests the
  artifact that ships, not a dev variant of it.

Production deployment is Tier 2 with the IdP, secrets, TLS, and the
backend image as the only deltas from the dev compose file. "Works in
Tier 2" is therefore a strong precondition for "works in prod".

Single-user / private deployments can run either tier. SaaS deployments
run Tier 2 against a managed SurrealDB cluster (with record-level access
rules enabled), managed Redis, and a real OIDC provider.

## Testing

See [`architecture/testing.md`](architecture/testing.md) — three tiers
(unit / integration / e2e), tooling per tier, repo layout, CI cadence,
vibe-coded guardrails, and operational details.

## What we explicitly do *not* build ourselves

- User accounts, login UI, MFA, password reset → OIDC provider.
- Session management, JWT validation, cookie issuance → Traefik.
- Cross-tenant isolation enforcement → SurrealDB access rules.
- Embedding models, LLM inference → external providers via `rig`.

Anything in this list is non-differentiating. Owning it would slow us down
without making the product better.
