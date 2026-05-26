# Multi-Tenancy and Identity

Status: **implemented.** This document records the current model for
tenant isolation and request identity. High-level architecture entrypoint:
[`ARCH.md`](./ARCH.md).

## Model

Delphi uses one codebase for single-user and SaaS deployments. The app
logic is tenant-aware at the boundary, but routine handler code does not
manually thread tenant ids through every query. Tenant isolation is
enforced by SurrealDB.

Core records:

- `tenant` — tenant slug, display data, billing placeholders.
- `app_user` — stable identity keyed by `(iss, sub)` from the JWT.
- `membership` — user-to-tenant association. The schema permits multiple
  memberships; v1 binds one active tenant per token.
- Domain rows — `document`, `document_content`, `chunk`,
  `document_version`, `source_state`, `conversation`, `message`,
  `upload_session`, `ingestion_rejection` — carry `tenant_id`.

## Request Flow

1. The browser sends an HttpOnly BFF session cookie.
2. oauth2-proxy resolves the session and forwards the IdP access token to
   the backend as `Authorization: Bearer <jwt>`.
3. `JwtClaimsExtractor` validates the token in-process using the same
   key material configured for SurrealDB access:
   - HS512 in Tier 1/dev/tests.
   - JWKS in Tier 2/prod.
4. The identity middleware acquires a connection from `RequestDbPool` and
   calls `db.authenticate(jwt)`.
5. SurrealDB validates the JWT again via `DEFINE ACCESS app_session`,
   resolves `(iss, sub)` to an `app_user`, and binds `$auth`.
6. Handlers run with an `AuthedDb`. SurrealDB `PERMISSIONS` clauses
   enforce tenant and user scope on every domain query.

If the user is unknown, the middleware runs a cold-path provisioning step
through `SystemDb`, then retries request authentication once.

## Trust Boundaries

- The backend does **not** trust `X-Auth-*` headers for production
  identity.
- The BFF and IdP remain part of the trusted edge: they own login,
  session refresh, logout, user lifecycle, role assignment, and tenant
  claim issuance.
- The backend performs defence-in-depth JWT validation so direct access to
  the backend port cannot forge identity without a valid signature.
- SurrealDB is the final tenant-isolation boundary. A handler with a bad
  `WHERE` clause still cannot read or write another tenant's domain rows
  through an authenticated request session.

## Privileged Paths

`SystemDb` is the only privileged DB handle. It bypasses SurrealDB
`PERMISSIONS` and is intentionally limited to:

- startup schema application,
- `app_session` JWT access definition,
- tenant/user provisioning on first login,
- source-scheduler cursor writes,
- ingestion rejection writes that user sessions are not allowed to create,
- admin tooling.

Request handlers should prefer `AuthedDb`; adding a new `SystemDb` use
should be treated as an architecture decision.

## Roles

Roles come from the JWT as a JSON array. The backend currently uses the
leaf capability `ingester` to gate upload/ingestion endpoints. Role
hierarchy is configured in Keycloak with composite roles; for example,
`owner` includes `ingester`, so backend checks stay flat.

## Tenant Provisioning

Current behaviour provisions a tenant from the JWT `tenant_id` claim when
needed. This is acceptable because the IdP/BFF configuration is the
tenant-admission control plane in current deployments. Operators must
control which tenant slugs the IdP can emit, and claims should be
normalized before provisioning.

## Schema Invariants

See [`backend/schema.surql`](../../backend/schema.surql):

- domain tables define `tenant_id record<tenant>` with default
  `$auth.tenant_id` and non-`NONE` assertions,
- user-scoped tables also default `user_id` / `user` from `$auth.id`,
- `PERMISSIONS` compare row `tenant_id` to `$auth.tenant_id`,
- system/root sessions must set tenant fields explicitly.

The integration tests in `backend/tests/cross_tenant_isolation.rs` prove
engine refusal on representative cross-tenant reads and writes.
