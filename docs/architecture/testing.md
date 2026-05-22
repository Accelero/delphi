# Delphi — Testing Strategy

How testing is organised in this repo, and why. Sister doc to
[`ARCH.md`](../ARCH.md) (which references this one).

The strategy is shaped by one constraint that doesn't usually get
top-billing: **most of this code is written with help from coding
agents.** That changes the failure modes the test suite has to catch,
and it pushes a few tier-by-tier decisions in directions a human-only
team might pick differently. Those decisions are called out below.

## Goals

- **Catch wiring bugs**, which is what LLM-generated code is most prone to.
  Unit-correct code that fails at module boundaries.
- **Catch silent regressions**, especially in code an agent might "clean
  up" later (protocol writers, parsers, header extractors).
- **Stay fast at the inner loop.** The cost of a slow test suite is
  measured in skipped runs, not seconds.
- **Match dev to prod.** The test stack must look like production, not a
  parallel reality with its own bugs.
- **Stay simple.** Three tiers, four tools, no zoo.

## The three tiers

```
Unit ─────────────► colocated with source
                     - backend: inline #[cfg(test)] mod tests {}
                     - frontend: <name>.test.tsx next to <name>.tsx

Integration ──────► two homes (forced by language conventions)
                     - backend: backend/tests/*.rs (Rust integration tests)
                     - frontend: colocated next to the highest-level
                                  component in the test's scope

E2E ──────────────► tests/ (root)
                     - Playwright against either Tier 1 or Tier 2
                     - tier1 = dev-auth stack; tier2 = full BFF stack
```

That's the entire ladder. There is **no separate frontend integration
folder**, **no `frontend/tests/`**, and **no fourth tier** between
integration and e2e. Anything that doesn't fit a tier above is e2e.

## The placement rule

> **Tests live where their owner lives — unless the language forces them
> out.** Unit tests live with the unit. Integration tests live with the
> highest-level component in their scope. Full-stack tests live in the
> root.

The "language forces them out" carve-out applies to **Rust integration
tests only.** Cargo compiles `backend/tests/*.rs` as separate binaries
that import the crate as an external library — they cannot live inline.
TypeScript has no equivalent constraint, so the rule applies cleanly:
even a test that renders five components together belongs next to
whichever component owns that subtree's entry point.

When a test really has no good owner — it spans the entire app — that's
the signal it's an **e2e test** and belongs in `tests/`.

## Repo layout

```
delphi/
├── tests/                          # full-stack e2e (Playwright)
│   ├── playwright.config.ts        #   tier1 + tier2 projects
│   ├── package.json                #   isolated; @playwright/test only
│   ├── helpers/                    #   compose helpers, OIDC login
│   ├── fixtures/                   #   seed data
│   └── e2e/                        #   *.spec.ts
│
├── backend/
│   ├── src/                        # #[cfg(test)] mod tests inline
│   ├── tests/                      # cross-module integration
│   │   ├── common/                 #   build_test_app, in-mem DB, fakes
│   │   ├── auth_pipeline.rs
│   │   └── health.rs
│   └── Cargo.toml                  # [lib] + [[bin]] split
│
└── frontend/
    ├── src/                        # *.test.{ts,tsx} colocated
    │   ├── components/
    │   │   ├── chat.tsx
    │   │   └── chat.test.tsx
    │   └── lib/
    │       ├── api.ts
    │       └── api.test.ts
    ├── test-utils/                 # shared rig (NOT a test directory)
    │   ├── setup.ts                #   Vitest setup file
    │   ├── render.tsx              #   RTL wrapper with providers
    │   ├── fixtures.ts
    │   └── msw/
    │       ├── server.ts
    │       └── handlers.ts
    ├── vite.config.ts
    └── vitest.config.ts            # extends vite.config.ts
```

A note on naming: `frontend/test-utils/` is **not** a tests directory.
It contains zero test files — only the rig (Vitest setup, MSW handlers,
RTL render wrapper, fixtures). Calling it `tests/` would imply tests
live there, which they don't (they're colocated).

## Tooling

| Tier | Tool | Notes |
|---|---|---|
| Rust unit + integration | `cargo test` | Built-in. Drives the full axum router via `tower::ServiceExt::oneshot()` — in-process, no network, every middleware layer attached. |
| TS unit + component + integration | **Vitest** | Reuses `vite.config.ts` (plugins, aliases, JSX). Pairs with `@testing-library/react` + **MSW** for `/api/*` mocks. |
| In-test database | **Embedded SurrealDB `Mem` engine** | `Surreal::<Any>::connect("memory")`. Same code path as prod (which uses `ws://`); the engine is selected at runtime by URL. Fast, no docker, no testcontainers. |
| Fake LLM | hand-rolled `LlmClient` impl | Lives in `backend/tests/common/fake_llm.rs`. Default emits one `Text("ok")` delta; tests override with `with_script(...)`. |
| Full-stack e2e | **Playwright** | Multi-browser, auto-waiting, trace viewer. Two projects (tier1, tier2); untagged tests run in both, `@tier2` in a test name scopes to tier 2. |
| Backend↔frontend type drift | **ts-rs** *(planned)* | Derive on every API request/response struct. CI fails on diff of `frontend/src/types/api.gen.ts`. **Not yet bootstrapped.** |
| Coverage (signal, not gate) | `cargo-llvm-cov`, Vitest `--coverage` | Reported, not enforced. |

## Two stack profiles for e2e

E2E tests run against one of two compose stacks (see ARCH.md and
`docker-compose*.yml`):

- **Tier 1 (`docker-compose.yml`).** SurrealDB + backend (with the
  `dev-auth` cargo feature) + Vite frontend. The dev injector writes
  `X-Auth-*` headers itself; downstream auth pipeline runs unchanged.
  Fast smoke tests. URL: `http://localhost:5173`.
- **Tier 2 (`docker-compose.full.yml`).** Traefik + Dex (OIDC IdP) +
  oauth2-proxy (BFF) + Redis + SurrealDB + backend (no `dev-auth`) +
  frontend. Tests the full auth perimeter. URL: `http://localhost`.

Tests opt into Tier 2 by including `@tier2` in the test title:

```ts
test("OIDC login dance @tier2", async ({ page }) => { … });
test("chat round-trip", async ({ page }) => { … });   // both tiers
```

## CI cadence

The four levels of test invocation, fastest to slowest:

| Trigger | Runs | Wall time |
|---|---|---|
| **On save** (developer) | Vitest watch + `cargo check` | <2 s |
| **Pre-push** (git hook) | Full `cargo test` (both feature configs) + Vitest + (planned) ts-rs drift check | ~30 s |
| **PR** | All of the above + Playwright Tier 1 | ~2 min |
| **Nightly + on `main`** | All of the above + Playwright Tier 2 | ~5 min |

The pre-push hook is the gate. PR-CI is the safety net. Nightly is the
long-tail (OIDC dance, full-stack flakes).

## Vibe-coded guardrails

These exist to catch the specific failure modes coding agents introduce
that hand-written code typically doesn't:

- **Property tests** for `HeaderClaimsExtractor` (auth boundary parser).
  Generates arbitrary `HeaderMap`s and asserts `extract().is_ok() ↔`
  required headers present and non-empty. *Planned.*
- **Snapshot tests** on the SSE protocol writers in `api/sse.rs`
  (`sse::user_message`, `sse::text`, `sse::citations`, `sse::error`,
  `sse::finish`, `sse::clear`, `sse::resync`). Byte-level snapshots so the
  first time someone "cleans up" the streaming code, the diff is loud.
  *In place* (`api/sse.rs::tests`).
- **Type-drift check** via ts-rs in CI: regenerate, fail on
  `git diff frontend/src/types/api.gen.ts`. *Planned.*
- **Equivalence test** between dev-mode header injector and the
  production header extractor — already in place
  (`auth/dev.rs::tests`).

These three are the highest-leverage additions; everything else stays
in unit/integration/e2e tiers.

## Operational details

A few non-obvious things you need to know.

### Backend: lib + bin split

`backend/Cargo.toml` declares both `[lib]` and `[[bin]]`, sharing the
same package name. The lib is what `tests/*.rs` imports
(`use delphi::api::build_router`); the bin is a thin wrapper that calls
`delphi::api::serve(...)`. This is forced by Cargo: integration tests
are compiled as separate binaries that consume the crate externally,
so there must be a library to consume.

### Backend: `Surreal<Any>` over `Surreal<Client>`

`SurrealStorage` types on `Surreal<Any>` (the runtime-polymorphic
engine), not `Surreal<Client>` (WebSocket-only). Production passes
`ws://surrealdb:8000/rpc`; tests pass `memory`. Same code path, same
upserts, same schema. The `kv-mem` Cargo feature is on by default so
the in-memory engine is always available — including in release builds,
which makes future "spin up an embedded DB at startup" use cases
possible without a rebuild.

### Frontend: Bun installs, Node runs Vitest

Bun is the package manager and dev-server runtime. **It is not the test
runner.** Vitest's worker pool (tinypool) trips over Bun's
`child_process.spawnSync` shim with
`Cannot access 'dispose' before initialization`. The canonical command
is `make frontend-test`, which runs Vitest under a Node container.
`bun run dev` and `bun install` work fine.

### Vitest extends Vite

`frontend/vitest.config.ts` `mergeConfig`s `vite.config.ts` so test
files see the same JSX transform, the same `@/` alias, and the same
plugins as the dev server. Don't duplicate aliases or plugins — extend
the Vite config.

## Mental model

One sentence: **unit tests live with their unit, integration tests live
with their owner, full-stack tests live in `tests/`, and the only
exception is Rust forcing integration tests into `backend/tests/`.**

If you can't figure out where a new test belongs, ask: "what is this
test the test of?" The answer is a file or a feature. Tests go next to
that file, or in the appropriate `tests/` if cross-cutting.

## What's not yet implemented

Listed so the gap between "designed" and "running" is honest:

- `ts-rs` type generation + drift check.
- Property tests on `HeaderClaimsExtractor`.
- A frontend component test (e.g. `user-menu.test.tsx`).
- CI workflow wiring the four cadences.

Each is a small, independent PR; the harness for each is in place.
