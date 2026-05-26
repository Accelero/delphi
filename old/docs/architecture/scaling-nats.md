# Horizontal Scaling — NATS Eventing & Work Backbone

Status: **planning.** No NATS in the stack yet. This doc is the migration
plan; sister to [`ARCH.md`](./ARCH.md). It captures the design discussion
and the chosen sequencing; open questions at the bottom are still being
refined.

Companion docs this touches: [`chat-v4.md`](./chat-v4.md) (chat streaming,
already abstracted for this), [`ingestion-unify-on-upload.md`](./ingestion-unify-on-upload.md)
(the upload-path convergence), [`discovery-feed.md`](./discovery-feed.md)
(feed), [`storage-backend.md`](./storage-backend.md) (SurrealDB).

## 1. Why

We want **full horizontal scalability of the backend** — run N replicas
behind the load balancer. The thing that breaks the moment a second replica
exists is the **real-time eventing layer**, which today is **process-local
in-memory**:

- **Chat** — `chat/inprocess.rs` is the `TurnBus`: per-conversation delta
  log, single-flight `running` flag, per-turn cancel token, all in one
  process's `DashMap`. Two tabs of one conversation on different replicas
  diverge; `/stop` issued on replica A can't cancel a turn running on B.
- **Feed** — `ingestion/notifier.rs` broadcasts `FeedItemEvent` over a
  `tokio::sync::broadcast` channel that only exists in the ingesting
  replica's memory. An SSE client on another replica never sees it.

These are not durability problems; they're *fan-out across processes*
problems. The fix is a shared bus. We adopt **NATS** as the single
eventing-and-work backbone for everything we build, with **SurrealDB
remaining the source of truth**.

## 2. What NATS is (and isn't) here

- **Core NATS pub/sub** — ephemeral, best-effort, in-memory fan-out.
  Millions of small msgs/sec on one node. Used for **live event fan-out**
  (chat deltas, feed events). No persistence; replay comes from the DB.
- **JetStream** — durable streams + consumers (work-queues), at-least-once
  delivery, DLQ, replay-by-sequence. Used for **durable work** (ingest
  pipeline) and, where needed, **mid-stream replay**.
- **JetStream KV / Object** — Redis-like KV + blob store on the same
  cluster, available if app-owned state ever needs it (none today).

**NATS does not provide atomicity.** Atomic visibility stays the DB's job
(single-row "publish" flip; see §6). NATS guarantees *delivery and
parallelism*; the DB guarantees *no partial entry is ever visible*.

**NATS is itself horizontally scalable.** Core clustering = full-mesh,
clients connect to any node, subjects route across the cluster. JetStream
replicates streams via Raft (R1/R3/R5) for HA; durable throughput scales by
partitioning across streams/subjects. For our volumes a single node (dev) /
3-node cluster (prod HA) is ample — the LLM provider and DB are the real
ceilings, never NATS.

**Connection model (the thing that makes it scale):** one NATS connection
per **replica**, not per SSE client. Subscriptions are cheap subject
filters. So NATS connection count = number of replicas (tens), not number
of tabs (thousands).

## 3. Scope & non-goals

- **Redis stays.** It is the oauth2-proxy session store, not ours. No
  self-hosted BFF proxy supports a NATS session backend (oauth2-proxy:
  cookie | redis only; Pomerium/Oathkeeper/Authelia use Postgres/Redis),
  and we don't fork the proxy. The only way to drop Redis is oauth2-proxy
  **cookie mode** (stateless sessions) — a separate, deferred decision
  unrelated to NATS. NATS is the backbone for **app-owned** eventing/work;
  the proxy↔Redis pair is a self-contained appliance we leave alone.
- **No new durability semantics for the DB.** SurrealDB stays the truth and
  the reconcile anchor.

## 4. Layering

```
core NATS pub/sub  ── live, ephemeral fan-out ──▶ chat deltas, feed events
JetStream          ── durable work + replay  ──▶ ingest pipeline, (later) knowledge extraction
SurrealDB          ── truth + atomic visibility + reconcile anchor
```

One bus, three jobs. Everything *we* build publishes/consumes here; the DB
remains where correctness lives.

## 5. Migration order

Sequenced deliberately — smallest blast radius first, feed last because it
rides a separate rework:

1. **Chat → NATS** (first). Already abstracted behind `TurnBus`; lowest
   risk, highest "proves the pattern" value.
2. **Ingest pipeline → NATS** (second). Work-queue with parallel stages.
   **Embedding is on the critical path for now.**
3. **Feed → NATS** (last). Folded into a **feed rework**, not done
   standalone.

Each phase ships independently; the bus is introduced in Phase 1 and reused.

## 6. Phase detail

### Phase 1 — Chat (`NatsBus`)

> **This is the former chat-v4 "Phase 2."** [`chat-v4.md`](./chat-v4.md)
> built chat as server-authoritative replication behind a **`TurnBus`**
> trait, shipped single-instance/in-memory as Phase 1, and *deferred* the
> multi-backend backend as a "Phase 2 — Redis (forward sketch)." That
> sketch now **lives here and is rebased onto NATS**; chat-v4.md keeps only
> the shipped Phase-1 design and points at this section for scale-out.

The whole point of v4 is that the multi-backend move is a **drop-in second
`TurnBus` impl** — **no change to the wire contract, worker, handlers, or
client.** `AppState.turn_bus` is selected at startup: `InProcessBus`
(default, Tier 1) or **`NatsBus`** (multi-replica).

v4 splits **DURABLE (SurrealDB: committed messages)** from **EPHEMERAL
(per-conversation delta log + single-flight lock + cancel)**. `NatsBus`
moves only the ephemeral half onto NATS, mapping each in-process primitive
to a NATS one — the trait's `Cursor` is opaque (decimal `u64` in-process),
so the wire's `id:` line is byte-identical either way:

| `TurnBus` primitive (v4 §6) | `InProcessBus` | `NatsBus` |
|---|---|---|
| ordered delta log (replay + live) | `Vec<(Cursor,Bytes)>` + watch | **JetStream** stream `CHAT`, subject `chat.turn.<conv>`; **cursor = stream sequence** |
| log trim / GC (§9) | sweep: grace-trim + evict | stream `MaxMsgsPerSubject` + `MaxAge` |
| `subscribe(from)` (§4.1/§7) | reader loop over the Vec | ordered JetStream consumer **starting after seq `from`**; `from` < oldest retained ⇒ **`resync`** |
| single-flight `try_start` | `running` bool under mutex | **JetStream KV** `chat_locks/<conv>` atomic `create` (Err ⇒ 409); lease **TTL** |
| `cancel` / `/stop` (§8) | flip `CancellationToken` | **core NATS** publish `chat.<conv>.control`; the running replica's subscription flips its local token |
| orphaned-turn cleanup (crashed worker) | `Drop` appends `clear` | KV lease TTL expiry → a **lease-watcher** publishes `clear` to the stream so drainers unstick |

**Read path (the two-tier fan-out, v4 §7).** Each replica runs **one**
ordered consumer **per active conversation it has subscribers for** and
fans out to its local sockets via the existing reader loop — *not* one
consumer per tab. **No sticky LB routing**: any replica can read the
stream, so a tab lands anywhere.

**Single-writer holds across replicas (v4 §8).** The worker is the sole
publisher to `chat.turn.<conv>`; `/stop` publishes a cancel that the
*running* replica turns into a token flip. `clear` XOR `finish` stays
structurally guaranteed — **no distributed phase machine.**

**Mid-generation late-join is free here.** Because the log *is* a JetStream
stream, a joiner mid-turn replays the partial by sequence — the v4 `resync`
mechanism is the exact fallback when the wanted sequence has been trimmed.
(This resolves the "core-NATS-only loses mid-gen replay" worry: backing the
log with JetStream is the chosen answer.)

**Build steps (chat phase):**

1. Add `async-nats` + a NATS service (JetStream on) to Tier-1 compose;
   `DELPHI_NATS_*` config (env-only, fail-at-startup).
2. `backend/src/chat/nats.rs`: `NatsBus: TurnBus` — JetStream stream +
   per-conversation consumer for the log, KV bucket for single-flight +
   lease, core pub/sub for cancel, the lease-watcher for orphan `clear`.
   `Cursor` ← stream sequence.
3. `AppState.turn_bus`: select `InProcessBus | NatsBus` by config; default
   stays in-process.
4. Tests: run the **same `TurnBus` trait tests** against `NatsBus` (dev
   NATS); Tier-2 two-replica e2e — fan-out and `/stop` across replicas,
   reconnect/`resync` across the window.
5. Update `ARCH.md`'s Redis bullet to note NATS as the new *app* dependency
   (Redis stays the proxy's).

**Open chat decisions** (moved from chat-v4 "Open questions"): stream
topology — one `CHAT` stream with per-conversation subjects (assumed) vs
per-turn; single-flight — JetStream KV CAS (assumed) vs SurrealDB
conditional update; confirm `/stream` needs no LB affinity. See §9.

### Phase 2 — Ingest pipeline

The pipeline is a chain of JetStream work-queue subjects (each message
delivered to exactly one worker, acked on success, redelivered on crash).
Steps, in order, **with embedding on the critical path for now**:

```
upload /complete ─ txn ─▶ document(state=staging) + content        [DB: invisible]
                       └─ publish ingest.validate.meta
ingest.validate.meta    → ok → ingest.validate.payload
ingest.validate.payload → ok → ingest.chunk
ingest.chunk            → INSERT chunk rows; fan out one msg per (chunk × model):
                              ingest.embed   ×(N_chunks · M_models)   ◀── parallel core
ingest.embed            → embed + UPSERT chunk.embedding[model] (idempotent by chunk+model);
                          completion derived from state; when all present → ingest.publish
ingest.publish          → UPDATE state='ready'        ◀── single atomic commit point
```

**Atomicity.** Everything above runs on an **invisible** row
(`state != 'ready'`). The only visibility transition is the final
single-row `UPDATE … 'ready'` — one DB transaction, atomic. A crash at any
step leaves a `staging`/`indexing` doc, never a half-visible one. Read paths
(corpus list, metadata/keyword search, vector search, `get_document`) filter
`state='ready'`; that discipline is what the publish flip rests on.

**Schema addition** (`document` has no lifecycle field today):

```surql
DEFINE FIELD IF NOT EXISTS state ON document TYPE string
  ASSERT $value IN ['staging','indexing','ready','failed'] DEFAULT 'staging';
DEFINE FIELD IF NOT EXISTS index_attempts   ON document TYPE int      DEFAULT 0;
DEFINE FIELD IF NOT EXISTS index_claimed_at ON document TYPE option<datetime>;
DEFINE FIELD IF NOT EXISTS index_error      ON document TYPE option<string>;
DEFINE INDEX IF NOT EXISTS document_state   ON document FIELDS state;
```

**Parallelism + barrier.** Every stage scales independently (add workers to
`ingest.embed` without touching chunking). The embed fan-out
(`N_chunks · M_models` independent tasks) is the parallel core; multiple
embedding models are just more messages on `ingest.embed`, each with its own
completion. The **barrier** before the publish flip is **state-derived**, not
a counter: the publish trigger fires when
`COUNT(chunk WHERE doc=$d AND embedding present) == expected` — idempotent
under redelivery (a blind countdown is not; see §7).

**Reliability model — idempotent operations + saga.** (Per decision; §7
details the rules and how this composes with the invisible-until-flip flip.)

**Future option (documented, not now):** make embedding **degradable** —
split visibility into Tier-1 (upload + metadata + payload ⇒ `state='ready'`,
blocking) and Tier-2 (embedding ⇒ per-model `embed_state`, async, tolerated
on failure). A paper with metadata + text is already useful (metadata/keyword
search, reading, doc-chat-over-text); only vector retrieval needs embeddings.
We are **not** doing this yet — embedding stays on the critical path "for
now" — but the schema (`state` + a future per-model `embeddings` map) leaves
room for it.

### Phase 3 — Feed (as part of a feed rework)

Last, and folded into a broader feed rework rather than shipped alone.
Mechanically the smallest: replace the in-process `broadcast::Sender<FeedItemEvent>`
with **core NATS pub/sub** on `feed.<tenant_id>` (best-effort, matching the
current "best-effort by design" contract). Any replica publishes on ingest;
every replica with a feed SSE subscriber relays. Detail deferred to the feed
rework; see [`discovery-feed.md`](./discovery-feed.md).

## 7. Reliability: idempotency + saga

Two properties, by decision, plus the visibility flip that ties them
together.

**Idempotency — mandatory (JetStream is at-least-once; any step can run
twice):**

- validations are pure → trivially idempotent.
- chunk/embed write with **deterministic keys** (`doc+ordinal`,
  `chunk+model`) as **upserts** → redelivery overwrites, never duplicates.
- the publish flip is naturally idempotent.
- **The counter trap:** a blind `pending -= 1` is *not* idempotent (double
  delivery → premature flip / negative). Completion is **derived from state**
  (count embeddings present vs expected), so re-running is harmless.

**Saga — compensating actions per step (choreography via subjects):** on
terminal failure of a step, run the inverse of the work already done
(delete chunks written, abort the multipart upload, etc.) walking backward.
Compensations are themselves steps that can fail, so they are **idempotent**
too, and a **sweeper backstop** (below) covers compensations that never
complete.

**How saga + invisible-until-flip compose:** the flip means nothing partial
is ever *observed*, so compensation is never racing a reader. Saga gives
**eager cleanup** (no orphans linger); the flip gives **atomic visibility**;
the sweeper gives a **safety net** when a process dies mid-compensation:

- **Sweeper / reconcile** — docs stuck in `indexing`/`staging` past a
  timeout (`index_claimed_at`) are requeued or GC'd
  (`DELETE chunk/content WHERE doc=$d`, then drop the row). `DELETE … WHERE`
  is idempotent.
- **S3 lifecycle** — the one external effect (the object PUT before commit)
  is reclaimed by an S3 lifecycle rule expiring unreferenced staging keys,
  not by hand in a compensation.

> Note: with embedding *on the critical path*, the pipeline is long enough
> that eager saga compensation earns its keep (more partial work to reclaim
> on failure). If embedding later becomes degradable (§6 future option),
> most compensation collapses back into "leave it invisible + sweep," since
> the failing step no longer blocks visibility. Revisit the saga surface
> then.

## 8. Infra & deployment

- **Dependency.** `async-nats` (Tokio-native) in the backend.
- **Tier 1 (`docker-compose.yml`).** Add a single-node `nats` service
  (JetStream enabled). Backend connects on startup.
- **Tier 2 / prod (`docker-compose.full.yml`).** 3-node NATS cluster with
  JetStream (R3 streams) for HA. **Redis stays** (oauth2-proxy session
  store) — unaffected.
- **Config.** `DELPHI_NATS_*` env (URL, creds, stream/retention sizing) per
  the env-config convention (prefix, env-only, fail-at-startup).
- **Workers.** Whether ingest consumers run inside backend replicas or as a
  dedicated worker service is **open (§9)** — JetStream consumers make
  either shape a deployment choice, not a code change.

## 9. Open questions (to refine)

1. **Chat stream topology** — one `CHAT` stream with per-conversation
   subjects (assumed) vs per-turn streams; retention sizing
   (`MaxMsgsPerSubject` / `MaxAge`) vs the v4 two-turn buffer.
   *(Mid-gen replay itself is settled: the log is a JetStream stream, so
   replay-by-sequence is native and `resync` is the trim fallback — §6.)*
2. **Single-flight claim** — JetStream KV CAS (assumed) vs SurrealDB
   conditional update for the cross-replica "one turn per conversation"
   guard; plus the orphan-lease TTL value.
3. **Worker topology** — ingest consumers in-process (backend replicas) vs a
   dedicated worker service. Interacts with the separate arxiv-poller
   service (itself just an OAuth-bot client of the upload API).
4. **Saga surface** — exact compensating action per step, and which steps
   are compensated eagerly vs left to the sweeper.
5. **Subject naming & tenant isolation** — `chat.<conv_id>`,
   `feed.<tenant>`, `ingest.*`; whether tenant scoping needs NATS accounts /
   subject-permission boundaries or stays app-enforced.
6. **JetStream sizing** — stream retention, replication factor, ack/redelivery
   limits, DLQ wiring.
7. **Delivery guarantee** — at-least-once (chosen, hence idempotency) vs
   JetStream exactly-once dedup window; whether the latter simplifies any
   step.
8. **Multi-embedding-model timing** — which models run by default, and
   whether models beyond the first should be allowed to lag (a softening of
   "embedding on the critical path").

## 10. Test plan (outline)

- **Unit** — second `TurnBus` impl against the same trait tests as the
  in-process one; idempotency of each ingest step under duplicate delivery;
  state-derived barrier under double-fire.
- **Integration** — backend ↔ embedded NATS (single node) driving a full
  ingest through the subject chain; crash-injection between steps asserts no
  visible partial + correct sweeper/compensation behaviour.
- **E2e (Tier 2)** — two backend replicas + NATS cluster: chat fan-out and
  `/stop` across replicas; feed event delivered to a subscriber on a
  different replica than the ingester.
