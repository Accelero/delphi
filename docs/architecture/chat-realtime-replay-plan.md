# Chat Realtime Replay Plan

Status: partially implemented backend refinement.

Implemented so far:

- Tenant/conversation event subjects: `chat.events.{tenant_id}.{conversation_id}`.
- `CHAT_REPLAY` metadata bucket stores the current and previous turn sequence
  ranges.
- `CHAT_LOCKS` owner state with `requested` -> `running`, `worker_id`,
  stop fields, lease expiry, lock renewal, and release.
- Worker-specific stop wakeup subject:
  `chat.control.worker.{worker_id}.stop`.
- Stop requests persist through the same lock key, so immediate stop and
  worker claim race through one compare-and-set state machine.
- Interrupted turns commit user + partial assistant content with explicit
  `interrupted = true` and `finish_reason = user_interrupted`.
- JetStream work-queue command ACKs use periodic progress ACKs during active
  turns and final ACK after terminal handling.
- API and realtime service authorize chat access through SurrealDB before
  submitting turns or subscribing to live events.
- Replay on WebSocket subscribe/reconnect uses JetStream sequence cursors and
  sends `resync_required` when the cursor is outside the retained window.
- Runtime timings are configurable through container env vars:
  `CHAT_LOCK_TTL_SECONDS`, `CHAT_EVENTS_MAX_AGE_SECONDS`,
  `CHAT_REPLAY_TTL_SECONDS`, `CHAT_COMMAND_ACK_WAIT_SECONDS`,
  `CHAT_COMMAND_ACK_PROGRESS_SECONDS`, and `CHAT_STOP_POLL_SECONDS`.

Still open:

- Dedicated tests for the real NATS/SurrealDB stop, replay, and reconnect
  paths.

This document captures the target backend behavior for WebSocket chat replay
over NATS JetStream. It intentionally ignores frontend polish.

## Goal

Provide feature parity with the old SSE replay behavior while keeping the new
microservice shape:

- Late joiners can receive the current in-flight turn.
- Reconnects can resume from the last event id when still inside the replay
  window.
- Stale clients get an explicit `resync_required` response and reload durable
  history from SurrealDB.
- Replay state is shared across all realtime-service instances.
- Cleanup does not require a dedicated cleanup worker in the first version.

## Subject Model

Use one JetStream event subject per conversation:

```text
chat.events.{tenant_id}.{conversation_id}
```

Do not include `user_id` in NATS subjects. Conversation ownership and future
sharing are application-level access-control concerns, not transport routing
keys. The subject scope is tenant + conversation:

- `tenant_id`: hard isolation boundary and coarse NATS filter.
- `conversation_id`: chat session identifier.
- `turn_id`: carried in the event envelope and in `CHAT_REPLAY` metadata for
  replay boundaries, but not part of the subject.
- `user_id`: carried in the event envelope for auditing and current owner-only
  filtering, but not part of the subject.

This keeps subject cardinality bounded by active conversations instead of
turns. Physical cleanup is TTL-only for now; replay correctness comes from KV
metadata, not from deleting old events immediately.

The `CHAT_EVENTS` stream should match:

```text
chat.events.*.*
```

## Retention

Use TTL-only cleanup:

- `CHAT_EVENTS.max_age`: 30 minutes
- `CHAT_REPLAY_KV.max_age`: 25 minutes
- `CHAT_LOCKS.max_age`: 15 minutes

Reasoning:

- No normal turn should last 10 minutes.
- Replay metadata expires before event payloads, so realtime normally does not
  try to read sequence numbers that TTL already removed.
- The replay service only replays ranges named by `CHAT_REPLAY`; retained older
  JetStream events are ignored even if TTL has not removed them yet.
- Avoiding per-turn purge keeps the event path append-oriented and avoids extra
  JetStream control-plane churn.

If turns later approach 15 minutes, add lock renewal instead of increasing the
window blindly.

## Worker Identity

Each `chat-worker` process has a stable id for its process lifetime.

Initial implementation:

- Read `CHAT_WORKER_ID` from the environment when present.
- Otherwise generate a random UUID at startup.
- Include `worker_id` in all chat-worker logs.
- Include `worker_id` in active turn ownership state.

Later in Kubernetes, `CHAT_WORKER_ID` can be set from pod metadata, for
example a pod name plus pod UID. The protocol must only require uniqueness
among live workers; the exact id format is operational metadata.

## Replay Metadata

Add a JetStream KV bucket:

```text
bucket: CHAT_REPLAY
key: {tenant_id}/{conversation_id}
```

Value:

```json
{
  "previous_turn": {
    "turn_id": "01...",
    "start_seq": 100,
    "end_seq": 140
  },
  "current_turn": {
    "turn_id": "01...",
    "start_seq": 141,
    "end_seq": null
  }
}
```

The KV entry is the authoritative replay index. JetStream contents are the
physical transport buffer.

## Worker Rotation

On `turn_started`:

1. Publish the `turn_started` event to the per-conversation subject.
2. Use the JetStream PubAck sequence as the turn `start_seq`.
3. Load `CHAT_REPLAY/{tenant_id}/{conversation_id}`.
4. Rotate metadata:
   - old `current_turn` becomes `previous_turn`
   - new turn becomes `current_turn`
5. Write the new replay KV entry.
6. Do not physically purge the evicted turn. TTL removes old events later.

On `finish`, `interrupted`, or `clear`:

1. Publish the terminal event.
2. Store the terminal PubAck sequence as `current_turn.end_seq`.
3. Keep the KV entry until TTL or the next rotation.

## Active Turn Ownership

The active conversation lock must record both the turn and, once known, the
worker that owns execution:

```text
bucket: CHAT_LOCKS
key: {tenant_id}/{conversation_id}
```

Requested value, written by `api-service` before publishing the command:

```json
{
  "turn_id": "01...",
  "state": "requested",
  "worker_id": null,
  "stop_requested": false,
  "stop_requested_by": null,
  "stop_requested_at": null,
  "lease_expires_at": "2026-05-26T10:00:00Z"
}
```

Running value, written by the `chat-worker` after it pulls the command and
verifies the lock still belongs to the same `turn_id`:

```json
{
  "turn_id": "01...",
  "state": "running",
  "worker_id": "chat-worker-8f5f6a5e-2b3a-4f36-b2a1-3e6f4c9c2b41",
  "stop_requested": false,
  "stop_requested_by": null,
  "stop_requested_at": null,
  "lease_expires_at": "2026-05-26T10:00:00Z"
}
```

The worker must update the lock with compare-and-set semantics so a stale
worker cannot steal a lock that has already changed. The lock lease is renewed
while the turn runs.

Stop request fields live on the same `CHAT_LOCKS` key as worker ownership.
This is the race guard for immediate stop: NATS KV cannot atomically read one
key and update another, but it can compare-and-set a single key by revision.

This `worker_id` is used only for control routing and observability. Durable
correctness still comes from:

- the exact `turn_id`
- `stop_requested` on the active lock
- command redelivery when the worker stops ACKing progress

## Stop And Interrupt

Stop is a user request to interrupt the active turn. It is not a separate
JetStream work-queue job.

Stop API behavior:

1. Validate the caller JWT.
2. Authorize conversation access in SurrealDB using tenant and user access
   rules.
3. Read `CHAT_LOCKS/{tenant_id}/{conversation_id}`.
4. If no active lock exists, return `204`.
5. Compare-and-set the same `CHAT_LOCKS` entry to set
   `stop_requested = true`, `stop_requested_by`, and `stop_requested_at`.
   Preserve the current `turn_id`, `state`, `worker_id`, and lease fields.
   If the CAS fails because the worker claimed the turn or another stop
   updated the lock, re-read and retry. If the lock disappeared, return `204`.
6. If the updated lock has a `worker_id`, publish a fast wakeup message to
   that worker:

```text
chat.control.worker.{worker_id}.stop
```

Payload:

```json
{
  "tenant_id": "tenant-a",
  "conversation_id": "01...",
  "turn_id": "01..."
}
```

7. Return `204`.

If the lock is still in `state = requested` and `worker_id = null`, the API
sets `stop_requested` on the lock and returns `204`. The worker will see the
stop flag as part of its ownership claim.

Worker claim ordering:

1. Subscribe to `chat.control.worker.{worker_id}.stop` before making
   `worker_id` visible in `CHAT_LOCKS`.
2. Read `CHAT_LOCKS/{tenant_id}/{conversation_id}` with its revision.
3. Compare-and-set `state = requested` to `state = running`, adding
   `worker_id` and preserving any stop request fields.
4. If the CAS fails, re-read. A concurrent stop and a concurrent claim cannot
   both silently win.
5. After successful CAS, inspect the lock value. If `stop_requested = true`,
   interrupt before starting the provider stream.

Worker stop handling:

- Every worker subscribes to exactly its own control subject:

```text
chat.control.worker.{worker_id}.stop
```

- Do not use a queue group for this subscription. Stop must reach the worker
  that owns the LLM stream.
- The worker keeps an in-memory map from `turn_id` to cancellation token for
  active turns it owns.
- On control message, the worker validates tenant, conversation, and turn,
  then cancels the matching local token.
- While streaming, the worker races provider deltas against the cancellation
  token.
- The worker also periodically rereads or watches
  `CHAT_LOCKS/{tenant_id}/{conversation_id}` during long-running turns as the
  durable fallback if the core NATS wakeup is missed.
- The worker treats `CHAT_LOCKS.stop_requested = true` as authoritative.

On interrupt before normal finish:

1. Stop reading the provider stream.
2. Commit the user message and partial assistant message to SurrealDB.
3. Mark the assistant message or turn with explicit interrupt metadata, for
   example `status = interrupted`, `finish_reason = user_interrupted`, and
   `interrupted = true`.
4. Publish an `interrupted` terminal event.
5. Release the conversation lock.
6. Final ACK the command.

The client learns that stop completed from the `interrupted` realtime event,
not from the HTTP `204`.

Crash behavior:

- If the worker is alive, the worker-specific control publish gives immediate
  cancellation.
- If the API publishes to a stale or dead `worker_id`, the stop fields in
  `CHAT_LOCKS` remain authoritative.
- When progress ACKs stop, JetStream redelivers the original turn command.
- The next worker claims the lock, sees `stop_requested = true`, and commits
  the turn as interrupted without starting or continuing the provider stream.
- Because stop is keyed by `turn_id`, it cannot interrupt the next turn in the
  same conversation.
- Lock TTL removes abandoned stop state if no worker ever handles it.

## Command Consumer ACKs

`CHAT_COMMANDS` uses JetStream work-queue retention with an explicit-ACK
durable consumer shared by all chat-worker instances. A pulled command is
pending for one worker at a time. If the final ACK never arrives before
`ack_wait`, JetStream redelivers the command to another worker.

Chat turns can last several minutes, so the worker must not rely only on a
large `ack_wait`. Use periodic progress ACKs while a turn is healthy:

```text
worker receives TurnRequested
  -> start progress ACK loop, every 30 seconds
  -> process turn
  -> publish finish/interrupted/clear
  -> release conversation lock
  -> final ACK command
  -> stop progress ACK loop
```

Target settings:

- `CHAT_COMMAND_ACK_WAIT_SECONDS`: 120
- `CHAT_COMMAND_ACK_PROGRESS_SECONDS`: 30

The progress interval must be comfortably lower than `ack_wait`.

Final ACK rules:

- ACK only after the worker has emitted a terminal event or intentionally
  decided the command should not retry.
- Release the conversation lock before the final ACK.
- If the worker process dies, progress ACKs stop and JetStream redelivers
  after `ack_wait`.
- If processing is duplicated after a crash or timeout, DB commit remains
  idempotent and stale UI is resolved through replay/resync.

Implementation detail:

- `NatsChatBus` should expose a command progress-ACK method keyed by
  `turn_id`, next to the final ACK method.
- The progress loop should be tied to the turn task lifetime and cancelled
  in a `finally`/drop-safe path.
- If a progress ACK fails, log it and keep processing; final ACK/redelivery
  semantics remain authoritative.

## Realtime Replay

On WebSocket subscribe:

- Authorize the conversation subscription for the authenticated user.
- Load replay KV for the conversation.

Fresh late joiner, no `last_event_id`:

- If `current_turn.end_seq == null`, replay from `current_turn.start_seq`.
- If idle or no KV entry exists, send nothing; DB history is the source of
  truth.

Reconnect with `last_event_id`:

- Treat `event_id` as a JetStream stream sequence.
- If it falls inside `previous_turn` or `current_turn`, replay events after
  that sequence.
- If it is outside the KV replay window, send `resync_required`.
- If JetStream cannot return events that KV says should exist, send
  `resync_required`.

After replay, the socket follows live NATS events for subscribed conversations.

## Realtime Access Control

Realtime authorization must happen before replay and before live subscription
is accepted.

Current model:

- `chat_conversation.tenant_id` is the tenant boundary.
- `chat_conversation.user_id` is the owner.
- `deleted_at = NONE` means the conversation is visible.

For now, `realtime-service` authorizes `subscribe_conversation` by calling the
same storage repository as the API:

```rust
repo
    .get_conversation(&auth.tenant_id, &auth.user_id, &conversation_id)
    .await?;
```

If this returns `NotFound` or `Forbidden`, the service sends a websocket error
and does not add the conversation to the socket's authorized subscription set.

Live forwarding must then require:

```text
event.tenant_id == auth.tenant_id
conversation_id is in socket.authorized_conversations
```

For the current owner-only model, also require:

```text
event.user_id == auth.user_id
```

When shared chat sessions are added, replace the owner-only check with a
membership/ACL table, for example:

```text
chat_conversation_member {
  tenant_id,
  conversation_id,
  user_id,
  role,
  created_at,
  revoked_at
}
```

At that point, `chat_conversation.user_id` remains the owner/creator, but
realtime subscription access is granted by membership rows. The NATS subject
layout does not change because it is already tenant+conversation scoped.

Do not hold a database handle for the lifetime of a WebSocket. Use storage only
for short subscribe-time authorization checks and replay setup.

## NATS Authorization Boundary

NATS can authenticate clients and authorize publish/subscribe permissions by
subject, including tenant-scoped subject filters when service credentials are
issued that way.

For this app, NATS authorization is defense-in-depth between backend services,
not the source of truth for user access:

- Browser clients never connect directly to NATS.
- Backend services use service credentials.
- NATS can restrict a service to subjects such as `chat.events.<tenant>.>`.
- NATS cannot check SurrealDB conversation membership for an end user.
- Per-user and shared-session ACLs must stay in the application layer.

Therefore, use NATS subject permissions for coarse tenant/service isolation,
and use SurrealDB-backed authorization in `api-service` and `realtime-service`
for conversation access.

## Event IDs

Replace UUID event ids with JetStream stream sequence numbers from PubAck.

The WebSocket envelope should carry:

```json
{
  "type": "event",
  "conversation_id": "01...",
  "event_id": "141",
  "event": { "type": "text_delta", "delta": "..." }
}
```

The frontend treats `event_id` as opaque. The backend treats it as a stream
sequence for replay.

## Correctness Rules

- KV metadata defines what is replayable.
- JetStream TTL is storage cleanup, not correctness.
- Replay must never expose events merely because old JetStream messages still
  exist.
- `finish` still means the SurrealDB commit succeeded.
- `interrupted` means the SurrealDB commit succeeded with partial assistant
  content and explicit interrupt metadata.
- `clear` still means no user/assistant message was committed for the turn and
  clients must discard speculative in-flight UI.
- If replay is uncertain, prefer `resync_required` over partial replay.
