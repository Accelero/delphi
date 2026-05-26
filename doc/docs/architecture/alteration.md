# Alteration Notes

These are walkthrough decisions to apply to the main architecture and gap
analysis after the current chat workflow review is complete.

## Transient Turn State In NATS KV

Decision under review:

- Move transient active-turn state out of SurrealDB lifecycle rows and into
  `CHAT_LOCKS`.
- Treat `CHAT_LOCKS` as the authority for `requested`, `running`, active
  ownership, lease expiry, bounded prompt payload/reference, and stop flags.
- Treat SurrealDB as the authority for visible outcomes: committed messages,
  interrupted partial assistant messages, conversation metadata, and optional
  failure markers.

Motivation:

- Avoid orphan `chat_turn(status=requested)` rows if the API crashes between
  DB write, KV lock creation, and NATS publish.
- Let abandoned requested/running work expire through the KV lease/TTL.
- Keep the hot coordination path in NATS while DB remains committed truth.

Proposed submit flow:

1. API authenticates and authorizes conversation access.
2. API validates `parent_message_id` against the committed DB tail.
3. API creates `CHAT_LOCKS/<tenant_id>/<conversation_id>` with the requested
   turn payload.
4. API publishes `TurnRequested` with IDs only.
5. Worker reads the requested payload from `CHAT_LOCKS`.

Requested lock payload:

```json
{
  "turn_id": "01...",
  "user_message_id": "01...",
  "parent_message_id": "01...",
  "text": "bounded prompt text for now",
  "state": "requested",
  "worker_id": null,
  "stop_requested": false,
  "stop_requested_by": null,
  "stop_requested_at": null,
  "lease_expires_at": "2026-05-27T10:00:00Z"
}
```

Notes:

- `text` must be bounded by the chat message limit while no separate
  content/object store exists.
- Later, large prompts should use `text_ref` instead of inline text.
- `chat_turn` can remain as optional audit/debug state, but should not be
  required for requested/running coordination.
- On worker crash, stale running state should converge to error/resync and let
  the user reprompt, not silently regenerate a different answer.

## Conversation-Targeted Stop

Decision under review:

- Stop is targeted by conversation, not by worker identity.
- Remove `worker_id` from the `CHAT_LOCKS` schema if it is no longer needed
  for ownership validation/debugging.
- API sets `stop_requested=true` on `CHAT_LOCKS`.
- If the lock is running, API publishes:

```text
chat.control.<tenant_id>.<conversation_id>.stop
```

- The active worker subscribes to that subject only while it owns the turn.
- `CHAT_LOCKS.stop_requested` remains authoritative if the control wakeup is
  missed.

Scale note:

- This creates one plain NATS subscription per active turn instead of one per
  worker.
- That is acceptable for expected chat-worker concurrency, but should be load
  tested if workers handle very high numbers of concurrent turns.
