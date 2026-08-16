---
title: Chat Failure Analysis
description: Crash behavior, recovery expectations, and invariants for chat services.
---

# Chat Failure Analysis

Crash behavior is documented as a failure matrix. The matrix is the source of
truth for recovery expectations; diagrams only show the common paths.

## API Crash Matrix

| Crash point | Durable state | Recovery | User-visible result | Required invariant |
| --- | --- | --- | --- | --- |
| Before auth/access completes | none | client retries | request fails or times out | no side effects before auth |
| After access check, before lock | none | client retries | request fails or times out | no accepted work without lock |
| After requested KV lock, before publish | requested lock only | lock lease/TTL expires | conversation may be temporarily `in_flight` | requested lock must expire |
| After publish, before HTTP response | lock + command | worker processes command | client may timeout, reload sees result | command is durable after PubAck |
| After HTTP `202` | lock + command | worker owns rest | normal stream or reload result | API does not need to stay alive |

Transient requested state and the bounded prompt payload live in `CHAT_LOCKS`,
so an API crash does not leave orphan `chat_turn(status=requested)` rows.

## Worker Crash Matrix

| Crash point | Durable state | Recovery | User-visible result | Required invariant |
| --- | --- | --- | --- | --- |
| Before reading command | command pending | JetStream redelivers | no visible change | command not ACKed |
| After command read, before claim | requested lock | JetStream redelivers | no visible change | command not ACKed |
| After claim, before provider | running lock | lease expires, redelivery sees stale running | error/resync, user can reprompt | stale running must not silently regenerate |
| During LLM stream | running lock, live events may be visible | lease expires, redelivery marks failed/resync | stream stops, error/resync | partial visible stream is not treated as committed truth |
| After DB commit, before KV terminal marker | committed DB state, running KV lock | current stale-lease recovery can mark failed unless it reconciles DB by `turn_id` first | reload can show committed answer while live path may emit error/clear | known gap: DB commit must become discoverable before stale-running failure |
| After KV terminal marker, before finish event | committed DB state + terminal KV marker | redelivery publishes missing terminal event, ACKs, and releases | reload shows committed answer; finish may arrive late | DB commit precedes terminal event |
| After finish event, before DB commit | invalid ordering | avoid by design | not allowed | terminal event must never precede DB commit |
| After DB commit + finish, before ACK | committed DB state, terminal KV marker, command unacked | redelivery ACKs without rerun | no duplicate answer | terminal state is idempotent |
| After ACK, before lock release | committed/interrupted/failed outcome, terminal KV lock may remain | no command redelivery remains; KV TTL eventually clears lock | no duplicate answer, but new turns can be temporarily blocked | ACK-before-release trades no rerun for possible lock TTL wait |

## Redelivery Decision Table

| Observed state on redelivery | Action |
| --- | --- |
| No lock | record/publish failed cleanup, ACK; this is abnormal because payload is missing |
| Requested lock for same `turn_id` | claim and process |
| Running lock with fresh lease | do not process; do not final ACK |
| Running lock with stale lease | mark failed, publish cleanup, ACK after terminal handling |
| Stop flag set before provider start | commit interrupted/failed stop outcome without LLM call |
| Terminal committed/interrupted/failed KV marker exists | publish missing terminal event if needed, ACK, release |

## Ordering Invariants

- Only one active lock may exist per conversation.
- A successful API response requires a PubAck for the command.
- A terminal realtime event is published only after DB commit succeeds.
- The worker final-ACKs the command only after terminal handling, then releases
  the KV marker.
- Stale running turns do not silently regenerate a different answer.
- Stop is scoped to tenant, conversation, and turn.
- Realtime subscription is authorized before NATS events are consumed.

## Saga View

```d2
direction: down

Submit: "Submit turn"
Lock: "Create requested KV lock"
Command: "Publish TurnRequested"
Claim: "Worker claims running lock"
Stream: "Stream LLM deltas"
Commit: "Commit visible DB outcome"
Marker: "Mark KV terminal"
Terminal: "Publish terminal event"
Ack: "ACK command"
Release: "Release KV marker"
Fail: "Error/resync + release"

Submit -> Lock -> Command -> Claim -> Stream -> Commit -> Marker -> Terminal -> Ack -> Release
Claim -> Commit: "stop already requested"
Stream -> Commit: "stop"
Stream -> Fail: "fatal error"
Fail -> Ack
Claim -> Fail: "stale/race detected"
```
