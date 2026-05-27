---
title: Chat Request Flow
description: Submit-to-commit, stop, and browser reconciliation flow for chat turns.
---

# Chat Request Flow

This page describes what happens when a user submits a chat message.

## Submit-To-Commit Path

This diagram follows the current implementation for
`POST /api/chat/conversations/:id/turns` when the browser already has, or
creates, a realtime subscription for the conversation. Steps prefixed with a
letter and number are state-changing operations; reads, validation, and routing
are left unnumbered unless they decide whether a state change is allowed.

```d2
shape: sequence_diagram

Browser: Browser UI
BrowserState: Browser local state
API: api-service
DB: SurrealDB
KV: NATS KV CHAT_LOCKS
JS: NATS JetStream CHAT_COMMANDS
WorkerState: chat-worker memory
Worker: chat-worker
LLM: LLM provider
Events: NATS JetStream CHAT_EVENTS
Replay: NATS KV CHAT_REPLAY
RT: realtime-service
RTState: realtime-service memory

Browser has no active conversation subscription: {
  Browser -> RT: WS /ws/chat
  RT -> RTState: R01 create socket maps and queues
  Browser -> RT: subscribe_conversation(conversation_id,last_event_id)
  RT -> DB: authorize visible conversation
  DB -> DB: R02 ensure principal rows / last_seen_at
  RT -> Events: R03 create exact-subject consumer
  RT -> RTState: R04 create/reuse fanout hub
  RT -> RTState: R05 insert ActiveSubscription
  RT -> RTState: R06 record replay cursor
  RT -> Browser: subscribed
  BrowserState -> BrowserState: R07 realtimeStatus=connected
}

Browser -> API: POST /api/chat/conversations/:id/turns
BrowserState -> BrowserState: S06 clear draft, status=submitted
API -> API: validate JWT
API -> API: validate ULIDs and text
API -> DB: load visible conversation and tail
DB -> DB: S07 ensure principal rows / last_seen_at
API -> DB: check parent_message_id equals tail
Existing lock is expired: {
  API -> KV: S08 purge expired CHAT_LOCKS key
}
API -> KV: S09 create requested lock with prompt payload
Lock handoff: {
  API -> Browser: 409 in_flight
  BrowserState -> BrowserState: S10 restore draft, status=error, refresh
  API -> JS: S11 append TurnRequested wakeup
  JS -> API: PubAck
  API -> Browser: 202 Accepted
}

JS -> Worker: deliver TurnRequested
WorkerState -> WorkerState: S12 store JetStream ack handle
Worker -> KV: S13 CAS requested -> running, read payload, refresh lease
WorkerState -> WorkerState: S14 start maintenance/watchers
Worker -> DB: load committed history
DB -> DB: S15 ensure principal rows / last_seen_at

Worker -> Events: S16 append turn_started
Events -> Worker: PubAck sequence=start_seq
Worker -> Replay: S21 update current_turn start_seq
Events -> RT: deliver turn_started
RT -> RTState: S22 update last_sent_sequences
RT -> Browser: WebSocket event turn_started
BrowserState -> BrowserState: S23 status=submitted, clear error

Worker -> Events: S20 append user_message
Events -> RT: deliver user_message
RT -> RTState: S25 update last_sent_sequences
RT -> Browser: WebSocket event user_message
BrowserState -> BrowserState: S26 append visible user, status=streaming

Worker -> LLM: start stream
For each provider text chunk: {
  LLM -> Worker: text chunk
  WorkerState -> WorkerState: S27 append assistant_text
  Worker -> Events: S28 append text_delta
  Events -> RT: deliver text_delta
  RT -> RTState: S29 update last_sent_sequences
  RT -> Browser: WebSocket event text_delta
  BrowserState -> BrowserState: S30 append overlayText / assistant-live
}

Maintenance while turn is running: {
  Worker -> JS: S31 in-progress ACK
  Worker -> KV: S32 renew CHAT_LOCKS lease
}

WorkerState -> WorkerState: S33 generate assistant_message_id
Worker -> DB: commit completed turn transaction
DB -> DB: S34 ensure principal rows / last_seen_at
DB -> DB: S35 delete branch messages
DB -> DB: S36 create committed user message
DB -> DB: S37 create committed assistant message
DB -> DB: S38 update conversation metadata
DB -> DB: S39 upsert terminal chat_turn committed
Worker -> KV: S40 mark CHAT_LOCKS terminal committed with assistant_message_id

Worker -> Events: S41 append finish
Events -> Worker: PubAck sequence=end_seq
Worker -> Replay: S42 update current_turn end_seq
Events -> RT: deliver finish
RT -> RTState: S43 update last_sent_sequences
RT -> Browser: WebSocket event finish
BrowserState -> BrowserState: S44 replace live message, status=ready
Browser -> API: GET /api/chat/conversations/:id
API -> DB: load committed conversation
DB -> DB: S45 ensure principal rows / last_seen_at
BrowserState -> BrowserState: S46 replace local messages/title

Worker -> KV: S47 mark terminal event published
Worker -> JS: S48 final ACK TurnRequested
Worker -> KV: S49 purge CHAT_LOCKS key
WorkerState -> WorkerState: S50 remove pending ack handle
```

### Submit Failure Compensation

The API owns the handoff from HTTP to NATS. Once it creates the active lock
with the requested prompt payload, it must publish the wakeup command or
compensate before returning an error.

```d2
shape: sequence_diagram

API: api-service
DB: SurrealDB
KV: NATS KV CHAT_LOCKS
JS: NATS JetStream CHAT_COMMANDS

API -> KV: S09 create requested lock with prompt payload
command publish fails after lock creation: {
  API -> DB: S49 upsert failed chat_turn with publish error
  API -> KV: S50 purge CHAT_LOCKS key
  API -> API: return bus error
}
command publish succeeds: {
  API -> JS: S11 append TurnRequested wakeup
  API -> API: return 202 Accepted
}
```

### Worker Failure Before Commit

If the worker cannot obtain an LLM stream, loses the lock, or hits a stream
error before a terminal DB commit, the visible user and assistant messages are
not committed. The worker records a failed turn and sends realtime cleanup
events.

```d2
shape: sequence_diagram

Worker: chat-worker
DB: SurrealDB
Events: NATS JetStream CHAT_EVENTS
Replay: NATS KV CHAT_REPLAY
KV: NATS KV CHAT_LOCKS
JS: NATS JetStream CHAT_COMMANDS
RT: realtime-service
BrowserState: Browser local state

Worker -> DB: S52 upsert terminal chat_turn failed
Worker -> KV: S53 mark CHAT_LOCKS terminal failed
Worker -> Events: S54 append error event
Events -> RT: deliver error
RT -> BrowserState: S55 status=error, display message
Worker -> Events: S56 append clear event
Events -> Worker: PubAck sequence=end_seq
Worker -> Replay: S57 update current_turn end_seq
Events -> RT: deliver clear
RT -> BrowserState: S58 remove overlays, status=ready
Worker -> KV: S59 mark terminal event published
Worker -> JS: S60 final ACK TurnRequested
Worker -> KV: S61 purge CHAT_LOCKS key
```

## Active Turn State

```d2
direction: down

Start: "No CHAT_LOCKS key"
Idle
Requested
InFlightRejected: "In-flight rejected"
Running
Expired
Committed
Interrupted
Failed
StaleRunning: "Stale running"

Start -> Idle
Idle -> Requested: "API creates lock"
Requested -> InFlightRejected: "second POST"
InFlightRejected -> Requested: "original turn active"
Requested -> Running: "worker CAS claim"
Requested -> Expired: "API crash / no claim"
Running -> Committed: "DB commit"
Running -> Interrupted: "stop requested"
Running -> Failed: "worker error"
Running -> StaleRunning: "worker crash"
StaleRunning -> Failed: "redelivery recovery"
Committed -> Idle: "ACK then release"
Interrupted -> Idle: "ACK then release"
Failed -> Idle: "ACK then release or TTL"
Expired -> Idle: "KV TTL"
```

## Concurrent POST Race

Two users, tabs, or devices can submit against the same conversation at the
same time. The KV create is the serialization point.

```d2
shape: sequence_diagram

A: Request A
B: Request B
API: api-service
KV: NATS KV
JS: JetStream

A -> API: POST turn A
B -> API: POST turn B
API -> KV: C01 create requested lock for A
API -> KV: attempt requested lock for B
KV -> API: A wins
KV -> API: B conflict
API -> JS: C02 append TurnRequested A
API -> A: 202 Accepted
API -> B: 409 in_flight
```

Invariant: only one active turn per conversation can be published as accepted.

## Stop Flow

```d2
shape: sequence_diagram

Browser: Browser UI
BrowserState: Browser local state
API: api-service
DB: SurrealDB
KV: NATS KV CHAT_LOCKS
Control: NATS Core worker stop subject
Worker: chat-worker
Events: NATS JetStream CHAT_EVENTS
Replay: NATS KV CHAT_REPLAY
JS: NATS JetStream CHAT_COMMANDS
RT: realtime-service
RTState: realtime-service memory

Browser -> API: POST /api/chat/conversations/:id/stop
BrowserState -> BrowserState: T01 status=stopping
API -> DB: authorize visible conversation
DB -> DB: T02 ensure principal rows / last_seen_at
Existing lock is expired: {
  API -> KV: T03 purge expired CHAT_LOCKS key
}
API -> KV: T04 set stop_requested=true
lock has worker_id: {
  API -> Control: T05 publish worker stop
}
lock is requested: {
  API -> API: no wakeup; worker sees flag on claim
}
API -> Browser: 204
Control -> Worker: stop wakeup
Worker -> KV: verify stop_requested + active turn
Worker -> DB: commit interrupted transaction
DB -> DB: T06 ensure principal rows / last_seen_at
DB -> DB: T07 delete branch messages
DB -> DB: T08 create committed user message
DB -> DB: T09 create interrupted assistant message
DB -> DB: T10 update conversation metadata
DB -> DB: T11 update chat_turn interrupted
Worker -> Events: T12 append interrupted event
Events -> Worker: PubAck sequence=end_seq
Worker -> Replay: T13 update current_turn end_seq
Events -> RT: deliver interrupted
RT -> RTState: T14 update last_sent_sequences
RT -> Browser: WebSocket event interrupted
BrowserState -> BrowserState: T15 upsert interrupted assistant, status=ready
Worker -> KV: T16 purge CHAT_LOCKS key
Worker -> JS: T17 final ACK TurnRequested
```

The HTTP `204` only means the stop request was accepted. The browser learns
that streaming actually stopped from the `interrupted` realtime event.
