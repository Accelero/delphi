---
title: Chat System
description: Service boundaries, state ownership, and realtime fanout for Delphi chat.
---

# Chat System

The chat system is split into small services connected by NATS and SurrealDB.
The API handles authenticated commands, the worker owns LLM execution, and the
realtime service only relays authorized live events to browser tabs.

```d2
direction: right

Browser: "Browser\nReact + WebSocket"
API: "api-service\nHTTP chat API"
RT: "realtime-service\nWebSocket fanout"
Worker: "chat-worker\nLLM execution"
DB: {
  label: "SurrealDB\ncommitted chat state"
  shape: cylinder
}
KV: {
  label: "NATS KV\nCHAT_LOCKS\nCHAT_REPLAY"
  shape: cylinder
}
Commands: "NATS JetStream\nCHAT_COMMANDS"
Events: "NATS JetStream\nCHAT_EVENTS"
LLM: "LLM provider\nGemini via rig"

Browser -> API: "POST /api/chat/.../turns"
Browser <-> RT: "WS /ws/chat"
API -> DB: "auth/access checks"
API -> KV: "create requested lock + prompt payload"
API -> Commands: "publish TurnRequested wakeup"
Worker -> Commands: "pull command"
Worker -> KV: "claim/read payload/renew/release lock"
Worker -> DB: "load history + commit messages"
Worker <-> LLM: "stream response"
Worker -> Events: "publish turn events"
RT -> Events: "exact conversation consumer"
RT -> DB: "authorize subscribe"
RT -> Browser: "send JSON events"
```

## Turn Lifecycle

The turn lifecycle is a saga. SurrealDB is the canonical committed chat history,
while NATS KV and JetStream hold transient turn ownership, replay cursors, and
live events. The numbers below are the crash-analysis checkpoints.

```d2
shape: sequence_diagram

Browser: Browser UI
BrowserState: Browser local state
API: api-service
DB: SurrealDB
Locks: NATS KV CHAT_LOCKS
Commands: JetStream CHAT_COMMANDS
Worker: chat-worker
LLM: LLM provider
Events: JetStream CHAT_EVENTS
Replay: NATS KV CHAT_REPLAY
RT: realtime-service

Existing or new realtime subscription: {
  Browser -> RT: 0 WS /ws/chat subscribe_conversation(last_event_id)
  RT -> DB: 0 authorize visible conversation
  RT -> Events: 0 create exact-subject consumer
  RT -> Browser: 0 subscribed / replay / resync_required
}

Browser -> BrowserState: 1 create user_message_id + turn_id, status=submitted
Browser -> API: 2 POST /api/chat/conversations/:id/turns
API -> API: 3 validate JWT, ULIDs, and non-empty text
API -> DB: 3 assert_parent_tail(conversation_id,parent_message_id)
API -> Locks: 4 create requested lock with prompt payload
API -> Commands: 5 append TurnRequested(message_id=turn_id)
Commands -> API: 5 PubAck
API -> Browser: 6 HTTP 202 Accepted

Commands -> Worker: 7 deliver TurnRequested, explicit ACK pending
Worker -> Locks: 8 CAS requested -> running, set worker_id + lease
Turn maintenance loop: {
  Worker -> Commands: 9 progress ACK command
  Worker -> Locks: 9 renew worker lease
}
Worker -> DB: 10 load committed conversation history

Worker -> Events: 11 append turn_started
Events -> Replay: 11 set current_turn.start_seq
Events -> RT: 12 deliver turn_started
RT -> Browser: 12 WebSocket event
Browser -> BrowserState: 12 status=submitted

Worker -> Events: 11 append user_message
Events -> RT: 12 deliver user_message
RT -> Browser: 12 WebSocket event
Browser -> BrowserState: 12 append visible user, status=streaming

Worker -> LLM: 13 stream_chat(history + prompt)
For each provider text chunk: {
  LLM -> Worker: 14 text delta
  Worker -> Worker: 14 append to assistant_text
  Worker -> Events: 14 append text_delta
  Events -> RT: 14 deliver text_delta
  RT -> Browser: 14 WebSocket event
  Browser -> BrowserState: 14 update assistant-live overlay
}

LLM -> Worker: 15 stream ends
Worker -> DB: 16 transaction commit user + assistant + chat_turn
Worker -> Locks: 17 mark terminal committed/interrupted/failed
Worker -> Events: 18 append finish/interrupted or error+clear
Events -> Replay: 18 set current_turn.end_seq
Events -> RT: 18 deliver terminal event
RT -> Browser: 21 WebSocket terminal event
Browser -> BrowserState: 21 status=ready, refresh conversation
Worker -> Locks: 19 terminal_event_published=true
Worker -> Commands: 20 final ACK TurnRequested
Worker -> Locks: 20 release lock
```

## Turn State Machine

```d2
direction: right

None: "No active KV lock"
Requested: "requested\nprompt payload stored"
Running: "running\nworker_id + renewable lease"
Stop: "stop_requested = true"
Committed: "committed\nassistant_message_id"
Interrupted: "interrupted\nassistant_message_id + partial content"
Failed: "failed\nterminal_error"
Published: "terminal_event_published = true"
Released: "lock purged"

None -> Requested: "API acquire_lock"
Requested -> Running: "worker claim_lock"
Running -> Running: "renew_lock + progress ACK"
Running -> Stop: "POST /stop mutates KV"
Stop -> Interrupted: "worker commits partial turn"
Running -> Committed: "DB commit succeeded"
Running -> Failed: "LLM/commit/lease failure"
Committed -> Published: "finish event published"
Interrupted -> Published: "interrupted event published"
Failed -> Published: "error + clear published"
Published -> Released: "command ACK, release_lock"
```

## Crash Boundary Matrix

This matrix uses the lifecycle numbers above. It describes the current
implementation, including recovery-sensitive gaps that should be reviewed
before relying on the flow as fully self-healing.

| Boundary | Durable state at crash | Current recovery behavior | User-visible result | Invariant or risk |
| --- | --- | --- | --- | --- |
| 1-2 Browser crashes before POST reaches API | none | user resubmits manually | no server-side turn | no side effect before API receives request |
| 3 API crashes during auth/validation | none | client retry is safe | timeout/error | no lock before auth and parent-tail validation |
| 4 API crashes after requested lock, before command publish | `CHAT_LOCKS` has `requested` prompt payload only | no worker is woken; lock blocks new turns until KV TTL/lease expiry | temporary in-flight conversation | requested lock must expire or be reconciled |
| 5 API crashes after command PubAck, before HTTP 202 | requested lock + durable `CHAT_COMMANDS` command | worker processes command; browser may reconnect/resync | client may see timeout but turn can complete | PubAck makes command durable |
| 7 Worker crashes after command delivery, before claim | command unacked + requested lock | JetStream redelivers | no live events yet | command must not be ACKed before ownership |
| 8 Worker crashes after claim, before first live event | command unacked + running lock with worker lease | redelivery sees fresh running lock until lease expiry, then marks failed | request may stall then clear/error | stale running turns do not regenerate silently |
| 11 Worker crashes after `turn_started`/`user_message` | running lock + start/user live events + replay start | stale lease path marks failed and publishes cleanup | optimistic user/live state is cleared or resynced | live events are not committed truth |
| 14 Worker crashes during LLM delta stream | running lock + partial `text_delta` events | stale lease path marks failed and publishes cleanup | stream stops, then error/clear or resync | partial deltas must not become DB messages |
| 16 Worker crashes after DB commit, before KV terminal marker | SurrealDB has committed messages, but KV still says running | current stale-lease recovery can mark the turn failed because it does not first reconcile DB by `turn_id` | reload can show committed messages, while live path may emit error/clear | known recovery gap: DB commit should be made discoverable before stale-running failure |
| 17 Worker crashes after KV terminal marker, before terminal event | DB committed + KV terminal marker with assistant id | redelivery publishes missing terminal event, ACKs, releases | finish/interrupted may arrive late; reload is correct | terminal marker makes terminal event replayable |
| 18 Worker crashes after terminal event, before `terminal_event_published` | DB committed + terminal event in `CHAT_EVENTS`; KV terminal flag false | redelivery may republish the terminal event, then marks published | duplicate terminal event is possible but idempotent by message id | browser and replay must tolerate duplicate terminal events |
| 19 Worker crashes after terminal flag, before command ACK | DB committed + terminal event published + command unacked | redelivery sees terminal flag, ACKs and releases | normal or late completion | terminal flag prevents rerun |
| 20 Worker crashes after command ACK, before lock release | command ACKed + terminal KV lock still present | no redelivery remains; lock blocks new turns until KV TTL | completed turn visible, but conversation may be temporarily in-flight | known release gap: ACK-before-release trades no rerun for possible lock TTL wait |
| 0 Realtime service crashes during stream | JetStream events + replay index continue; browser WS drops | browser reconnects with `last_event_id`; service reauthorizes and replays range or requests resync | short disconnect, then replay or full refresh | realtime owns no committed state |
| Any point after DB commit, browser reloads | SurrealDB committed messages | API `GET conversation` returns authoritative history | committed answer visible even if live terminal event was missed | DB is canonical chat truth |

## Runtime Responsibilities

| Component | Owns | Does not own |
| --- | --- | --- |
| `api-service` | auth, access checks, command acceptance, active lock and prompt payload creation | LLM execution, WebSocket delivery |
| `chat-worker` | active turn execution, LLM stream, DB commit, terminal event | browser connections |
| `realtime-service` | WebSocket sessions, replay, local fanout | command execution, LLM calls |
| SurrealDB | committed conversation/message truth | transient active-turn coordination |
| NATS JetStream/KV | commands, live events, active locks, replay cursors | long-term chat history |

## Event Subjects

```text
chat.commands.turn_requested
chat.events.<tenant_id>.<conversation_id>
chat.control.worker.<worker_id>.stop
```

The realtime service subscribes to exact conversation event subjects only
after SurrealDB authorizes the user for that conversation.

```d2
direction: down

TabA: "Tab A subscribes to conversation C"
TabB: "Tab B subscribes to conversation C"
Auth: "SurrealDB access check"
Hub: "Local conversation fanout hub\nkey: tenant/conversation"
Nats: "NATS exact consumer\nchat.events.tenant.C"

TabA -> Auth
TabB -> Auth
Auth -> Hub
Hub -> Nats
Nats -> Hub
Hub -> TabA
Hub -> TabB
```

Multiple tabs on the same realtime replica share one local fanout hub and one
exact NATS consumer.
