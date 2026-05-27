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
