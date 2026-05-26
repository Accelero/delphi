# Chat System

The chat system is split into small services connected by NATS and SurrealDB.
The API handles authenticated commands, the worker owns LLM execution, and the
realtime service only relays authorized live events to browser tabs.

```mermaid
flowchart LR
  Browser["Browser\nReact + WebSocket"]
  API["api-service\nHTTP chat API"]
  RT["realtime-service\nWebSocket fanout"]
  Worker["chat-worker\nLLM execution"]
  DB[("SurrealDB\ncommitted chat state")]
  KV[("NATS KV\nCHAT_LOCKS\nCHAT_REPLAY")]
  Commands[["NATS JetStream\nCHAT_COMMANDS"]]
  Events[["NATS JetStream\nCHAT_EVENTS"]]
  LLM["LLM provider\nGemini via rig"]

  Browser -->|"POST /api/chat/.../turns"| API
  Browser <-->|"WS /ws/chat"| RT
  API -->|"auth/access checks"| DB
  API -->|"create requested lock"| KV
  API -->|"publish TurnRequested"| Commands
  Worker -->|"pull command"| Commands
  Worker -->|"claim/renew/release lock"| KV
  Worker -->|"load history + commit messages"| DB
  Worker <-->|"stream response"| LLM
  Worker -->|"publish turn events"| Events
  RT -->|"exact conversation consumer"| Events
  RT -->|"authorize subscribe"| DB
  RT -->|"send JSON events"| Browser
```

## Runtime Responsibilities

| Component | Owns | Does not own |
| --- | --- | --- |
| `api-service` | auth, access checks, command acceptance, active lock creation | LLM execution, WebSocket delivery |
| `chat-worker` | active turn execution, LLM stream, DB commit, terminal event | browser connections |
| `realtime-service` | WebSocket sessions, replay, local fanout | command execution, LLM calls |
| SurrealDB | committed conversation/message truth | transient active-turn coordination |
| NATS JetStream/KV | commands, live events, active locks, replay cursors | long-term chat history |

## Event Subjects

```text
chat.commands.turn_requested
chat.events.<tenant_id>.<conversation_id>
chat.control.<tenant_id>.<conversation_id>.stop
```

The realtime service subscribes to exact conversation event subjects only
after SurrealDB authorizes the user for that conversation.

```mermaid
flowchart TD
  TabA["Tab A subscribes to conversation C"]
  TabB["Tab B subscribes to conversation C"]
  Auth["SurrealDB access check"]
  Hub["Local conversation fanout hub\nkey: tenant/conversation"]
  Nats["NATS exact consumer\nchat.events.tenant.C"]

  TabA --> Auth
  TabB --> Auth
  Auth --> Hub
  Hub --> Nats
  Nats --> Hub
  Hub --> TabA
  Hub --> TabB
```

Multiple tabs on the same realtime replica share one local fanout hub and one
exact NATS consumer.
