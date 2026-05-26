# Chat Request Flow

This page describes what happens when a user submits a chat message.

## Happy Path

```mermaid
sequenceDiagram
  autonumber
  participant Browser
  participant API as api-service
  participant DB as SurrealDB
  participant KV as NATS KV CHAT_LOCKS
  participant JS as NATS JetStream CHAT_COMMANDS
  participant Worker as chat-worker
  participant LLM as LLM provider
  participant Events as NATS JetStream CHAT_EVENTS
  participant RT as realtime-service

  Browser->>API: POST /api/chat/conversations/:id/turns
  API->>API: validate JWT
  API->>DB: authorize conversation access
  API->>DB: check parent is committed tail
  API->>KV: create requested lock for tenant/conversation
  alt lock already exists
    API-->>Browser: 409 in_flight
  else lock created
    API->>JS: publish TurnRequested
    JS-->>API: PubAck
    API-->>Browser: 202 Accepted
  end

  Worker->>JS: pull TurnRequested
  Worker->>KV: CAS requested -> running
  Worker->>Events: publish turn_started + user_message
  Worker->>DB: load committed history
  Worker->>LLM: start stream

  loop provider chunks
    LLM-->>Worker: text chunk
    Worker->>Events: publish text_delta
    RT->>Events: consume conversation events
    RT-->>Browser: WebSocket event
    Worker->>KV: renew lease / check stop
    Worker->>JS: progress ACK
  end

  Worker->>DB: commit user + assistant messages
  Worker->>Events: publish finish
  RT-->>Browser: finish
  Worker->>KV: release lock
  Worker->>JS: final ACK
```

## Active Turn State

```mermaid
stateDiagram-v2
  [*] --> Idle: no CHAT_LOCKS key
  Idle --> Requested: API creates lock
  Requested --> InFlightRejected: second POST hits same conversation
  InFlightRejected --> Requested: original turn still active
  Requested --> Running: worker CAS claim
  Requested --> Expired: API crash before publish / no worker claim
  Running --> Committed: DB commit succeeded
  Running --> Interrupted: stop requested + partial commit
  Running --> Failed: worker error or stale running recovery
  Running --> StaleRunning: worker crash / missed progress
  StaleRunning --> Failed: redelivery recovery
  Committed --> Idle: release lock
  Interrupted --> Idle: release lock
  Failed --> Idle: release lock or TTL
  Expired --> Idle: KV TTL
```

## Concurrent POST Race

Two users, tabs, or devices can submit against the same conversation at the
same time. The KV create is the serialization point.

```mermaid
sequenceDiagram
  autonumber
  participant A as Request A
  participant B as Request B
  participant API as api-service
  participant KV as NATS KV
  participant JS as JetStream

  A->>API: POST turn A
  B->>API: POST turn B
  API->>KV: create lock tenant/conversation for A
  API->>KV: create lock tenant/conversation for B
  KV-->>API: A wins
  KV-->>API: B conflict
  API->>JS: publish TurnRequested A
  API-->>A: 202 Accepted
  API-->>B: 409 in_flight
```

Invariant: only one active turn per conversation can be published as accepted.

## Stop Flow

```mermaid
sequenceDiagram
  autonumber
  participant Browser
  participant API as api-service
  participant DB as SurrealDB
  participant KV as NATS KV CHAT_LOCKS
  participant Control as NATS Core control subject
  participant Worker as chat-worker
  participant Events as CHAT_EVENTS
  participant RT as realtime-service

  Browser->>API: POST /api/chat/conversations/:id/stop
  API->>DB: authorize conversation access
  API->>KV: CAS stop_requested=true
  alt lock is running
    API->>Control: publish chat.control.tenant.conversation.stop
  else lock is requested
    API->>API: no wakeup; worker sees flag on claim
  end
  API-->>Browser: 204
  Control-->>Worker: stop wakeup
  Worker->>KV: verify stop_requested + active turn
  Worker->>Events: publish interrupted
  Worker->>DB: commit user + partial assistant interrupted
  Worker->>KV: release lock
  RT-->>Browser: interrupted event
```

The HTTP `204` only means the stop request was accepted. The browser learns
that streaming actually stopped from the `interrupted` realtime event.
