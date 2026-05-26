<section class="home-hero">
  <div class="hero-copy">
    <p class="hero-label">Delphi Microservice Migration</p>
    <h1>Architecture docs for the document RAG SaaS rewrite.</h1>
    <p class="hero-text">
      A compact system guide for chat, realtime streaming, NATS coordination,
      crash behavior, and the next migration decisions.
    </p>
    <div class="hero-actions">
      <a class="primary-action" href="architecture/chat-system/">Open Chat Architecture</a>
      <a class="secondary-action" href="architecture/chat-failure-analysis/">Review Failure Modes</a>
    </div>
  </div>
  <div class="hero-panel" aria-label="Current architecture focus">
    <div class="panel-header">
      <span class="status-dot"></span>
      <span>Current Slice</span>
    </div>
    <div class="signal-grid">
      <div>
        <strong>API</strong>
        <span>auth, access, command acceptance</span>
      </div>
      <div>
        <strong>NATS</strong>
        <span>commands, events, active locks</span>
      </div>
      <div>
        <strong>Worker</strong>
        <span>LLM stream and commit</span>
      </div>
      <div>
        <strong>Realtime</strong>
        <span>authorized WebSocket fanout</span>
      </div>
    </div>
  </div>
</section>

## Start Here

<div class="doc-card-grid">
  <a class="doc-card" href="architecture/chat-system/">
    <span class="card-kicker">System</span>
    <strong>Chat Architecture</strong>
    <p>Service boundaries, NATS subjects, state ownership, and realtime fanout.</p>
  </a>
  <a class="doc-card" href="architecture/chat-request-flow/">
    <span class="card-kicker">Flow</span>
    <strong>Request Walkthrough</strong>
    <p>Happy path, concurrent POST race, active-turn state machine, and stop flow.</p>
  </a>
  <a class="doc-card" href="architecture/chat-failure-analysis/">
    <span class="card-kicker">Reliability</span>
    <strong>Crash Analysis</strong>
    <p>API and worker crash matrices, redelivery decisions, and invariants.</p>
  </a>
  <a class="doc-card" href="architecture/alteration/">
    <span class="card-kicker">Review</span>
    <strong>Alteration Notes</strong>
    <p>Pending workflow decisions to apply after the current walkthrough.</p>
  </a>
</div>

## Operating Model

<div class="principle-strip">
  <div>
    <strong>SurrealDB</strong>
    <span>committed conversation truth</span>
  </div>
  <div>
    <strong>NATS JetStream</strong>
    <span>durable commands and live events</span>
  </div>
  <div>
    <strong>NATS KV</strong>
    <span>active lock and replay coordination</span>
  </div>
  <div>
    <strong>WebSocket</strong>
    <span>authorized realtime delivery</span>
  </div>
</div>

## Local Preview

```bash
make docs-serve
```

The local server uses `uv` and serves the site from `http://127.0.0.1:8000`
unless another address is passed directly to MkDocs.
