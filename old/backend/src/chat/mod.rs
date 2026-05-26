//! Chat-streaming primitives (v4 — server-authoritative replication).
//!
//! Multi-tab, SSE-based fan-out (see
//! [`docs/architecture/chat-v4.md`](../../../docs/architecture/chat-v4.md)):
//! POST `/messages` is fire-and-forget, GET `/stream` is the single
//! source of truth, and a per-conversation [`TurnBus`] coordinates the
//! single-flight slot, the ordered delta log (replay + live), and cancel
//! delivery — all behind one trait so the in-memory impl can be swapped
//! for a NATS-backed one without touching the worker, handlers, or wire.
//!
//! ### Layout
//!
//! ```text
//! chat/
//! ├── mod.rs        public interface (this file)
//! ├── bus.rs        TurnBus trait + TurnHandle + Cursor + AlreadyRunning + TaskId
//! ├── inprocess.rs  InProcessBus — DashMap<ConversationId, Session>, the §7 reader
//! └── worker.rs     spawn_worker — detached single-writer per-turn future
//! ```
//!
//! `api/*` consumes this module via `crate::chat::*`; `chat::` itself
//! imports `crate::api::sse::*` (sibling, allowed) for the SSE frame
//! writers.

mod bus;
mod inprocess;
mod worker;

pub use bus::{AlreadyRunning, Cursor, TaskId, TurnBus, TurnHandle};
pub use inprocess::InProcessBus;
pub use worker::{spawn_worker, turn_request, TurnRequest};
