//! Chat-streaming primitives (v3).
//!
//! Multi-tab, SSE-based fan-out (see
//! [`docs/architecture/chat.md`](../../../docs/architecture/chat.md)):
//! POST `/messages` is fire-and-forget, GET `/stream` is the single
//! source of truth, and per-conversation [`SessionState`] coordinates
//! the worker, the buffered frames, and the live subscriber list.
//!
//! ### Layout
//!
//! ```text
//! chat/
//! ├── mod.rs       public interface (this file)
//! ├── registry.rs  SessionRegistry — ConversationId → Arc<SessionState>
//! ├── session.rs   SessionState — buffer + subscribers + phase
//! └── worker.rs    spawn_worker — detached per-turn future
//! ```
//!
//! `api/*` consumes this module via `crate::chat::*`; `chat::` itself
//! imports `crate::api::sse::*` (sibling, allowed) for the SSE frame
//! writers.

mod registry;
mod session;
mod worker;

pub use registry::{SessionRegistry, TaskId};
pub use session::{AlreadyRunning, SessionState, TurnPhase};
pub use worker::{spawn_worker, turn_request, TurnRequest};
