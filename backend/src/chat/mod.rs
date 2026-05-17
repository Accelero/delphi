//! Per-session chat-streaming primitives.
//!
//! Decouples the LLM call from the HTTP request that submitted it. The
//! POST handler persists the user message and spawns a worker; the
//! worker streams into a shared in-memory buffer; one or more SSE
//! readers tail that buffer. The submitting tab can close without
//! killing the turn, and a tab opened mid-turn replays the buffer's
//! current contents before tailing live deltas.
//!
//! See [`docs/architecture/chat-streaming.md`](../../../docs/architecture/chat-streaming.md)
//! for the design — this module is the public face of it.
//!
//! ### Layout
//!
//! ```text
//! chat/
//! ├── mod.rs       public interface (this file)
//! ├── state.rs     SessionState — buffer + notify + locks
//! ├── reader.rs    SessionReader — impl AsyncRead over the buffer
//! └── registry.rs  SessionRegistry — Weak-map of live sessions
//! ```
//!
//! `api/*` consumes this module via `crate::chat::*`; `chat::` itself
//! imports `crate::api::stream::*` (sibling, allowed) for the framed
//! protocol writers.

mod reader;
mod registry;
mod state;
mod worker;

pub use reader::SessionReader;
pub use registry::SessionRegistry;
pub use state::SessionState;
pub use worker::{spawn as spawn_worker, turn_request, TurnRequest};
