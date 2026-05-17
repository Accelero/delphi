//! Chat-streaming primitives.
//!
//! Post-redesign (see
//! [`docs/architecture/chat-streaming.md`](../../../docs/architecture/chat-streaming.md)):
//! one POST returns the stream body itself; the spawned worker is the
//! whole model. There is no buffer, no multi-reader fanout, no
//! re-attach mechanism.
//!
//! ### Layout
//!
//! ```text
//! chat/
//! ├── mod.rs       public interface (this file)
//! ├── registry.rs  TaskRegistry — TaskId → CancellationToken
//! └── worker.rs    spawn_worker — detached per-turn future
//! ```
//!
//! `api/*` consumes this module via `crate::chat::*`; `chat::` itself
//! imports `crate::api::stream::*` (sibling, allowed) for the framed
//! protocol writers.

mod registry;
mod worker;

pub use registry::{TaskId, TaskRegistry};
pub use worker::{spawn_worker, turn_request, TurnRequest};
