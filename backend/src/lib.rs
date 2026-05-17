//! Delphi backend library.
//!
//! Exists alongside the bin (`src/main.rs`) so integration tests in
//! `backend/tests/` can call into crate internals through their public
//! interface. Module privacy follows the rules in `.claude/CLAUDE.md` —
//! cross-module access only through the `mod.rs` public surface, which
//! is what gets re-exported here.

pub mod admin;
pub mod api;
pub mod auth;
pub mod chat;
pub mod chunker;
pub mod config;
pub mod embedder;
pub mod error;
pub mod filter;
pub mod ingestion;
pub mod llm;
pub mod object_store;
pub mod sources;
pub mod state;
pub mod storage;
pub mod text_extractor;

// Subcommands the CLI binary surfaces. Defined here so the bin and any
// integration-test process can both reach the type without one depending
// on the other.
#[derive(clap::Subcommand)]
pub enum AdminCmd {
    /// Show row counts per table
    Status,
    /// Delete all data, keep schema
    Wipe,
}
