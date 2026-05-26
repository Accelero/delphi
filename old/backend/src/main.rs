//! Delphi backend entry point.
//!
//! Single binary with subcommands:
//!     delphi serve              # axum HTTP server (API + SPA)
//!     delphi admin status       # row counts
//!     delphi admin wipe         # delete all data, keep schema
//!
//! Schema is applied automatically on every `serve` startup (idempotent),
//! so there's no separate init step.

use anyhow::Context;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use delphi::{admin, api, AdminCmd};

#[derive(Parser)]
#[command(name = "delphi", version, about = "Delphi research-tool backend")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the HTTP server (API + static SPA)
    Serve(ServeArgs),
    /// Database administration commands
    #[command(subcommand)]
    Admin(AdminCmd),
}

#[derive(clap::Args)]
struct ServeArgs {
    /// Address to bind, e.g. 0.0.0.0:8081
    #[arg(long, env = "DELPHI_SERVER_BIND_ADDR", default_value = "0.0.0.0:8081")]
    bind: String,

    /// Path to the built frontend (Vite `dist/`). Optional in dev.
    #[arg(long, env = "STATIC_DIR")]
    static_dir: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,delphi=debug")),
        )
        .init();

    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Serve(args) => api::serve(args.bind, args.static_dir)
            .await
            .context("running http server"),
        Cmd::Admin(cmd) => admin::run(cmd).await.context("running admin command"),
    }
}
