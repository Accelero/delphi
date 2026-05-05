//! Two of the three dev-mode safety layers:
//!
//! 1. (compile-time) the `dev-auth` cargo feature flag — gating
//!    `AuthMode::Dev` itself. Lives in `auth/config.rs`.
//! 2. (runtime) [`enforce_production_guard`] — refuses to start with
//!    `AUTH_MODE=dev` when `RUST_ENV=production`.
//! 3. (UX) [`print_banner`] — loud yellow banner on stderr in dev mode.

use anyhow::Result;

use crate::auth::config::AuthMode;

pub fn enforce_production_guard(mode: &AuthMode) -> Result<()> {
    let prod_marker = std::env::var("RUST_ENV").as_deref() == Ok("production");
    #[cfg(feature = "dev-auth")]
    if matches!(mode, AuthMode::Dev(_)) && prod_marker {
        anyhow::bail!(
            "REFUSING TO START: AUTH_MODE=dev with RUST_ENV=production. \
             This is a fatal misconfiguration."
        );
    }
    let _ = (mode, prod_marker);
    Ok(())
}

pub fn print_banner(mode: &AuthMode) {
    #[cfg(feature = "dev-auth")]
    if matches!(mode, AuthMode::Dev(_)) {
        eprintln!(
            "\n\x1b[33m\
             ╔════════════════════════════════════════════════════════════════╗\n\
             ║  WARNING: DELPHI RUNNING IN DEV AUTH MODE                       ║\n\
             ║  All requests are auto-authenticated as the dev user.           ║\n\
             ║  Do NOT use in production.                                      ║\n\
             ╚════════════════════════════════════════════════════════════════╝\
             \x1b[0m\n"
        );
    }
    let _ = mode;
}
