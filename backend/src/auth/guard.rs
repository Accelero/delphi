//! Three runtime safety layers around production deployment:
//!
//! 1. (compile-time) the `dev-auth` cargo feature flag — gating
//!    `AuthMode::Dev` itself. Lives in `auth/config.rs`.
//! 2. (runtime) [`enforce_production_guard`] — refuses to start when
//!    `RUST_ENV=production` is paired with any of:
//!      - `AUTH_MODE=dev` (dev-auth bypass)
//!      - `SURREAL_SERVICE_USER` unset / equal to `root` (default
//!        credential leak)
//!      - `SURREAL_SERVICE_PASS` unset / equal to `root`
//! 3. (UX) [`print_banner`] — loud yellow banner on stderr in dev mode.
//!
//! Closes audit findings C3 (in part) and C4.

use anyhow::Result;

use super::config::AuthMode;

pub fn enforce_production_guard(mode: &AuthMode) -> Result<()> {
    let prod_marker = std::env::var("RUST_ENV").as_deref() == Ok("production");
    if !prod_marker {
        // Dev / staging: nothing to enforce.
        let _ = mode;
        return Ok(());
    }

    // Production: refuse the dev-auth bypass.
    #[cfg(feature = "dev-auth")]
    if matches!(mode, AuthMode::Dev(_)) {
        anyhow::bail!(
            "REFUSING TO START: AUTH_MODE=dev with RUST_ENV=production. \
             This is a fatal misconfiguration."
        );
    }

    // Production: refuse default Surreal credentials.
    let service_user = std::env::var("SURREAL_SERVICE_USER").unwrap_or_default();
    let service_pass = std::env::var("SURREAL_SERVICE_PASS").unwrap_or_default();
    if service_user.is_empty() || service_user == "root" {
        anyhow::bail!(
            "REFUSING TO START: SURREAL_SERVICE_USER is unset or equals the default \
             'root' under RUST_ENV=production. Provision a dedicated DB EDITOR-role \
             user and set SURREAL_SERVICE_USER / SURREAL_SERVICE_PASS to its credentials."
        );
    }
    if service_pass.is_empty() || service_pass == "root" {
        anyhow::bail!(
            "REFUSING TO START: SURREAL_SERVICE_PASS is unset or equals the default \
             'root' under RUST_ENV=production. Set a real password."
        );
    }

    let _ = mode;
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
