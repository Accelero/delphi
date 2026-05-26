//! Database admin commands: status / wipe.
//!
//! Schema bootstrap is no longer here — `api::serve()` applies the schema
//! itself on startup. These remaining commands are diagnostic / destructive
//! ops that are run rarely and out of band.
//!
//! Runs against the privileged [`SystemDb`] handle (above-RBAC). Wipe
//! and counts default to **all tenants** — same scope as the historical
//! behaviour before tenancy landed. Tenant scoping (`--tenant=<slug>`)
//! is a follow-up.

use anyhow::Result;

use crate::config::system_db_from_env;
use crate::AdminCmd;

pub async fn run(cmd: AdminCmd) -> Result<()> {
    let system = system_db_from_env().await?;
    match cmd {
        AdminCmd::Status => {
            let c = system.counts(None).await?;
            println!("document          {}", c.documents);
            println!("document_content  {}", c.document_content);
            println!("chunk             {}", c.chunks);
            println!("document_version  {}", c.document_versions);
        }
        AdminCmd::Wipe => {
            system.wipe(None).await?;
            println!("wiped");
        }
    }
    Ok(())
}
