//! Database admin commands: status / wipe.
//!
//! Schema bootstrap is no longer here — `api::serve()` applies the schema
//! itself on startup. These remaining commands are diagnostic / destructive
//! ops that are run rarely and out of band.

use anyhow::Result;

use crate::config::storage_from_env;
use crate::AdminCmd;

pub async fn run(cmd: AdminCmd) -> Result<()> {
    let storage = storage_from_env().await?;
    match cmd {
        AdminCmd::Status => {
            let c = storage.counts().await?;
            println!("document          {}", c.documents);
            println!("document_content  {}", c.document_content);
            println!("chunk             {}", c.chunks);
            println!("document_version  {}", c.document_versions);
        }
        AdminCmd::Wipe => {
            storage.wipe().await?;
            println!("wiped");
        }
    }
    Ok(())
}
