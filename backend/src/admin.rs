//! Database admin commands: init / status / wipe.

use anyhow::Result;

use crate::AdminCmd;
use crate::config::storage_from_env;

pub async fn run(cmd: AdminCmd) -> Result<()> {
    let storage = storage_from_env().await?;
    match cmd {
        AdminCmd::Init => {
            storage.init_schema().await?;
            println!("schema applied");
        }
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
