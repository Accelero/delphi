use std::sync::Arc;

use crate::object_store::ObjectStore;

use super::SourceAdapter;

/// In-memory list of installed adapters. Built once at startup.
#[derive(Default)]
pub struct AdapterRegistry {
    adapters: Vec<Arc<dyn SourceAdapter>>,
}

impl AdapterRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an adapter if its `try_from_env` returned one. Silently
    /// drops `None` so adapters are opt-in by env configuration.
    pub fn try_register(&mut self, adapter: Option<Arc<dyn SourceAdapter>>) {
        if let Some(a) = adapter {
            tracing::info!(adapter = a.name(), "source adapter registered");
            self.adapters.push(a);
        }
    }

    pub fn register(&mut self, adapter: Arc<dyn SourceAdapter>) {
        tracing::info!(adapter = adapter.name(), "source adapter registered");
        self.adapters.push(adapter);
    }

    pub fn into_inner(self) -> Vec<Arc<dyn SourceAdapter>> {
        self.adapters
    }

    pub fn is_empty(&self) -> bool {
        self.adapters.is_empty()
    }
}

/// **The install seam for in-tree adapters.** To add a new bundled
/// adapter: implement `SourceAdapter`, give it a `try_from_env()`
/// constructor, and register it here.
///
/// Adapters that need to stash original artefacts (PDFs, etc.) take
/// the shared `ObjectStore` as a constructor dependency — that's why
/// it's an argument here.
pub fn default_registry(_object_store: Arc<dyn ObjectStore>) -> AdapterRegistry {
    let reg = AdapterRegistry::new();
    // future:
    // reg.try_register(SemanticScholarAdapter::try_from_env().map(|a| Arc::new(a) as _));
    // reg.try_register(PubmedAdapter::try_from_env().map(|a| Arc::new(a) as _));
    reg
}
