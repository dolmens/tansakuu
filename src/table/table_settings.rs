use std::sync::Arc;

pub struct TableSettings {}

impl TableSettings {
    pub fn new() -> Self {
        Self {}
    }
}

pub type TableSettingsRef = Arc<TableSettings>;
