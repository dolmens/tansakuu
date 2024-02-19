use std::{any::Any, sync::Arc};

#[derive(Clone)]
pub struct ArenaGuard {
    _arena: Arc<dyn Any>,
}

unsafe impl Send for ArenaGuard {}
unsafe impl Sync for ArenaGuard {}

impl ArenaGuard {
    pub fn new(arena: Arc<dyn Any>) -> Self {
        Self { _arena: arena }
    }
}
