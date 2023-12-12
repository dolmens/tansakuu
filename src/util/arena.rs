use std::{alloc::Layout, ptr::NonNull, sync::Arc};

use bumpalo::Bump;

pub trait ArenaGuard: Send + Sync {}

pub trait Arena {
    fn guard(&self) -> Arc<dyn ArenaGuard>;
    fn alloc_layout(&self, layout: Layout) -> NonNull<u8>;
}

#[derive(Clone)]
pub struct BumpArena {
    bump: Arc<Bump>,
}

impl BumpArena {
    pub fn new() -> Self {
        Self {
            bump: Arc::new(Bump::new()),
        }
    }
}

impl Arena for BumpArena {
    fn guard(&self) -> Arc<dyn ArenaGuard> {
        Arc::new(BumpArenaGuard {
            bump: self.bump.clone(),
        })
    }

    fn alloc_layout(&self, layout: Layout) -> NonNull<u8> {
        self.bump.alloc_layout(layout)
    }
}

pub struct BumpArenaGuard {
    bump: Arc<Bump>,
}

impl ArenaGuard for BumpArenaGuard {}
unsafe impl Send for BumpArenaGuard {}
unsafe impl Sync for BumpArenaGuard {}
