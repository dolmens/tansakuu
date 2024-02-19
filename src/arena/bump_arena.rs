use std::{alloc::Layout, ptr::NonNull, sync::Arc};

use allocator_api2::alloc::{AllocError, Allocator};
use bumpalo::Bump;

use super::ArenaGuard;

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

    pub fn arena_guard(&self) -> ArenaGuard {
        ArenaGuard::new(self.bump.clone())
    }

    pub fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.bump.as_ref().allocate(layout)
    }

    pub unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        self.bump.as_ref().deallocate(ptr, layout)
    }
}
