use std::{alloc::Layout, ptr::NonNull, sync::Arc};

use allocator_api2::alloc::{AllocError, Allocator};
use bumpalo::Bump;

use super::{Arena, ArenaGuard};

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
    fn guard(&self) -> ArenaGuard {
        ArenaGuard::new(self.bump.clone())
    }

    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.bump.as_ref().allocate(layout)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        self.bump.as_ref().deallocate(ptr, layout)
    }
}
