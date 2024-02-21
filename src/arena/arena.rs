use std::{alloc::Layout, ptr::NonNull};

use allocator_api2::alloc::AllocError;

use super::ArenaGuard;

pub trait Arena {
    fn guard(&self) -> ArenaGuard;

    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError>;

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout);
}
