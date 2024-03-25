use std::{fmt::Debug, ptr::NonNull};

use super::alloc::Deallocation;

pub struct Bytes {
    ptr: NonNull<u8>,
    len: usize,
    deallocation: Deallocation,
}

impl Bytes {
    /// Takes ownership of an allocated memory region,
    ///
    /// # Arguments
    ///
    /// * `ptr` - Pointer to raw parts
    /// * `len` - Length of raw parts in **bytes**
    /// * `deallocation` - Type of allocation
    ///
    /// # Safety
    ///
    /// This function is unsafe as there is no guarantee that the given pointer is valid for `len`
    /// bytes. If the `ptr` and `capacity` come from a `Buffer`, then this is guaranteed.
    #[inline]
    pub unsafe fn new(ptr: NonNull<u8>, len: usize, deallocation: Deallocation) -> Bytes {
        Bytes {
            ptr,
            len,
            deallocation,
        }
    }

    fn as_slice(&self) -> &[u8] {
        self
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn ptr(&self) -> NonNull<u8> {
        self.ptr
    }

    pub fn capacity(&self) -> usize {
        match self.deallocation {
            Deallocation::Standard(layout) => layout.size(),
            // we only know the size of the custom allocation
            // its underlying capacity might be larger
            Deallocation::Custom(_, size) => size,
        }
    }
}

// Deallocation is Send + Sync, repeating the bound here makes that refactoring safe
// The only field that is not automatically Send+Sync then is the NonNull ptr
unsafe impl Send for Bytes where Deallocation: Send {}
unsafe impl Sync for Bytes where Deallocation: Sync {}

impl Drop for Bytes {
    #[inline]
    fn drop(&mut self) {
        match &self.deallocation {
            Deallocation::Standard(layout) => match layout.size() {
                0 => {} // Nothing to do
                _ => unsafe { std::alloc::dealloc(self.ptr.as_ptr(), *layout) },
            },
            // The automatic drop implementation will free the memory once the reference count reaches zero
            Deallocation::Custom(_allocation, _size) => (),
        }
    }
}

impl std::ops::Deref for Bytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl PartialEq for Bytes {
    fn eq(&self, other: &Bytes) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Debug for Bytes {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Bytes {{ ptr: {:?}, len: {}, data: ", self.ptr, self.len,)?;

        f.debug_list().entries(self.iter()).finish()?;

        write!(f, " }}")
    }
}
