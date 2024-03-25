use std::{alloc::Layout, ptr::NonNull, sync::Arc};

use num::Num;

use super::{alloc::Deallocation, bytes::Bytes};

#[derive(Clone, Debug)]
pub struct Buffer {
    data: Arc<Bytes>,
    ptr: *const u8,
    length: usize,
}

impl PartialEq for Buffer {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice().eq(other.as_slice())
    }
}

impl Eq for Buffer {}

unsafe impl Send for Buffer where Bytes: Send {}
unsafe impl Sync for Buffer where Bytes: Sync {}

impl Buffer {
    /// Auxiliary method to create a new Buffer
    #[inline]
    pub fn from_bytes(bytes: Bytes) -> Self {
        let length = bytes.len();
        let ptr = bytes.as_ptr();
        Buffer {
            data: Arc::new(bytes),
            ptr,
            length,
        }
    }

    /// Create a [`Buffer`] from the provided [`Vec`] without copying
    #[inline]
    pub fn from_vec<T: Num>(vec: Vec<T>) -> Self {
        let bytes = unsafe {
            let data = NonNull::new_unchecked(vec.as_ptr() as _);
            let len = vec.len() * std::mem::size_of::<T>();
            let layout = Layout::array::<T>(vec.capacity()).unwrap_unchecked();
            let deallocation = Deallocation::Standard(layout);
            Bytes::new(data, len, deallocation)
        };
        std::mem::forget(vec);
        Self::from_bytes(bytes)
    }

    /// Returns the number of bytes in the buffer
    #[inline]
    pub fn len(&self) -> usize {
        self.length
    }

    /// Returns the capacity of this buffer.
    /// For externally owned buffers, this returns zero
    #[inline]
    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    /// Returns whether the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Returns the byte slice stored in this buffer
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.length) }
    }

    /// Returns a pointer to the start of this buffer.
    ///
    /// Note that this should be used cautiously, and the returned pointer should not be
    /// stored anywhere, to avoid dangling pointers.
    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    /// View buffer as a slice of a specific type.
    ///
    /// # Panics
    ///
    /// This function panics if the underlying buffer is not aligned
    /// correctly for type `T`.
    pub fn typed_data<T: Num>(&self) -> &[T] {
        // SAFETY
        // ArrowNativeType is trivially transmutable, is sealed to prevent potentially incorrect
        // implementation outside this crate, and this method checks alignment
        let (prefix, offsets, suffix) = unsafe { self.as_slice().align_to::<T>() };
        assert!(prefix.is_empty() && suffix.is_empty());
        offsets
    }
}
