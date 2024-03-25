use std::{panic::RefUnwindSafe, ptr::NonNull, sync::Arc};

use tantivy_common::OwnedBytes;

use super::{alloc::Deallocation, bytes::Bytes};

struct OwnedBytesRefUnwindSafe {
    owned_bytes: OwnedBytes,
}

impl RefUnwindSafe for OwnedBytesRefUnwindSafe {}

impl AsRef<OwnedBytes> for OwnedBytesRefUnwindSafe {
    #[inline]
    fn as_ref(&self) -> &OwnedBytes {
        &self.owned_bytes
    }
}

impl From<OwnedBytes> for OwnedBytesRefUnwindSafe {
    fn from(owned_bytes: OwnedBytes) -> Self {
        Self { owned_bytes }
    }
}

impl From<OwnedBytes> for Bytes {
    fn from(owned_bytes: OwnedBytes) -> Self {
        let owned_bytes = OwnedBytesRefUnwindSafe::from(owned_bytes);
        let ptr = owned_bytes.as_ref().as_slice().as_ptr() as *mut _;
        let ptr = unsafe { NonNull::new_unchecked(ptr) };
        let len = owned_bytes.as_ref().len();
        let deallocation = Deallocation::Custom(Arc::new(owned_bytes), len);
        unsafe { Self::new(ptr, len, deallocation) }
    }
}
