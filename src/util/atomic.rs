//! A collection of specialized atomics.
//!
//! In theory, it is perfectly possible to use a mixed of Ordering on the same instance of an Atomic, depending on the
//! situation.
//!
//! In practice, it is the author's experience that this is a rarely needed capability which only makes
//! auditing/reviewing harder.
//!
//! Thus, these little types come with pre-established memory ordering.

use std::{
    ptr::NonNull,
    sync::atomic::{AtomicPtr, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering},
};

macro_rules! atomic {
    ($name:ident, $underlying:ident, $raw:ident, $load_ordering:expr, $store_ordering:expr) => {
        #[derive(Default)]
        pub struct $name($underlying);

        impl $name {
            pub const fn new(v: $raw) -> Self {
                Self($underlying::new(v))
            }
            pub fn load(&self) -> $raw {
                self.0.load($load_ordering)
            }
            pub fn store(&self, v: $raw) {
                self.0.store(v, $store_ordering);
            }
        }
    };
}

atomic! { AcqRelUsize, AtomicUsize, usize, Ordering::Acquire, Ordering::Release }

atomic! { AcqRelU64, AtomicU64, u64, Ordering::Acquire, Ordering::Release }

atomic! { RelaxedU32, AtomicU32, u32, Ordering::Relaxed, Ordering::Relaxed }

atomic! { RelaxedU8, AtomicU8, u8, Ordering::Relaxed, Ordering::Relaxed }

macro_rules! atomic_ptr {
    ($name:ident, $load_ordering:expr, $store_ordering:expr) => {
        pub struct $name<T>(AtomicPtr<T>);

        impl<T> $name<T> {
            pub fn new(ptr: *mut T) -> Self {
                Self(AtomicPtr::new(ptr))
            }
            pub fn new_with_box(value: Box<T>) -> Self {
                Self(AtomicPtr::new(Box::into_raw(value)))
            }
            pub fn load(&self) -> *mut T {
                self.0.load($load_ordering)
            }
            pub fn store(&self, ptr: *mut T) {
                self.0.store(ptr, $store_ordering);
            }
            pub fn store_with_nonnull(&self, ptr: NonNull<T>) {
                self.0.store(ptr.as_ptr(), $store_ordering);
            }
            pub fn store_with_box(&self, value: Box<T>) {
                self.0.store(Box::into_raw(value), $store_ordering);
            }
        }

        impl<T> Default for $name<T> {
            fn default() -> Self {
                $name::new(std::ptr::null_mut())
            }
        }
    };
}

atomic_ptr! {AcqRelAtomicPtr, Ordering::Acquire, Ordering::Release}
atomic_ptr! {RelaxedAtomicPtr, Ordering::Relaxed, Ordering::Relaxed}
