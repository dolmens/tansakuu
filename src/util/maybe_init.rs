use std::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    sync::atomic::{AtomicBool, Ordering},
};

use super::atomic::AcqRelBool;

pub struct MaybeInit<T> {
    initialized: AcqRelBool,
    writer_locked: AtomicBool,
    data: UnsafeCell<MaybeUninit<T>>,
}

unsafe impl<T: Send> Send for MaybeInit<T> {}
unsafe impl<T: Send + Sync> Sync for MaybeInit<T> {}

impl<T> MaybeInit<T> {
    pub fn new() -> Self {
        Self {
            initialized: AcqRelBool::new(false),
            writer_locked: AtomicBool::new(false),
            data: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }

    pub fn new_with_value(data: T) -> Self {
        Self {
            initialized: AcqRelBool::new(true),
            writer_locked: AtomicBool::new(true),
            data: UnsafeCell::new(MaybeUninit::new(data)),
        }
    }

    pub fn is_initialized(&self) -> bool {
        self.initialized.load()
    }

    pub fn initialize_by(&self, data: T) -> Option<T> {
        if self
            .writer_locked
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            unsafe {
                (*self.data.get()).as_mut_ptr().write(data);
            }
            self.initialized.store(true);

            None
        } else {
            Some(data)
        }
    }

    pub fn get(&self) -> Option<&T> {
        if self.is_initialized() {
            unsafe { Some(&*(*self.data.get()).as_ptr()) }
        } else {
            None
        }
    }
}

impl<T> Drop for MaybeInit<T> {
    fn drop(&mut self) {
        if self.is_initialized() {
            unsafe {
                std::ptr::drop_in_place((*self.data.get()).as_mut_ptr());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread};

    use super::MaybeInit;

    #[test]
    fn test_maybe_init_basic() {
        let m = MaybeInit::<String>::new();
        assert!(!m.is_initialized());
        assert_eq!(m.get(), None);
        let s = "hello".to_string();
        assert_eq!(m.initialize_by(s.clone()), None);
        assert!(m.is_initialized());
        assert_eq!(m.get(), Some(&s));
        assert_eq!(m.initialize_by(s.clone()), Some(s));
    }

    #[test]
    fn test_maybe_init_multithread() {
        let m = Arc::new(MaybeInit::<String>::new());
        let mr = m.clone();
        let s = "hello".to_string();
        let sr = s.clone();
        let reader = thread::spawn(move || {
            loop {
                if mr.is_initialized() {
                    break;
                }
                thread::yield_now();
            }
            assert_eq!(mr.get(), Some(&sr));
        });
        assert_eq!(m.initialize_by(s.clone()), None);
        assert!(m.is_initialized());
        assert_eq!(m.get(), Some(&s));
        assert_eq!(m.initialize_by(s.clone()), Some(s));
        reader.join().unwrap();
    }
}
