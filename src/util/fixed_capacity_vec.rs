use std::{
    alloc::{self, Layout},
    mem,
    ops::Deref,
    ptr::{self, NonNull},
    slice,
};

use super::AcqRelUsize;

pub struct FixedCapacityVec<T> {
    len: AcqRelUsize,
    buf: RawVec<T>,
}

struct RawVec<T> {
    capacity: usize,
    ptr: NonNull<T>,
}

unsafe impl<T> Sync for FixedCapacityVec<T> {}

impl<T> RawVec<T> {
    fn with_capacity(capacity: usize) -> Self {
        let layout = Layout::array::<T>(capacity).unwrap();
        let ptr = unsafe { alloc::alloc(layout) } as *mut T;
        if ptr.is_null() {
            alloc::handle_alloc_error(layout);
        }

        Self {
            capacity,
            ptr: unsafe { NonNull::new_unchecked(ptr) },
        }
    }
}

impl<T> Drop for RawVec<T> {
    fn drop(&mut self) {
        let elem_size = mem::size_of::<T>();

        if self.capacity != 0 && elem_size != 0 {
            unsafe {
                alloc::dealloc(
                    self.ptr.as_ptr() as *mut u8,
                    Layout::array::<T>(self.capacity).unwrap(),
                );
            }
        }
    }
}

impl<T> FixedCapacityVec<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            len: AcqRelUsize::new(0),
            buf: RawVec::with_capacity(capacity),
        }
    }

    pub fn capacity(&self) -> usize {
        self.buf.capacity
    }

    pub fn len(&self) -> usize {
        self.len.load()
    }

    pub unsafe fn push(&self, elem: T) {
        let len = self.len();
        if len == self.capacity() {
            panic!("FixedCapacityVec overflow");
        }

        unsafe {
            ptr::write(self.ptr().as_ptr().add(len), elem);
        }
        self.len.store(len + 1);
    }

    fn ptr(&self) -> NonNull<T> {
        self.buf.ptr
    }
}

impl<T> Deref for FixedCapacityVec<T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        unsafe {
            let len = self.len();
            slice::from_raw_parts(self.ptr().as_ptr(), len)
        }
    }
}

impl<T> Drop for FixedCapacityVec<T> {
    fn drop(&mut self) {
        let mut len = self.len();
        while len > 0 {
            unsafe {
                len -= 1;
                let _ = ptr::read(self.ptr().as_ptr().add(len));
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use std::{thread, time::Duration};

    use super::FixedCapacityVec;

    #[test]
    fn test_simple() {
        let capacity = 64;
        let v = FixedCapacityVec::with_capacity(capacity);
        assert_eq!(v.len(), 0);
        for i in 0..capacity {
            unsafe {
                v.push((i + 1) * 10);
            }
            assert_eq!(v.len(), i + 1);
        }
        for i in 0..capacity {
            assert_eq!(v.get(i).unwrap().clone(), (i + 1) * 10);
        }
    }

    #[test]
    fn test_multithreads() {
        let capacity = 64;
        let v = FixedCapacityVec::<usize>::with_capacity(capacity);
        thread::scope(|scope| {
            let t = scope.spawn(|| loop {
                let len = v.len();
                for i in 0..len {
                    assert_eq!(v.get(i).unwrap().clone(), (i + 1) * 10);
                }
                if len == capacity {
                    break;
                }
                thread::sleep(Duration::from_millis(1));
            });

            for i in 0..capacity {
                unsafe {
                    v.push((i + 1) * 10);
                }
                assert_eq!(v.len(), i + 1);
            }
            for i in 0..capacity {
                assert_eq!(v.get(i).unwrap().clone(), (i + 1) * 10);
            }

            t.join().unwrap();
        });
    }
}
