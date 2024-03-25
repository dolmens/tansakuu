use std::{alloc::Layout, fmt::Debug, panic::RefUnwindSafe, sync::Arc};

pub trait Allocation: RefUnwindSafe + Send + Sync {}

impl<T: RefUnwindSafe + Send + Sync> Allocation for T {}

pub enum Deallocation {
    Standard(Layout),
    Custom(Arc<dyn Allocation>, usize),
}

impl Debug for Deallocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Deallocation::Standard(layout) => {
                write!(f, "Deallocation::Standard, {layout:?}")
            }
            Deallocation::Custom(_, size) => {
                write!(f, "Deallocation::Custom {{ capacity: {size} }}")
            }
        }
    }
}
