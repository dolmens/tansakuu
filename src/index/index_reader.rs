use downcast_rs::{impl_downcast, DowncastSync};

use crate::query::Term;

use super::PostingIterator;

pub trait IndexReader: Send + Sync + DowncastSync {
    fn lookup(&self, key: &Term) -> Option<Box<dyn PostingIterator>>;
}

impl_downcast!(sync IndexReader);
