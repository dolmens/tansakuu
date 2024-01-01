use downcast_rs::{impl_downcast, DowncastSync};

use super::PostingIterator;

pub trait IndexReader: Send + Sync + DowncastSync {
    fn lookup(&self, key: &str) -> Option<Box<dyn PostingIterator>>;
}
impl_downcast!(sync IndexReader);
