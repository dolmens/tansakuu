use super::PostingIterator;

pub trait IndexReader: Send + Sync {
    fn lookup(&self, key: &str) -> Option<Box<dyn PostingIterator>>;
}
