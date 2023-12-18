use super::SegmentPosting;

pub trait IndexSegmentReader {
    fn segment_posting(&self, key: &str) -> SegmentPosting;
}
