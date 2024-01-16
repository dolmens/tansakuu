use std::ops::Range;

#[derive(Clone)]
pub struct TermInfo {
    pub skip_item_count: usize,
    pub skip_offset: usize,
    pub skip_len: usize,
    pub posting_item_count: usize,
    pub posting_offset: usize,
    pub posting_len: usize,
}

impl TermInfo {
    pub fn skip_range(&self) -> Range<usize> {
        self.skip_offset..self.skip_offset + self.skip_len
    }

    pub fn posting_range(&self) -> Range<usize> {
        self.posting_offset..self.posting_offset + self.posting_len
    }
}
