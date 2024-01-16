use std::ops::Range;

#[derive(Clone)]
pub struct TermInfo {
    pub skip_item_count: usize,
    pub skip_start: usize,
    pub skip_end: usize,
    pub posting_item_count: usize,
    pub posting_start: usize,
    pub posting_end: usize,
}

impl TermInfo {
    pub fn skip_range(&self) -> Range<usize> {
        self.skip_start..self.skip_end
    }

    pub fn posting_range(&self) -> Range<usize> {
        self.posting_start..self.posting_end
    }
}
