use std::ops::Range;

#[derive(Clone)]
pub struct TermInfo {
    pub skip_list_item_count: usize,
    pub skip_list_start: usize,
    pub skip_list_end: usize,
    pub posting_item_count: usize,
    pub posting_start: usize,
    pub posting_end: usize,
    pub position_skip_list_item_count: usize,
    pub position_skip_list_start: usize,
    pub position_skip_list_end: usize,
    pub position_list_item_count: usize,
    pub position_list_start: usize,
    pub position_list_end: usize,
}

impl TermInfo {
    pub fn skip_list_range(&self) -> Range<usize> {
        self.skip_list_start..self.skip_list_end
    }

    pub fn posting_range(&self) -> Range<usize> {
        self.posting_start..self.posting_end
    }

    pub fn position_skip_list_range(&self) -> Range<usize> {
        self.position_skip_list_start..self.position_skip_list_end
    }

    pub fn position_list_range(&self) -> Range<usize> {
        self.position_list_start..self.position_list_end
    }
}
