use std::ops::Range;

#[derive(Clone)]
pub struct TermInfo {
    pub df: usize,
    pub doc_list_start: usize,
    pub doc_list_end: usize,
    pub skip_list_start: usize,
    pub skip_list_end: usize,

    pub ttf: usize,
    pub position_list_start: usize,
    pub position_list_end: usize,
    pub position_skip_list_start: usize,
    pub position_skip_list_end: usize,
}

impl TermInfo {
    pub fn skip_list_range(&self) -> Range<usize> {
        self.skip_list_start..self.skip_list_end
    }

    pub fn doc_list_range(&self) -> Range<usize> {
        self.doc_list_start..self.doc_list_end
    }

    pub fn position_skip_list_range(&self) -> Range<usize> {
        self.position_skip_list_start..self.position_skip_list_end
    }

    pub fn position_list_range(&self) -> Range<usize> {
        self.position_list_start..self.position_list_end
    }
}
