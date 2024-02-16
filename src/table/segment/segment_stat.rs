use std::collections::HashMap;

#[derive(Default)]
pub struct SegmentStat {
    pub doc_count: usize,
    pub index_term_count: HashMap<String, usize>,
}

impl SegmentStat {
    pub fn new() -> Self {
        Default::default()
    }
}
