use crate::{FieldMask, TermFreq};

#[derive(Default, Clone, Copy)]
pub struct MatchData {
    pub tf: TermFreq,
    pub fm: FieldMask,
}
