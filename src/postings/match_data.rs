use crate::{FieldMask, TermFrequency};

#[derive(Default, Clone, Copy)]
pub struct MatchData {
    pub tf: TermFrequency,
    pub fm: FieldMask,
}
