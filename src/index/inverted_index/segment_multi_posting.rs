use crate::DocId;

use super::{BuildingSegmentPosting, PersistentSegmentPosting};

pub struct SegmentMultiPosting<'a> {
    base_docid: DocId,
    posting_data: SegmentMultiPostingData<'a>,
}

pub enum SegmentMultiPostingData<'a> {
    Persistent(Vec<PersistentSegmentPosting<'a>>),
    Building(Vec<BuildingSegmentPosting<'a>>),
}

impl<'a> SegmentMultiPosting<'a> {
    pub fn new(base_docid: DocId, posting_data: SegmentMultiPostingData<'a>) -> Self {
        Self {
            base_docid,
            posting_data,
        }
    }

    pub fn base_docid(&self) -> DocId {
        self.base_docid
    }

    pub fn posting_data(&self) -> &SegmentMultiPostingData {
        &self.posting_data
    }

    pub fn posting_count(&self) -> usize {
        self.posting_data.posting_count()
    }
}

impl<'a> SegmentMultiPostingData<'a> {
    pub fn posting_count(&self) -> usize {
        match self {
            Self::Persistent(postings) => postings.len(),
            Self::Building(postings) => postings.len(),
        }
    }
}
