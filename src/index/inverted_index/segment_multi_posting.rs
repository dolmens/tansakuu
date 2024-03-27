use crate::DocId;

use super::{BuildingSegmentPosting, PersistentSegmentPosting};

pub struct SegmentMultiPosting<'a> {
    base_docid: DocId,
    doc_count: usize,
    posting_data: SegmentMultiPostingData<'a>,
}

pub enum SegmentMultiPostingData<'a> {
    Persistent(Vec<PersistentSegmentPosting<'a>>),
    Building(Vec<BuildingSegmentPosting<'a>>),
}

pub struct SegmentMultiPostings<'a> {
    segments: Vec<SegmentMultiPosting<'a>>,
}

impl<'a> SegmentMultiPosting<'a> {
    pub fn new(
        base_docid: DocId,
        doc_count: usize,
        posting_data: SegmentMultiPostingData<'a>,
    ) -> Self {
        Self {
            base_docid,
            doc_count,
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

impl<'a> SegmentMultiPostings<'a> {
    pub fn new(segments: Vec<SegmentMultiPosting<'a>>) -> Self {
        Self { segments }
    }

    pub fn locate_segment(&self, docid: DocId) -> Option<usize> {
        for (i, segment) in self.segments.iter().enumerate() {
            if docid < segment.base_docid + (segment.doc_count as DocId) {
                return Some(i);
            }
        }
        None
    }

    pub fn locate_segment_from(&self, docid: DocId, current_cursor: usize) -> Option<usize> {
        for (i, segment) in self.segments.iter().enumerate().skip(current_cursor) {
            if docid < segment.base_docid + (segment.doc_count as DocId) {
                return Some(i);
            }
        }
        None
    }

    pub fn segment(&self, index: usize) -> &SegmentMultiPosting<'a> {
        &self.segments[index]
    }
}
