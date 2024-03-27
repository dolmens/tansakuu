use crate::{
    postings::{BuildingPostingList, TermInfo},
    DocId,
};

use super::PersistentPostingData;

#[derive(Clone)]
pub struct SegmentPosting<'a> {
    base_docid: DocId,
    doc_count: usize,
    posting_data: SegmentPostingData<'a>,
}

#[derive(Clone)]
pub enum SegmentPostingData<'a> {
    Persistent(PersistentSegmentPosting<'a>),
    Building(BuildingSegmentPosting<'a>),
}

#[derive(Clone)]
pub struct PersistentSegmentPosting<'a> {
    pub term_info: TermInfo,
    pub posting_data: &'a PersistentPostingData,
}

#[derive(Clone)]
pub struct BuildingSegmentPosting<'a> {
    pub building_posting_list: &'a BuildingPostingList,
}

#[derive(Clone)]
pub struct SegmentPostings<'a> {
    segments: Vec<SegmentPosting<'a>>,
}

impl<'a> SegmentPosting<'a> {
    pub fn new_persistent_segment(
        base_docid: DocId,
        doc_count: usize,
        term_info: TermInfo,
        posting_data: &'a PersistentPostingData,
    ) -> Self {
        Self {
            base_docid,
            doc_count,
            posting_data: SegmentPostingData::Persistent(PersistentSegmentPosting {
                term_info,
                posting_data,
            }),
        }
    }

    pub fn new_building_segment(
        base_docid: DocId,
        doc_count: usize,
        building_posting_list: &'a BuildingPostingList,
    ) -> Self {
        Self {
            base_docid,
            doc_count,
            posting_data: SegmentPostingData::Building(BuildingSegmentPosting {
                building_posting_list,
            }),
        }
    }

    pub fn base_docid(&self) -> DocId {
        self.base_docid
    }

    pub fn posting_data(&self) -> &SegmentPostingData {
        &self.posting_data
    }
}

impl<'a> SegmentPostings<'a> {
    pub fn new(segments: Vec<SegmentPosting<'a>>) -> Self {
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

    pub fn segment(&self, index: usize) -> &SegmentPosting<'a> {
        &self.segments[index]
    }
}
