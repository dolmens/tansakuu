use crate::{
    postings::{BuildingPostingList, TermInfo},
    DocId,
};

use super::InvertedIndexPersistentSegmentData;

#[derive(Clone)]
pub struct SegmentPosting<'a> {
    base_docid: DocId,
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
    pub index_data: &'a InvertedIndexPersistentSegmentData,
}

#[derive(Clone)]
pub struct BuildingSegmentPosting<'a> {
    pub building_posting_list: &'a BuildingPostingList,
}

impl<'a> SegmentPosting<'a> {
    pub fn new_persistent_segment(
        base_docid: DocId,
        term_info: TermInfo,
        index_data: &'a InvertedIndexPersistentSegmentData,
    ) -> Self {
        Self {
            base_docid,
            posting_data: SegmentPostingData::Persistent(PersistentSegmentPosting {
                term_info,
                index_data,
            }),
        }
    }

    pub fn new_building_segment(
        base_docid: DocId,
        building_posting_list: &'a BuildingPostingList,
    ) -> Self {
        Self {
            base_docid,
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
