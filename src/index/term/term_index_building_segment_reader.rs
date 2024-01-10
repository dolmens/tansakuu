use std::sync::Arc;

use crate::{index::SegmentPosting, postings::BuildingDocListReader, DocId};

use super::TermIndexBuildingSegmentData;

pub struct TermIndexBuildingSegmentReader {
    base_docid: DocId,
    index_data: Arc<TermIndexBuildingSegmentData>,
}

impl TermIndexBuildingSegmentReader {
    pub fn new(base_docid: DocId, index_data: Arc<TermIndexBuildingSegmentData>) -> Self {
        Self {
            base_docid,
            index_data,
        }
    }

    pub fn segment_posting(&self, tok: &str) -> crate::index::SegmentPosting {
        let docids = if let Some(building_doc_list) = self.index_data.postings.get(tok) {
            let doc_list_reader = BuildingDocListReader::open(building_doc_list);
            doc_list_reader
                .into_iter()
                .map(|(docid, _)| docid)
                .collect()
        } else {
            vec![]
        };
        SegmentPosting::new(self.base_docid, docids)
    }
}
