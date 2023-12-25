use std::sync::Arc;

use crate::{index::IndexSerializer, schema::Index};

use super::{TermIndexBuildingSegmentData, TermIndexSerializerWriter};

pub struct TermIndexSerializer {
    index_name: String,
    index_data: Arc<TermIndexBuildingSegmentData>,
}

impl TermIndexSerializer {
    pub fn new(index: &Index, index_data: Arc<TermIndexBuildingSegmentData>) -> Self {
        Self {
            index_name: index.name().to_string(),
            index_data,
        }
    }
}

impl IndexSerializer for TermIndexSerializer {
    fn serialize(&self, directory: &std::path::Path) {
        let path = directory.join(&self.index_name);
        let mut writer = TermIndexSerializerWriter::new(path);
        let postings = self.index_data.postings();
        for (term, posting) in &postings {
            writer.start_term(term.to_string());
            for &docid in posting {
                writer.add_doc(&term, docid);
            }
            writer.end_term(&term);
        }
    }
}
