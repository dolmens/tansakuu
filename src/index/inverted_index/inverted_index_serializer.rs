use std::sync::Arc;

use crate::{index::IndexSerializer, schema::Index};

use super::{InvertedIndexBuildingSegmentData, InvertedIndexSerializerWriter};

pub struct InvertedIndexSerializer {
    index_name: String,
    index_data: Arc<InvertedIndexBuildingSegmentData>,
}

impl InvertedIndexSerializer {
    pub fn new(index: &Index, index_data: Arc<InvertedIndexBuildingSegmentData>) -> Self {
        Self {
            index_name: index.name().to_string(),
            index_data,
        }
    }
}

impl IndexSerializer for InvertedIndexSerializer {
    fn serialize(&self, directory: &std::path::Path) {
        let path = directory.join(&self.index_name);
        let mut writer = InvertedIndexSerializerWriter::new(path);
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
