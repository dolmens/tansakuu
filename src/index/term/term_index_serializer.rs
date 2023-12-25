use std::{fs::File, io::Write, sync::Arc};

use crate::{index::index_serializer::IndexSerializer, schema::Index};

use super::TermIndexBuildingSegmentData;

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
        let mut file = File::create(path).unwrap();
        let postings = self.index_data.postings();
        for (term, posting) in &postings {
            write!(file, "{} ", term).unwrap();
            for docid in posting {
                write!(file, "{} ", docid).unwrap();
            }
            writeln!(file).unwrap();
        }
    }
}
