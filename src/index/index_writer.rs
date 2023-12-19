use std::sync::Arc;

use crate::DocId;

use super::IndexSegmentData;

pub trait IndexWriter {
    fn add_field(&mut self, field: &str, value: &str);
    fn end_document(&mut self, docid: DocId);
    fn index_data(&self) -> Arc<dyn IndexSegmentData>;
}
