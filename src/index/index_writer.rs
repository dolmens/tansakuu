use std::sync::Arc;

use crate::{document::OwnedValue, schema::FieldRef, DocId};

use super::IndexSegmentData;

pub trait IndexWriter {
    fn add_field(&mut self, field: &FieldRef, value: &OwnedValue);
    fn end_document(&mut self, docid: DocId);
    fn index_data(&self) -> Arc<dyn IndexSegmentData>;
}
