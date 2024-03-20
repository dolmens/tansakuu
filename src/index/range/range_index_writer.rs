use std::sync::Arc;

use crate::{
    document::Value,
    index::{
        inverted_index::{InvertedIndexPostingWriter, TokenHasher},
        IndexWriter, IndexWriterResource,
    },
    postings::PostingFormat,
    schema::{FieldType, IndexRef},
};

use super::{RangeIndexBuildingSegmentData, RangeValueEncoder};

pub struct RangeIndexWriter {
    bottom_writer: InvertedIndexPostingWriter,
    higher_writer: InvertedIndexPostingWriter,
    index: IndexRef,
    range_encoder: RangeValueEncoder,
}

impl RangeIndexWriter {
    pub fn new(index: IndexRef, _writer_resource: &IndexWriterResource) -> Self {
        // TODO: use seg stat to get a better initialize size
        let bottom_writer = InvertedIndexPostingWriter::new(PostingFormat::default(), 0);
        let higher_writer = InvertedIndexPostingWriter::new(PostingFormat::default(), 0);

        Self {
            bottom_writer,
            higher_writer,
            index,
            range_encoder: RangeValueEncoder::default(),
        }
    }
}

impl IndexWriter for RangeIndexWriter {
    fn add_field(&mut self, field: &crate::schema::FieldRef, value: &crate::document::OwnedValue) {
        let value = match field.data_type() {
            FieldType::UInt8 | FieldType::UInt16 | FieldType::UInt32 | FieldType::UInt64 => {
                if let Some(value) = value.as_u64() {
                    value
                } else {
                    return;
                }
            }
            _ => {
                // TODO: signed integer
                return;
            }
        };
        let keys = self.range_encoder.tokenize(value);
        let token_hasher = TokenHasher::default();
        let keys: Vec<_> = keys
            .into_iter()
            .map(|k| token_hasher.hash_bytes(&k.to_le_bytes()))
            .collect();
        let field_offset = self.index.field_offset(field).unwrap_or_default();
        self.bottom_writer.add_token(keys[0], field_offset);
        for i in 1..16 {
            self.higher_writer.add_token(keys[i], field_offset);
        }
    }

    fn end_document(&mut self, docid: crate::DocId) {
        self.bottom_writer.end_document(docid);
        self.higher_writer.end_document(docid);
    }

    fn index_data(&self) -> std::sync::Arc<dyn crate::index::IndexSegmentData> {
        Arc::new(RangeIndexBuildingSegmentData::new(
            self.index.clone(),
            self.bottom_writer.posting_data(),
            self.higher_writer.posting_data(),
        ))
    }
}
