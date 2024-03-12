use crate::{
    document::Value,
    index::{inverted_index::InvertedIndexPostingWriter, IndexWriter},
    schema::{DataType, IndexRef},
};

use super::RangeFieldEncoder;

pub struct RangeIndexWriter {
    bottom_writer: InvertedIndexPostingWriter,
    higher_writer: InvertedIndexPostingWriter,
    range_encoder: RangeFieldEncoder,
    index: IndexRef,
}

impl IndexWriter for RangeIndexWriter {
    fn add_field(&mut self, field: &crate::schema::FieldRef, value: &crate::document::OwnedValue) {
        let value = match field.data_type() {
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                if let Some(value) = value.as_u64() {
                    value
                } else {
                    return;
                }
            }
            _ => {
                return;
            }
        };
        let keys = self.range_encoder.tokenize(value);
        // TODO: hash, key=0
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
        unimplemented!()
    }
}
