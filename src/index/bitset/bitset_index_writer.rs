use std::sync::Arc;

use crate::{
    document::OwnedValue,
    index::{IndexWriter, IndexWriterResource},
    schema::IndexRef,
    util::ExpandableBitsetWriter,
};

use super::BitsetIndexBuildingSegmentData;

pub struct BitsetIndexWriter {
    nullable: bool,
    current_value: Option<bool>,
    values: ExpandableBitsetWriter,
    nulls: Option<ExpandableBitsetWriter>,
    index: IndexRef,
}

impl BitsetIndexWriter {
    pub fn new(index: IndexRef, writer_resource: &IndexWriterResource) -> Self {
        let recent_segment_doc_count = writer_resource
            .recent_segment_stat()
            .map(|segment| segment.doc_count)
            .unwrap_or(1024);
        let values = ExpandableBitsetWriter::with_capacity(recent_segment_doc_count);
        let nullable = index.is_nullable();
        let nulls = if nullable {
            Some(ExpandableBitsetWriter::with_capacity(1))
        } else {
            None
        };

        Self {
            nullable,
            current_value: None,
            values,
            nulls,
            index,
        }
    }
}

impl IndexWriter for BitsetIndexWriter {
    fn add_field(&mut self, _field: &crate::schema::FieldRef, value: &crate::document::OwnedValue) {
        let value = match value {
            OwnedValue::Bool(value) => Some(*value),
            OwnedValue::Null => None,
            OwnedValue::Array(array_iter) => {
                let mut all_nulls = true;
                let mut all_false = true;
                for elem in array_iter {
                    match elem {
                        OwnedValue::Bool(elem_value) => {
                            all_nulls = false;
                            if *elem_value {
                                all_false = false;
                            }
                        }
                        _ => {
                            all_nulls = false;
                        }
                    }
                }
                if all_nulls {
                    None
                } else {
                    Some(!all_false)
                }
            }
            _ => Some(true),
        };
        // If one field is not null, then this document is not null,
        // if one field is true, then this document is true.
        if let Some(value) = value {
            match self.current_value {
                Some(current_value) => {
                    if !current_value && value {
                        self.current_value = Some(value);
                    }
                }
                None => {
                    self.current_value = Some(value);
                }
            }
        }
    }

    fn end_document(&mut self, docid: crate::DocId) {
        if let Some(current_value) = self.current_value {
            if current_value {
                self.values.insert(docid as usize);
            }
        } else if self.nullable {
            self.nulls.as_mut().unwrap().insert(docid as usize);
        }
        self.values.set_item_len((docid + 1) as usize);
        if self.nullable {
            self.nulls
                .as_mut()
                .unwrap()
                .set_item_len((docid + 1) as usize);
        }

        self.current_value = None;
    }

    fn index_data(&self) -> std::sync::Arc<dyn crate::index::IndexSegmentData> {
        Arc::new(BitsetIndexBuildingSegmentData::new(
            self.index.clone(),
            self.values.bitset(),
            self.nulls.as_ref().map(|nulls| nulls.bitset()),
        ))
    }
}
