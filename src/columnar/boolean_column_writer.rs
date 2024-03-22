use std::sync::Arc;

use crate::{document::Value, schema::FieldRef, util::ExpandableBitsetWriter};

use super::{BooleanColumnBuildingSegmentData, ColumnWriter};

pub struct BooleanColumnWriter {
    index: usize,
    values: ExpandableBitsetWriter,
    nulls: Option<ExpandableBitsetWriter>,
    field: FieldRef,
}

impl BooleanColumnWriter {
    pub fn new(field: FieldRef) -> Self {
        // TODO: pass writer resource to get estimate segment doc count
        let values = ExpandableBitsetWriter::with_capacity(512 * 1024);
        // There may be no null values, so a small initial capacity is good.
        let nulls = if field.is_nullable() {
            Some(ExpandableBitsetWriter::with_capacity(1))
        } else {
            None
        };

        Self {
            index: 0,
            values,
            nulls,
            field,
        }
    }
}

impl ColumnWriter for BooleanColumnWriter {
    fn field(&self) -> &crate::schema::FieldRef {
        &self.field
    }

    fn add_value(&mut self, value: Option<&crate::document::OwnedValue>) {
        if let Some(value) = value.map(|value| value.as_bool()).flatten() {
            if value {
                self.values.insert(self.index);
            }
        } else {
            if let Some(nulls) = self.nulls.as_mut() {
                nulls.insert(self.index);
            }
        }
        self.index += 1;
    }

    fn column_data(&self) -> std::sync::Arc<dyn super::ColumnBuildingSegmentData> {
        Arc::new(BooleanColumnBuildingSegmentData {
            nullable: self.field.is_nullable(),
            values: self.values.bitset(),
            nulls: self.nulls.as_ref().map(|nulls| nulls.bitset()),
        })
    }
}
