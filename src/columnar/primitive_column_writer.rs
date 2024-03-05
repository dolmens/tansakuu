use std::sync::Arc;

use crate::{
    document::{OwnedValue, Value},
    schema::FieldRef,
    types::NativeType,
    util::chunked_vec::ChunkedVecWriter,
    BUILDING_COLUMN_VEC_CHUNK_SIZE, BUILDING_COLUMN_VEC_NODE_SIZE,
};

use super::{ColumnBuildingSegmentData, ColumnWriter, PrimitiveColumnBuildingSegmentData};

pub struct PrimitiveColumnWriter<T: NativeType> {
    field: FieldRef,
    writer: ChunkedVecWriter<Option<T>>,
    column_data: Arc<PrimitiveColumnBuildingSegmentData<T>>,
}

impl<T: NativeType> PrimitiveColumnWriter<T> {
    pub fn new(field: FieldRef) -> Self {
        let writer = ChunkedVecWriter::new(
            BUILDING_COLUMN_VEC_CHUNK_SIZE,
            BUILDING_COLUMN_VEC_NODE_SIZE,
        );
        let reader = writer.reader();
        let column_data = Arc::new(PrimitiveColumnBuildingSegmentData::new(reader));

        Self {
            field,
            writer,
            column_data,
        }
    }
}

macro_rules! impl_primitive_column_writer {
    ($ty:ty, $get_value:ident) => {
        impl ColumnWriter for PrimitiveColumnWriter<$ty> {
            fn field(&self) -> &FieldRef {
                &self.field
            }

            fn add_value(&mut self, value: Option<&OwnedValue>) {
                if let Some(value) = value.map(|value| value.$get_value()).flatten() {
                    self.writer.push(Some(value));
                } else {
                    if self.field.is_nullable() {
                        self.writer.push(None);
                    } else {
                        self.writer.push(Some(Default::default()));
                    }
                }
            }

            fn column_data(&self) -> Arc<dyn ColumnBuildingSegmentData> {
                self.column_data.clone()
            }
        }
    };
}

impl_primitive_column_writer!(i8, as_i8);
impl_primitive_column_writer!(i16, as_i16);
impl_primitive_column_writer!(i32, as_i32);
impl_primitive_column_writer!(i64, as_i64);
impl_primitive_column_writer!(u8, as_u8);
impl_primitive_column_writer!(u16, as_u16);
impl_primitive_column_writer!(u32, as_u32);
impl_primitive_column_writer!(u64, as_u64);

impl_primitive_column_writer!(f32, as_f32);
impl_primitive_column_writer!(f64, as_f64);
