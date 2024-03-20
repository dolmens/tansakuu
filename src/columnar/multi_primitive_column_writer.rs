use std::sync::Arc;

use crate::{
    document::Value, schema::FieldRef, types::NativeType, util::chunked_vec::ChunkedVecWriter,
    BUILDING_COLUMN_VEC_CHUNK_SIZE, BUILDING_COLUMN_VEC_NODE_SIZE,
};

use super::{ColumnWriter, MultiPrimitiveColumnBuildingSegmentData};

pub struct MultiPrimitiveColumnWriter<T: NativeType> {
    field: FieldRef,
    writer: ChunkedVecWriter<Option<Box<[T]>>>,
    column_data: Arc<MultiPrimitiveColumnBuildingSegmentData<T>>,
}

impl<T: NativeType> MultiPrimitiveColumnWriter<T> {
    pub fn new(field: FieldRef) -> Self {
        let writer = ChunkedVecWriter::new(
            BUILDING_COLUMN_VEC_CHUNK_SIZE,
            BUILDING_COLUMN_VEC_NODE_SIZE,
        );
        let reader = writer.reader();
        let column_data = Arc::new(MultiPrimitiveColumnBuildingSegmentData::new(reader));

        Self {
            field,
            writer,
            column_data,
        }
    }
}

macro_rules! impl_list_primitive_column_writer {
    ($ty:ty, $get_value:ident) => {
        impl ColumnWriter for MultiPrimitiveColumnWriter<$ty> {
            fn field(&self) -> &FieldRef {
                &self.field
            }

            fn add_value(&mut self, value: Option<&crate::document::OwnedValue>) {
                if let Some(iter) = value.map(|value| value.as_array()).flatten() {
                    let values: Vec<_> = iter
                        .map(|elem| elem.$get_value().map(|v| v as $ty).unwrap_or_default())
                        .collect();
                    self.writer.push(Some(values.into_boxed_slice()));
                } else {
                    if self.field.is_nullable() {
                        self.writer.push(None);
                    } else {
                        self.writer.push(Some(vec![].into_boxed_slice()));
                    }
                }
            }

            fn column_data(&self) -> Arc<dyn super::ColumnBuildingSegmentData> {
                self.column_data.clone()
            }
        }
    };
}

impl_list_primitive_column_writer!(i8, as_i64);
impl_list_primitive_column_writer!(i16, as_i64);
impl_list_primitive_column_writer!(i32, as_i64);
impl_list_primitive_column_writer!(i64, as_i64);
impl_list_primitive_column_writer!(u8, as_u64);
impl_list_primitive_column_writer!(u16, as_u64);
impl_list_primitive_column_writer!(u32, as_u64);
impl_list_primitive_column_writer!(u64, as_u64);

impl_list_primitive_column_writer!(f32, as_f64);
impl_list_primitive_column_writer!(f64, as_f64);
