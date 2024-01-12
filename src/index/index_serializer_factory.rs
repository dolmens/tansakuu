use std::sync::Arc;

use crate::schema::{Index, IndexType};

use super::{
    inverted_index::{InvertedIndexBuildingSegmentData, InvertedIndexSerializer},
    primary_key::PrimaryKeyIndexSerializer,
    IndexSegmentData, IndexSerializer,
};

#[derive(Default)]
pub struct IndexSerializerFactory {}

impl IndexSerializerFactory {
    pub fn create(
        &self,
        index: &Index,
        index_data: Arc<dyn IndexSegmentData>,
    ) -> Box<dyn IndexSerializer> {
        match index.index_type() {
            IndexType::InvertedIndex => {
                let inverted_index_data = index_data
                    .downcast_arc::<InvertedIndexBuildingSegmentData>()
                    .ok()
                    .unwrap();
                Box::new(InvertedIndexSerializer::new(index, inverted_index_data))
            }
            IndexType::PrimaryKey => {
                let primary_key_index_data = index_data.downcast_arc().ok().unwrap();
                Box::new(PrimaryKeyIndexSerializer::new(
                    index,
                    primary_key_index_data,
                ))
            }
        }
    }
}
