use std::sync::Arc;

use crate::schema::{IndexRef, IndexType};

use super::{
    inverted_index::{InvertedIndexBuildingSegmentData, InvertedIndexSerializer},
    unique_key::UniqueKeySerializer,
    IndexSegmentData, IndexSerializer,
};

#[derive(Default)]
pub struct IndexSerializerFactory {}

impl IndexSerializerFactory {
    pub fn create(
        &self,
        index: &IndexRef,
        index_data: Arc<dyn IndexSegmentData>,
    ) -> Box<dyn IndexSerializer> {
        match index.index_type() {
            IndexType::Text(_) => {
                let inverted_index_data = index_data
                    .downcast_arc::<InvertedIndexBuildingSegmentData>()
                    .ok()
                    .unwrap();
                Box::new(InvertedIndexSerializer::new(
                    index.clone(),
                    inverted_index_data,
                ))
            }
            IndexType::UniqueKey => {
                let primary_key_data = index_data.downcast_arc().ok().unwrap();
                Box::new(UniqueKeySerializer::new(index, primary_key_data))
            }
        }
    }
}
