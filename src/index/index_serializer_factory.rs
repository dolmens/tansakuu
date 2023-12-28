use std::sync::Arc;

use crate::schema::{Index, IndexType};

use super::{
    primary_key::PrimaryKeyIndexSerializer,
    term::{TermIndexBuildingSegmentData, TermIndexSerializer},
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
            IndexType::Term => {
                let term_index_data = index_data
                    .downcast_arc::<TermIndexBuildingSegmentData>()
                    .ok()
                    .unwrap();
                Box::new(TermIndexSerializer::new(index, term_index_data))
            }
            IndexType::PrimaryKey => {
                let primary_key_index_data = index_data.downcast_arc().ok().unwrap();
                Box::new(PrimaryKeyIndexSerializer::new(index, primary_key_index_data))
            }
        }
    }
}
