use crate::schema::{IndexRef, IndexType};

use super::{
    bitset::BitsetIndexWriter, inverted_index::InvertedIndexWriter, range::RangeIndexWriter,
    spatial::SpatialIndexWriter, unique_key::UniqueKeyWriter, IndexWriter, IndexWriterResource,
};

#[derive(Default)]
pub struct IndexWriterFactory {}

impl IndexWriterFactory {
    pub fn create(
        &self,
        index: &IndexRef,
        writer_resource: &IndexWriterResource,
    ) -> Box<dyn IndexWriter> {
        match index.index_type() {
            IndexType::Text(_) => {
                Box::new(InvertedIndexWriter::new(index.clone(), writer_resource))
            }
            IndexType::PrimaryKey | IndexType::UniqueKey => {
                Box::new(UniqueKeyWriter::new(writer_resource))
            }
            IndexType::Bitset => Box::new(BitsetIndexWriter::new(index.clone(), writer_resource)),
            IndexType::Range => Box::new(RangeIndexWriter::new(index.clone(), writer_resource)),
            IndexType::Spatial(_) => {
                Box::new(SpatialIndexWriter::new(index.clone(), writer_resource))
            }
        }
    }
}
