use std::sync::Arc;

use crate::{
    columnar::GeoLocationFieldEncoder,
    document::Value,
    index::{
        inverted_index::{InvertedIndexBuildingSegmentData, InvertedIndexPostingWriter},
        IndexWriter, IndexWriterResource,
    },
    postings::PostingFormat,
    schema::{IndexRef, IndexType},
};

use super::geohash::{geohash_encode_multi_step_embed, GEO_STEP_MAX};

pub struct SpatialIndexWriter {
    writer: InvertedIndexPostingWriter,
    // TODO: How to calculate max step by precision
    _precision: f64,
    index: IndexRef,
}

impl SpatialIndexWriter {
    pub fn new(index: IndexRef, _writer_resource: &IndexWriterResource) -> Self {
        // TODO: use stat to infer intial capacity
        let writer = InvertedIndexPostingWriter::new(PostingFormat::default(), 0);
        let index_options = match index.index_type() {
            IndexType::Spatial(index_options) => index_options,
            _ => {
                panic!("SpatialIndexWriter index non spatial index.");
            }
        };
        let precision = index_options.precision;

        Self {
            writer,
            _precision: precision,
            index,
        }
    }
}

impl IndexWriter for SpatialIndexWriter {
    fn add_field(&mut self, field: &crate::schema::FieldRef, value: &crate::document::OwnedValue) {
        let mut coords = vec![];
        if !field.is_multi() {
            if let Some(iter) = value.as_array() {
                let parts: Vec<_> = iter.flat_map(|elem| elem.as_f64()).collect();
                if parts.len() >= 2 {
                    coords.push((parts[0], parts[1]));
                }
            } else if let Some(encoded_str) = value.as_str() {
                let encoder = GeoLocationFieldEncoder::default();
                if let Some((lon, lat)) = encoder.parse(encoded_str) {
                    coords.push((lon, lat));
                }
            }
        } else {
            // TODO:
        }

        let field_offset = self.index.field_offset(field).unwrap_or_default();

        if !coords.is_empty() {
            for &(longitude, latitude) in &coords {
                let hashkeys =
                    match geohash_encode_multi_step_embed(longitude, latitude, 1, GEO_STEP_MAX) {
                        Ok(hashkeys) => hashkeys,
                        Err(_) => {
                            return;
                        }
                    };
                for &hash in &hashkeys {
                    self.writer.add_token(hash, field_offset);
                }
            }
        }
    }

    fn end_document(&mut self, docid: crate::DocId) {
        self.writer.end_document(docid);
    }

    fn index_data(&self) -> std::sync::Arc<dyn crate::index::IndexSegmentData> {
        Arc::new(InvertedIndexBuildingSegmentData::new(
            self.index.clone(),
            self.writer.posting_format().clone(),
            self.writer.posting_data(),
        ))
    }
}
