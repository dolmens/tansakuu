use std::sync::Arc;

use arrow::{
    array::{Float64Array, StructArray},
    buffer::NullBuffer,
};

use crate::columnar::GeoLocationColumnBuildingSegmentData;

use super::{
    geo_location_defines::{INVALID_LATITUDE, INVALID_LONGITUDE},
    ColumnSerializer,
};

#[derive(Default)]
pub struct GeoLocationColumnSerializer {}

impl ColumnSerializer for GeoLocationColumnSerializer {
    fn serialize(
        &self,
        column_data: &dyn super::ColumnBuildingSegmentData,
        _doc_count: usize,
        _docid_mapping: Option<&Vec<Option<crate::DocId>>>,
    ) -> arrow::array::ArrayRef {
        let geo_location_column_data = column_data
            .downcast_ref::<GeoLocationColumnBuildingSegmentData>()
            .unwrap();
        let (longitudes, latitudes): (Vec<_>, Vec<_>) = geo_location_column_data
            .values
            .iter()
            .map(|&coord| match coord {
                Some((lon, lat)) => (lon, lat),
                None => (INVALID_LONGITUDE, INVALID_LATITUDE),
            })
            .unzip();
        let longitude_array = Arc::new(Float64Array::from(longitudes));
        let latitude_array = Arc::new(Float64Array::from(latitudes));

        let longitude = Arc::new(arrow_schema::Field::new(
            "longitude",
            arrow_schema::DataType::Float64,
            false,
        ));
        let latitude = Arc::new(arrow_schema::Field::new(
            "latitude",
            arrow_schema::DataType::Float64,
            false,
        ));
        let fields = vec![longitude, latitude].into();
        let nulls = if geo_location_column_data.nullable {
            Some(NullBuffer::from(
                longitude_array
                    .iter()
                    .map(|lon| lon.unwrap() != INVALID_LONGITUDE)
                    .collect::<Vec<bool>>(),
            ))
        } else {
            None
        };
        Arc::new(StructArray::new(
            fields,
            vec![longitude_array, latitude_array],
            nulls,
        ))
    }
}
