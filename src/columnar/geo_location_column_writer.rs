use std::sync::Arc;

use crate::{
    document::Value, schema::FieldRef, util::chunked_vec::ChunkedVecWriter,
    BUILDING_COLUMN_VEC_CHUNK_SIZE, BUILDING_COLUMN_VEC_NODE_SIZE,
};

use super::{ColumnWriter, GeoLocationColumnBuildingSegmentData, GeoLocationFieldEncoder};

pub struct GeoLocationColumnWriter {
    field: FieldRef,
    writer: ChunkedVecWriter<Option<(f64, f64)>>,
}

impl GeoLocationColumnWriter {
    pub fn new(field: FieldRef) -> Self {
        let writer = ChunkedVecWriter::new(
            BUILDING_COLUMN_VEC_CHUNK_SIZE,
            BUILDING_COLUMN_VEC_NODE_SIZE,
        );

        Self { field, writer }
    }
}

impl ColumnWriter for GeoLocationColumnWriter {
    fn field(&self) -> &crate::schema::FieldRef {
        &self.field
    }

    fn add_value(&mut self, value: Option<&crate::document::OwnedValue>) {
        if let Some(iter) = value.map(|value| value.as_array()).flatten() {
            let coords: Vec<_> = iter.flat_map(|elem| elem.as_f64()).collect();
            if coords.len() >= 2 {
                self.writer.push(Some((coords[0], coords[1])));
                return;
            }
        } else if let Some(encoded_str) = value.map(|value| value.as_str()).flatten() {
            let encoder = GeoLocationFieldEncoder::default();
            if let Some((lon, lat)) = encoder.parse(encoded_str) {
                self.writer.push(Some((lon, lat)));
                return;
            }
        }

        // Force to be nullable
        self.writer.push(None);
    }

    fn column_data(&self) -> std::sync::Arc<dyn super::ColumnBuildingSegmentData> {
        Arc::new(GeoLocationColumnBuildingSegmentData {
            values: self.writer.reader(),
            nullable: self.field.is_nullable(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        columnar::{ColumnWriter, GeoLocationColumnBuildingSegmentData},
        document::OwnedValue,
        schema::{Field, FieldType},
    };

    use super::GeoLocationColumnWriter;

    #[test]
    fn test_basic() {
        let field = Arc::new(Field::new(
            "f1".to_string(),
            FieldType::GeoLocation,
            true,
            false,
            true,
            false,
        ));
        let mut column_writer = GeoLocationColumnWriter::new(field);
        let column_data: Arc<GeoLocationColumnBuildingSegmentData> =
            column_writer.column_data().downcast_arc().ok().unwrap();

        let (lon0, lat0) = (120.19, 30.26);
        let value0: OwnedValue = vec![lon0, lat0].into();
        column_writer.add_value(Some(&value0));

        column_writer.add_value(None);
        let invalid_value: OwnedValue = 1.into();
        column_writer.add_value(Some(&invalid_value));
        let invalid_value: OwnedValue = "a,b".into();
        column_writer.add_value(Some(&invalid_value));

        let (lon1, lat1) = (121.47, 31.23);
        let value1: OwnedValue = format!("{},{}", lon1, lat1).into();
        column_writer.add_value(Some(&value1));

        assert_eq!(column_data.values.get(0).unwrap().unwrap(), (lon0, lat0));
        assert_eq!(column_data.values.get(4).unwrap().unwrap(), (lon1, lat1));
    }
}
