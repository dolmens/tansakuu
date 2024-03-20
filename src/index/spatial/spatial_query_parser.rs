use crate::columnar::GeoLocationFieldEncoder;

#[derive(Default)]
pub struct SpatialQueryParser {}

pub struct SpartialNearbyQueryTerm {
    pub longitude: f64,
    pub latitude: f64,
    pub distance: f64,
}

impl SpatialQueryParser {
    pub fn parse(&self, term: &str) -> Option<SpartialNearbyQueryTerm> {
        let parts: Vec<_> = term.split(';').collect();
        if parts.len() != 2 {
            return None;
        }
        let field_encoder = GeoLocationFieldEncoder::default();
        let (longitude, latitude) = match field_encoder.parse(parts[0]) {
            Some((longitude, latitude)) => (longitude, latitude),
            None => {
                return None;
            }
        };
        let distance = parts[1].trim().parse::<f64>().unwrap_or_default();

        Some(SpartialNearbyQueryTerm {
            longitude,
            latitude,
            distance,
        })
    }
}
