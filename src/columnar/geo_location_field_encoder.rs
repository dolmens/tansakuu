#[derive(Default)]
pub struct GeoLocationFieldEncoder {}

impl GeoLocationFieldEncoder {
    // Invalid coord may parsed but will not be indexed
    pub fn parse(&self, value: &str) -> Option<(f64, f64)> {
        let parts: Vec<_> = value
            .split(',')
            .flat_map(|s| s.trim().parse::<f64>().ok())
            .collect();
        if parts.len() == 2 {
            Some((parts[0], parts[1]))
        } else {
            None
        }
    }
}
