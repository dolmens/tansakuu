#[derive(Default)]
pub struct RangeQueryEncoder {}

impl RangeQueryEncoder {
    // TODO: Need a real parser
    pub fn decode(&self, query: &str) -> (u64, u64) {
        let numbers: Vec<_> = query
            .split(',')
            .map(|s| s.parse::<u64>().unwrap())
            .collect();
        (numbers[0], numbers[1])
    }
}
