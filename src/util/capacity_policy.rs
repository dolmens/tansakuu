pub trait CapacityPolicy {
    fn next_capacity(&self, current: usize) -> usize;
}

#[derive(Default)]
pub struct FixedCapacityPolicy;

impl CapacityPolicy for FixedCapacityPolicy {
    fn next_capacity(&self, current: usize) -> usize {
        current
    }
}
