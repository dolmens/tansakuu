use super::capacity_policy::CapacityPolicy;

#[derive(Default)]
pub struct FractionalCapacityPolicy<const I: usize, const H: usize>;

impl<const I: usize, const H: usize> CapacityPolicy for FractionalCapacityPolicy<I, H> {
    fn next_capacity(&self, current: usize) -> usize {
        if current == 0 {
            I
        } else {
            std::cmp::min(current + (current >> 2), H)
        }
    }
}

pub const FRACTIONAL_CAPACITY_INIT_SIZE: usize = 128;
pub const FRACTIONAL_CAPACITY_CHUNK_SIZE: usize = 10 * 1024 * 1024;
pub type FractionalChunkCapacityPolicy =
    FractionalCapacityPolicy<FRACTIONAL_CAPACITY_INIT_SIZE, FRACTIONAL_CAPACITY_CHUNK_SIZE>;
