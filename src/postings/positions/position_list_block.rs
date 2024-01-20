use crate::POSITION_BLOCK_LEN;

pub struct PositionListBlock {
    pub len: usize,
    pub positions: [u32; POSITION_BLOCK_LEN],
}

impl PositionListBlock {
    pub fn new() -> Self {
        Self {
            len: 0,
            positions: [0; POSITION_BLOCK_LEN],
        }
    }
}
