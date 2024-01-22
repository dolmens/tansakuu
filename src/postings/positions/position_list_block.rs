use crate::POSITION_BLOCK_LEN;

pub struct PositionListBlock {
    pub offset: usize,
    pub len: usize,
    pub positions: [u32; POSITION_BLOCK_LEN],
}

impl PositionListBlock {
    pub fn new() -> Self {
        Self {
            offset: 0,
            len: 0,
            positions: [0; POSITION_BLOCK_LEN],
        }
    }
}
