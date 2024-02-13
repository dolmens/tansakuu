use crate::POSITION_LIST_BLOCK_LEN;

pub struct PositionListBlock {
    pub start_ttf: u64,
    pub len: usize,
    pub positions: [u32; POSITION_LIST_BLOCK_LEN],
}

impl PositionListBlock {
    pub fn new() -> Self {
        Self {
            start_ttf: 0,
            len: 0,
            positions: [0; POSITION_LIST_BLOCK_LEN],
        }
    }
}
