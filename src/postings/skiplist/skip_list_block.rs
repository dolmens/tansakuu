use crate::SKIPLIST_BLOCK_LEN;

use super::SkipListFormat;

pub struct SkipListBlock {
    pub len: usize,
    pub keys: [u32; SKIPLIST_BLOCK_LEN],
    pub offsets: [u32; SKIPLIST_BLOCK_LEN],
    pub values: Option<Box<[u32]>>,
}

impl SkipListBlock {
    pub fn new(skip_list_format: &SkipListFormat) -> Self {
        let values = if skip_list_format.has_value() {
            Some(
                std::iter::repeat(0)
                    .take(SKIPLIST_BLOCK_LEN)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            )
        } else {
            None
        };

        Self {
            len: 0,
            keys: [0; SKIPLIST_BLOCK_LEN],
            offsets: [0; SKIPLIST_BLOCK_LEN],
            values,
        }
    }
}
