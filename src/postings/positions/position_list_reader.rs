use std::io::Read;

use crate::postings::positions::PositionListBlock;

pub struct PositionListReader<R: Read> {

    input: R,
}

impl<R: Read> PositionListReader<R> {
    pub fn decode_one_block(&mut self, position_list_block: &mut PositionListBlock) {

    }
}
