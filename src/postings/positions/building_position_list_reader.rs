use crate::postings::{ByteSliceReader, skip_list::BuildingSkipListReader};

use super::{BuildingPositionList, PositionListBlock, PositionListReader};

pub struct BuildingPositionListReader<'a> {
    building_block_snapshot: i32,
    building_skip_ilst_reader: BuildingSkipListReader<'a>,
    position_list_reader: PositionListReader<ByteSliceReader<'a>>,
}

impl<'a> BuildingPositionListReader<'a> {
    pub fn open(building_position_list: &'a BuildingPositionList) {}

    pub fn decode_one_block(&mut self, positions_block: &mut PositionListBlock) {}
}
