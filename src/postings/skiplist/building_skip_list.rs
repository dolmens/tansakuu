use std::sync::Arc;

use crate::{
    postings::{ByteSliceList, SkipListFormat},
    util::AcqRelUsize,
};

pub struct BuildingSkipList {
    block: BuildingSkipListBlock,
    slice_list: Arc<ByteSliceList>,
    foramt: SkipListFormat,
}

pub struct BuildingSkipListBlock {
    len: AcqRelUsize,
}
