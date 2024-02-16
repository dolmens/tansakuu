use std::path::Path;

use downcast_rs::{impl_downcast, DowncastSync};

use crate::{schema::Index, table::SegmentStat};

pub trait IndexSegmentDataBuilder {
    fn build(&self, index: &Index, directory: &Path) -> Box<dyn IndexSegmentData>;
}

pub trait IndexSegmentData: DowncastSync + Send + Sync {
    fn collect_stat(&self, segment_stat: &mut SegmentStat) {
        let _ = segment_stat;
    }
}
impl_downcast!(sync IndexSegmentData);
