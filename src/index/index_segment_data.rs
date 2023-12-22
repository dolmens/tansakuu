use std::path::Path;

use downcast_rs::{impl_downcast, DowncastSync};

use crate::schema::Index;

pub trait IndexSegmentDataBuilder {
    fn build(&self, index: &Index, directory: &Path) -> Box<dyn IndexSegmentData>;
}

pub trait IndexSegmentData: DowncastSync + Send + Sync {}
impl_downcast!(sync IndexSegmentData);
