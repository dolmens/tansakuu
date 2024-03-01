use downcast_rs::{impl_downcast, DowncastSync};

pub trait ColumnBuildingSegmentData: DowncastSync {}
impl_downcast!(sync ColumnBuildingSegmentData);
