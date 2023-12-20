use downcast_rs::{impl_downcast, DowncastSync};

pub trait ColumnSegmentData: DowncastSync {}
impl_downcast!(sync ColumnSegmentData);
