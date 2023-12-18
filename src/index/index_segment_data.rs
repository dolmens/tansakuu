use downcast_rs::{impl_downcast, DowncastSync};

pub trait IndexSegmentData: DowncastSync + Send + Sync {}
impl_downcast!(sync IndexSegmentData);
