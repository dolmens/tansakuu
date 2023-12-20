use downcast_rs::{impl_downcast, DowncastSync};

pub trait ColumnReader: DowncastSync {}
impl_downcast!(sync ColumnReader);
