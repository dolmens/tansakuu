use std::path::Path;

use downcast_rs::{impl_downcast, DowncastSync};

use crate::{schema::Field, Directory};

pub trait ColumnSegmentDataBuilder {
    fn build(
        &self,
        field: &Field,
        directory: &dyn Directory,
        path: &Path,
    ) -> Box<dyn ColumnSegmentData>;
}

pub trait ColumnSegmentData: DowncastSync {}
impl_downcast!(sync ColumnSegmentData);
