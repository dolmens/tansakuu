use std::{collections::HashMap, sync::Arc};

use crate::{column::ColumnSegmentData, index::IndexSegmentData};

pub struct Segment {
    indexes: HashMap<String, Arc<dyn IndexSegmentData>>,
    columns: HashMap<String, Arc<dyn ColumnSegmentData>>,
}
