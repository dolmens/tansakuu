use super::BuildingSegmentIndexReader;

pub struct BuildingSegmentReader {
    index_reader: BuildingSegmentIndexReader,
}

impl BuildingSegmentReader {
    pub fn index_readers(&self) -> &BuildingSegmentIndexReader {
        &self.index_reader
    }
}
