use crate::{
    index::IndexSegmentDataBuilder,
    util::{bytes::Bytes, ImmutableBitset8},
};

use super::BitsetIndexPersistentSegmentData;

#[derive(Default)]
pub struct BitsetIndexSegmentDataBuilder {}

impl BitsetIndexSegmentDataBuilder {
    fn load_bitset(
        &self,
        directory: &dyn crate::Directory,
        path: &std::path::Path,
    ) -> Option<ImmutableBitset8> {
        if !directory.exists(&path).unwrap() {
            return None;
        }
        let file = directory.open_read(path).unwrap();
        let owned_bytes = file.read_bytes().unwrap();
        // TODO: Do we need check the length is zero?
        let bytes: Bytes = owned_bytes.into();
        Some(ImmutableBitset8::from_bytes(bytes))
    }
}

impl IndexSegmentDataBuilder for BitsetIndexSegmentDataBuilder {
    fn build(
        &self,
        index: &crate::schema::Index,
        directory: &dyn crate::Directory,
        index_path: &std::path::Path,
    ) -> Box<dyn crate::index::IndexSegmentData> {
        let index_path = index_path.join(index.name());
        let values_path = index_path.join("values");
        let values = self.load_bitset(directory, &values_path);
        let nulls = if index.is_nullable() {
            let nulls_path = index_path.join("nulls");
            self.load_bitset(directory, &nulls_path)
        } else {
            None
        };
        Box::new(BitsetIndexPersistentSegmentData { values, nulls })
    }
}
