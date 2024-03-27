use crate::{
    index::IndexSegmentDataBuilder,
    util::{bytes::Bytes, ImmutableBitset},
};

use super::BitsetIndexPersistentSegmentData;

#[derive(Default)]
pub struct BitsetIndexSegmentDataBuilder {}

impl BitsetIndexSegmentDataBuilder {
    fn load_bitset(
        &self,
        directory: &dyn crate::Directory,
        path: &std::path::Path,
    ) -> Option<ImmutableBitset> {
        if !directory.exists(&path).unwrap() {
            return None;
        }
        let file = directory.open_read(path).unwrap();
        let owned_bytes = file.read_bytes().unwrap();
        // TODO: If ImmutableBitset use u8 then don't need check align and length
        // TODO: Do we need check the length is zero?
        if owned_bytes
            .as_ptr()
            .align_offset(std::mem::align_of::<u64>())
            == 0
            && owned_bytes.len() % 8 == 0
        {
            let bytes: Bytes = owned_bytes.into();
            Some(ImmutableBitset::from_bytes(bytes))
        } else {
            None
        }
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
