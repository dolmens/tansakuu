use std::sync::Arc;

use crate::util::atomic::RelaxedAtomicPtr;

use super::{BuildingSkipList, BuildingSkipListWriter, SkipListFormat, SkipListWrite};

pub struct DeferredBuildingSkipListWriter {
    skip_list_writer: Option<Box<BuildingSkipListWriter>>,
    building_skip_list: Arc<AtomicBuildingSkipList>,
    skip_list_format: SkipListFormat,
}

pub struct AtomicBuildingSkipList {
    building_skip_list: RelaxedAtomicPtr<BuildingSkipList>,
}

impl AtomicBuildingSkipList {
    pub fn new() -> Self {
        Self {
            building_skip_list: RelaxedAtomicPtr::default(),
        }
    }

    pub fn load(&self) -> &BuildingSkipList {
        unsafe { self.building_skip_list.load().as_ref().unwrap() }
    }

    fn store(&self, building_skip_list: BuildingSkipList) {
        let boxed = Box::new(building_skip_list);
        self.building_skip_list.store(Box::into_raw(boxed));
    }
}

impl Drop for AtomicBuildingSkipList {
    fn drop(&mut self) {
        let ptr = self.building_skip_list.load();
        if !ptr.is_null() {
            unsafe {
                let _ = Box::from_raw(ptr);
            }
        }
    }
}

impl DeferredBuildingSkipListWriter {
    pub fn new(skip_list_format: SkipListFormat) -> Self {
        Self {
            skip_list_writer: None,
            building_skip_list: Arc::new(AtomicBuildingSkipList::new()),
            skip_list_format,
        }
    }

    pub fn building_skip_list(&self) -> Arc<AtomicBuildingSkipList> {
        self.building_skip_list.clone()
    }
}

impl SkipListWrite for DeferredBuildingSkipListWriter {
    fn add_skip_item_with_value(
        &mut self,
        key: u64,
        offset: u64,
        value: u64,
    ) -> std::io::Result<()> {
        if self.skip_list_writer.is_none() {
            let skip_list_writer = BuildingSkipListWriter::new(self.skip_list_format);
            let building_skip_list = skip_list_writer.building_skip_list().clone();
            self.building_skip_list.store(building_skip_list);
            self.skip_list_writer = Some(Box::new(skip_list_writer));
        }
        let skip_list_writer = self.skip_list_writer.as_mut().unwrap();
        skip_list_writer.add_skip_item_with_value(key, offset, value)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if let Some(skip_list_writer) = self.skip_list_writer.as_mut() {
            skip_list_writer.flush()
        } else {
            Ok(())
        }
    }

    fn written_bytes(&self) -> usize {
        if let Some(skip_list_writer) = self.skip_list_writer.as_ref() {
            skip_list_writer.written_bytes()
        } else {
            0
        }
    }
}
