use std::sync::Arc;

use allocator_api2::alloc::{Allocator, Global};

use crate::util::atomic::RelaxedAtomicPtr;

use super::{BuildingSkipList, BuildingSkipListWriter, SkipListFormat, SkipListWrite};

pub struct DeferredBuildingSkipListWriter<A: Allocator = Global> {
    skip_list_writer: Option<Box<BuildingSkipListWriter<A>>>,
    building_skip_list: Arc<AtomicBuildingSkipList<A>>,
    initial_slice_capacity: usize,
    skip_list_format: SkipListFormat,
    allocator: Option<A>,
}

pub struct AtomicBuildingSkipList<A: Allocator = Global> {
    building_skip_list: RelaxedAtomicPtr<BuildingSkipList<A>>,
}

impl<A: Allocator> AtomicBuildingSkipList<A> {
    pub fn new() -> Self {
        Self {
            building_skip_list: RelaxedAtomicPtr::default(),
        }
    }

    pub fn load(&self) -> &BuildingSkipList<A> {
        unsafe { self.building_skip_list.load().as_ref().unwrap() }
    }

    fn store(&self, building_skip_list: BuildingSkipList<A>) {
        let boxed = Box::new(building_skip_list);
        self.building_skip_list.store(Box::into_raw(boxed));
    }
}

impl<A: Allocator> Drop for AtomicBuildingSkipList<A> {
    fn drop(&mut self) {
        let ptr = self.building_skip_list.load();
        if !ptr.is_null() {
            unsafe {
                let _ = Box::from_raw(ptr);
            }
        }
    }
}

impl<A: Allocator> DeferredBuildingSkipListWriter<A> {
    pub fn new_in(
        skip_list_format: SkipListFormat,
        initial_slice_capacity: usize,
        allocator: A,
    ) -> Self {
        Self {
            skip_list_writer: None,
            building_skip_list: Arc::new(AtomicBuildingSkipList::new()),
            initial_slice_capacity,
            skip_list_format,
            allocator: Some(allocator),
        }
    }

    pub fn building_skip_list(&self) -> Arc<AtomicBuildingSkipList<A>> {
        self.building_skip_list.clone()
    }
}

impl<A: Allocator + Clone> SkipListWrite for DeferredBuildingSkipListWriter<A> {
    fn add_skip_item(&mut self, key: u64, offset: u64, value: Option<u64>) -> std::io::Result<()> {
        if self.skip_list_writer.is_none() {
            let skip_list_writer = BuildingSkipListWriter::new_in(
                self.skip_list_format.clone(),
                self.initial_slice_capacity,
                self.allocator.take().unwrap(),
            );
            let building_skip_list = skip_list_writer.building_skip_list().clone();
            self.building_skip_list.store(building_skip_list);
            self.skip_list_writer = Some(Box::new(skip_list_writer));
        }
        let skip_list_writer = self.skip_list_writer.as_mut().unwrap();
        skip_list_writer.add_skip_item(key, offset, value)
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
