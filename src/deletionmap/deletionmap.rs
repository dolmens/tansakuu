use crate::{
    table::{segment::PersistentSegment, SegmentId, SegmentRegistry, TableData},
    util::{
        Bitset, BitsetWriter, FixedSizeBitset, FixedSizeBitsetWriter, ImmutableBitset, MaybeInit,
    },
    Directory, DocId,
};

use std::{collections::HashMap, io, sync::Arc};

pub struct DeletionMapWriter {
    segment_registry: SegmentRegistry,
    segment_writers: Vec<DeletionMapSegmentWriter>,
    segment_ids: Vec<SegmentId>,
    deletionmap: Arc<DeletionMap>,
}

struct DeletionMap {
    segment_registry: SegmentRegistry,
    segment_map: HashMap<SegmentId, usize>,
    segments: Vec<DeletionMapSegmentReader>,
}

#[derive(Clone)]
pub struct DeletionMapReader {
    deletionmap: Arc<DeletionMap>,
}

pub struct ImmutableDeletionMap {
    bitset: ImmutableBitset,
}

enum DeletionMapSegmentWriter {
    Persistent(DeletionMapPersistentSegmentWriter),
    Building(DeletionMapBuildingSegmentWriter),
}

pub struct DeletionMapSegmentReader {
    inner: DeletionMapSegment,
}

enum DeletionMapSegment {
    Persistent(DeletionMapPersistentSegment),
    Building(DeletionMapBuildingSegment),
}

struct DeletionMapPersistentSegmentWriter {
    immutable: Option<ImmutableBitset>,
    mutable_writer: Option<FixedSizeBitsetWriter>,
    doc_count: usize,
}

struct DeletionMapPersistentSegment {
    mutable: MaybeInit<FixedSizeBitset>,
    immutable: Option<ImmutableBitset>,
}

struct DeletionMapBuildingSegmentWriter {
    writer: BitsetWriter,
}

struct DeletionMapBuildingSegment {
    bitset: Bitset,
}

impl DeletionMapWriter {
    pub fn new(table_data: &TableData) -> Self {
        let segment_count =
            table_data.persistent_segments().len() + table_data.building_segments().len();
        let mut segment_registry = SegmentRegistry::with_capacity(segment_count);
        let mut segment_writers = Vec::with_capacity(segment_count);
        let mut segment_ids = Vec::with_capacity(segment_count);

        for segment in table_data.persistent_segments() {
            segment_registry.add_persistent_segment(segment.meta());
            segment_ids.push(segment.meta().segment_id().clone());

            let doc_count = segment.meta().doc_count();
            let immutable = segment
                .data()
                .deletionmap()
                .map(|immutable_segment| immutable_segment.bitset().clone());
            let segment_writer =
                DeletionMapSegmentWriter::new_persistent_segment(doc_count, immutable);
            segment_writers.push(segment_writer);
        }

        for segment in table_data.building_segments() {
            segment_registry.add_building_segment(segment.meta(), segment.data().doc_count());
            segment_ids.push(segment.meta().segment_id().clone());
            let segment_writer = DeletionMapSegmentWriter::new_building_segment();
            segment_writers.push(segment_writer);
        }

        let segment_map: HashMap<_, _> = segment_ids
            .iter()
            .enumerate()
            .map(|(i, s)| (s.clone(), i))
            .collect();

        let segments: Vec<_> = segment_writers
            .iter()
            .map(|writer| writer.reader())
            .collect();

        let deletionmap = Arc::new(DeletionMap::new(
            segment_registry.clone(),
            segment_map.clone(),
            segments,
        ));

        Self {
            segment_registry,
            segment_writers,
            segment_ids,
            deletionmap,
        }
    }

    pub fn reload(&mut self, table_data: &TableData) {
        let mut current_segments: HashMap<_, _> = self
            .segment_ids
            .drain(..)
            .zip(self.segment_writers.drain(..))
            .collect();

        self.segment_registry.clear();

        for segment in table_data.persistent_segments() {
            self.segment_registry.add_persistent_segment(segment.meta());
            self.segment_ids.push(segment.meta().segment_id().clone());

            if let Some(segment_writer) = current_segments.remove(segment.meta().segment_id()) {
                self.segment_writers.push(segment_writer);
            } else {
                let doc_count = segment.meta().doc_count();
                let immutable = segment
                    .data()
                    .deletionmap()
                    .map(|immutable_segment| immutable_segment.bitset().clone());
                let segment_writer =
                    DeletionMapSegmentWriter::new_persistent_segment(doc_count, immutable);
                self.segment_writers.push(segment_writer);
            }
        }

        for segment in table_data.building_segments() {
            self.segment_registry
                .add_building_segment(segment.meta(), segment.data().doc_count());
            self.segment_ids.push(segment.meta().segment_id().clone());
            if let Some(segment_writer) = current_segments.remove(segment.meta().segment_id()) {
                self.segment_writers.push(segment_writer);
            } else {
                let segment_writer = DeletionMapSegmentWriter::new_building_segment();
                self.segment_writers.push(segment_writer);
            }
        }

        let segment_map: HashMap<_, _> = self
            .segment_ids
            .iter()
            .enumerate()
            .map(|(i, s)| (s.clone(), i))
            .collect();

        let segments: Vec<_> = self
            .segment_writers
            .iter()
            .map(|writer| writer.reader())
            .collect();

        self.deletionmap = Arc::new(DeletionMap::new(
            self.segment_registry.clone(),
            segment_map,
            segments,
        ));
    }

    pub fn reader(&self) -> DeletionMapReader {
        DeletionMapReader::new(self.deletionmap.clone())
    }

    pub fn delete_document(&mut self, docid: DocId) {
        if let Some(segment_cursor) = self.segment_registry.locate_segment(docid) {
            let docid_in_segment = self
                .segment_registry
                .docid_in_segment(docid, segment_cursor);
            let segment_writer = &mut self.segment_writers[segment_cursor];
            let segment_data = &self.deletionmap.segments[segment_cursor];
            segment_writer.delete_document(docid_in_segment, segment_data);
        }
    }
}

impl DeletionMapSegmentWriter {
    fn new_persistent_segment(doc_count: usize, immutable: Option<ImmutableBitset>) -> Self {
        Self::Persistent(DeletionMapPersistentSegmentWriter::new(
            doc_count, immutable,
        ))
    }

    fn new_building_segment() -> Self {
        Self::Building(DeletionMapBuildingSegmentWriter::new())
    }

    fn reader(&self) -> DeletionMapSegmentReader {
        let inner = match self {
            Self::Persistent(writer) => DeletionMapSegment::Persistent(writer.reader()),
            Self::Building(writer) => DeletionMapSegment::Building(writer.reader()),
        };
        DeletionMapSegmentReader { inner }
    }

    fn delete_document(&mut self, docid: DocId, segment_data: &DeletionMapSegmentReader) {
        match self {
            Self::Persistent(persistent_segment_writer) => {
                let segment_data = match &segment_data.inner {
                    DeletionMapSegment::Persistent(segment_data) => segment_data,
                    _ => return,
                };
                persistent_segment_writer.delete_document(docid, segment_data);
            }
            Self::Building(building_segment_writer) => {
                building_segment_writer.delete_document(docid);
            }
        }
    }
}

impl DeletionMapPersistentSegmentWriter {
    fn new(doc_count: usize, immutable: Option<ImmutableBitset>) -> Self {
        Self {
            mutable_writer: None,
            immutable,
            doc_count,
        }
    }

    fn reader(&self) -> DeletionMapPersistentSegment {
        let immutable = self.immutable.clone();
        let mutable = self
            .mutable_writer
            .as_ref()
            .map(|mutable_writer| mutable_writer.bitset());
        DeletionMapPersistentSegment::new(immutable, mutable)
    }

    fn delete_document(&mut self, docid: DocId, segment_data: &DeletionMapPersistentSegment) {
        if self.mutable_writer.is_none() {
            // TODO: merge immutable if exist
            let writer = FixedSizeBitsetWriter::with_capacity(self.doc_count);
            let bitset = writer.bitset();
            segment_data.mutable.initialize_by(bitset);
            self.mutable_writer = Some(writer);
        }
        let writer = self.mutable_writer.as_mut().unwrap();
        writer.insert(docid as usize);
    }
}

impl DeletionMapBuildingSegmentWriter {
    fn new() -> Self {
        Self {
            writer: BitsetWriter::empty(),
        }
    }

    fn reader(&self) -> DeletionMapBuildingSegment {
        DeletionMapBuildingSegment::new(self.writer.bitset())
    }

    fn delete_document(&mut self, docid: DocId) {
        self.writer.insert(docid as usize);
    }
}

impl DeletionMapSegmentReader {
    pub fn is_deleted(&self, docid: DocId) -> bool {
        match &self.inner {
            DeletionMapSegment::Persistent(persistent_segment) => {
                persistent_segment.is_deleted(docid)
            }
            DeletionMapSegment::Building(building_segment) => building_segment.is_deleted(docid),
        }
    }
}

impl DeletionMapPersistentSegment {
    fn new(immutable: Option<ImmutableBitset>, mutable: Option<FixedSizeBitset>) -> Self {
        let mutable = match mutable {
            Some(mutable) => MaybeInit::new_with_value(mutable),
            None => MaybeInit::new(),
        };

        Self { mutable, immutable }
    }

    fn is_deleted(&self, docid: DocId) -> bool {
        if let Some(bitset) = self.mutable.get() {
            return bitset.contains(docid as usize);
        }
        if let Some(immutable) = self.immutable.as_ref() {
            return immutable.contains(docid as usize);
        }
        false
    }
}

impl DeletionMapBuildingSegment {
    fn new(bitset: Bitset) -> Self {
        Self { bitset }
    }

    fn is_deleted(&self, docid: DocId) -> bool {
        self.bitset.contains(docid as usize)
    }
}

impl DeletionMap {
    fn new_readonly(persistent_segments: &[PersistentSegment]) -> Self {
        let mut segment_registry = SegmentRegistry::with_capacity(persistent_segments.len());
        let mut segment_map = HashMap::with_capacity(persistent_segments.len());
        let mut segments = Vec::with_capacity(persistent_segments.len());
        for segment in persistent_segments {
            segment_registry.add_persistent_segment(segment.meta());
            segment_map.insert(segment.meta().segment_id().clone(), segments.len());
            let doc_count = segment.meta().doc_count();
            let immutable = segment
                .data()
                .deletionmap()
                .map(|deletionmap| deletionmap.bitset().clone());
            let writer = DeletionMapPersistentSegmentWriter::new(doc_count, immutable);
            segments.push(DeletionMapSegmentReader {
                inner: DeletionMapSegment::Persistent(writer.reader()),
            });
        }
        Self {
            segment_registry,
            segment_map,
            segments,
        }
    }

    fn new(
        segment_registry: SegmentRegistry,
        segment_map: HashMap<SegmentId, usize>,
        segments: Vec<DeletionMapSegmentReader>,
    ) -> Self {
        Self {
            segment_registry,
            segment_map,
            segments,
        }
    }

    fn is_deleted(&self, docid: DocId) -> bool {
        if let Some(segment_cursor) = self.segment_registry.locate_segment(docid) {
            let docid_in_segment = self
                .segment_registry
                .docid_in_segment(docid, segment_cursor);
            self.segments[segment_cursor].is_deleted(docid_in_segment)
        } else {
            false
        }
    }

    fn segment_reader(&self, segment_id: &SegmentId) -> Option<&DeletionMapSegmentReader> {
        self.segment_map
            .get(segment_id)
            .map(|&segment_cursor| &self.segments[segment_cursor])
    }
}

impl ImmutableDeletionMap {
    pub fn load(_directory: &dyn Directory, _segment_id: SegmentId) -> io::Result<Option<Self>> {
        // TODO:
        // let deletionmap_path = PathBuf::from("deletionmap").join(segment_id.as_str());
        // if directory.exists(&deletionmap_path).unwrap() {
        //     let deletionmap_data = directory.open_read(&deletionmap_path).unwrap();
        //     if deletionmap_data.len() % 8 != 0 || deletionmap_data.len() * 8 < doc_count {
        //         let mut deletionmap_bytes = deletionmap_data.read_bytes().unwrap();
        //         let words: Vec<_> = (0..deletionmap_data.len() / 8)
        //             .map(|_| deletionmap_bytes.read_u64())
        //             .collect();
        //         let bitset = ImmutableBitset::from_vec(words);
        //         return Self { doc_count, bitset };
        //     } else {
        //         warn!(
        //             "Segment `{}` deletionmap data corrupted",
        //             segment_id.as_str()
        //         );
        //     }
        // }
        //
        Ok(None)
    }

    pub fn bitset(&self) -> &ImmutableBitset {
        &self.bitset
    }
}

impl DeletionMapReader {
    pub fn new_readonly(persistent_segments: &[PersistentSegment]) -> Self {
        let deletionmap = Arc::new(DeletionMap::new_readonly(persistent_segments));
        Self { deletionmap }
    }

    fn new(deletionmap: Arc<DeletionMap>) -> Self {
        Self { deletionmap }
    }

    pub fn is_deleted(&self, docid: DocId) -> bool {
        self.deletionmap.is_deleted(docid)
    }

    pub fn segment_reader(&self, segment_id: &SegmentId) -> Option<&DeletionMapSegmentReader> {
        self.deletionmap.segment_reader(segment_id)
    }
}
