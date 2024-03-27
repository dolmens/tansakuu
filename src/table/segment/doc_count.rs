use std::sync::Arc;

use crate::util::atomic::AcqRelUsize;

#[derive(Default)]
pub struct DocCountPublisher {
    doc_count: Arc<AcqRelUsize>,
}

#[derive(Clone)]
pub struct BuildingDocCount {
    doc_count: Arc<AcqRelUsize>,
}

#[derive(Clone)]
pub enum DocCountVariant {
    Static(usize),
    Dynamic(BuildingDocCount),
}

impl DocCountPublisher {
    pub fn new(doc_count: usize) -> Self {
        Self {
            doc_count: Arc::new(AcqRelUsize::new(doc_count)),
        }
    }

    pub fn reader(&self) -> BuildingDocCount {
        BuildingDocCount {
            doc_count: self.doc_count.clone(),
        }
    }

    pub fn doc_count(&self) -> usize {
        self.doc_count.load()
    }

    pub fn publish(&mut self, doc_count: usize) {
        self.doc_count.store(doc_count);
    }
}

impl BuildingDocCount {
    pub fn get(&self) -> usize {
        self.doc_count.load()
    }
}

impl DocCountVariant {
    pub fn get(&self) -> usize {
        match self {
            Self::Static(doc_count) => *doc_count,
            Self::Dynamic(doc_count) => doc_count.get(),
        }
    }
}
