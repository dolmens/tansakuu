use crate::{document::Document, DocId};

use super::{
    segment::{BuildingSegment, BuildingSegmentColumnData, BuildingSegmentIndexData},
    Table, TableColumnWriter, TableIndexWriter,
};

pub struct TableWriter<'a> {
    docid: DocId,
    column_writer: TableColumnWriter,
    index_writer: TableIndexWriter,
    table: &'a Table,
}

impl<'a> TableWriter<'a> {
    pub fn new(table: &'a Table) -> Self {
        let column_writer = TableColumnWriter::new(table.schema());
        let column_data = BuildingSegmentColumnData::new(&column_writer);
        let index_writer = TableIndexWriter::new(table.schema());
        let index_data = BuildingSegmentIndexData::new(&index_writer);

        let building_segment = BuildingSegment::new(column_data, index_data);
        table.add_building_segment(building_segment);

        Self {
            docid: 0,
            column_writer,
            index_writer,
            table,
        }
    }

    pub fn add_doc(&mut self, doc: &Document) {
        self.column_writer.add_doc(doc, self.docid);
        self.index_writer.add_doc(doc, self.docid);

        self.docid += 1;
    }
}

impl<'a> Drop for TableWriter<'a> {
    fn drop(&mut self) {}
}
