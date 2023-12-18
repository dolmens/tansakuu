use super::table::Table;

pub struct TableWriter<'a> {
    table: &'a Table,
}

impl<'a> TableWriter<'a> {
    pub fn new(table: &'a Table) -> Self {
        let writer = Self { table };
        table.reinit_reader();
        writer
    }

    // pub fn add_doc(&self, doc: &Document) {

    // }
}

impl<'a> Drop for TableWriter<'a> {
    fn drop(&mut self) {}
}
