pub struct Term {
    index_name: String,
    keyword: String,
}

impl Term {
    pub fn new(index_name: String, keyword: String) -> Self {
        Self {
            index_name,
            keyword,
        }
    }

    pub fn index_name(&self) -> &str {
        &self.index_name
    }

    pub fn keyword(&self) -> &str {
        &self.keyword
    }
}
