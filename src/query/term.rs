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

    // TODO: Should refactor Term to support variant types, include Null and NonNull
    pub fn null(index_name: String) -> Self {
        Self {
            index_name,
            keyword: "null".to_string(),
        }
    }

    // TODO: Should refactor Term to support variant types, include Null and NonNull
    pub fn non_null(index_name: String) -> Self {
        Self {
            index_name,
            keyword: "non_null".to_string(),
        }
    }

    pub fn index_name(&self) -> &str {
        &self.index_name
    }

    pub fn keyword(&self) -> &str {
        &self.keyword
    }

    pub fn as_bool(&self) -> bool {
        self.keyword.trim().eq_ignore_ascii_case("true")
    }

    // TODO: Should refactor Term to support variant types, include Null and NonNull
    pub fn is_null(&self) -> bool {
        self.keyword.trim().eq_ignore_ascii_case("null")
    }

    // TODO: Should refactor Term to support variant types, include Null and NonNull
    pub fn is_non_null(&self) -> bool {
        self.keyword.trim().eq_ignore_ascii_case("non_null")
    }
}
