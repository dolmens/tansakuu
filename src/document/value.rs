#[derive(Clone)]
pub enum Value {
    Str(String),
    I64(i64),
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::Str(value.to_string())
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::Str(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::I64(value)
    }
}

impl TryInto<String> for Value {
    type Error = Value;

    fn try_into(self) -> Result<String, Self::Error> {
        if let Self::Str(s) = self {
            Ok(s)
        } else {
            Err(self)
        }
    }
}

impl TryInto<i64> for Value {
    type Error = Value;

    fn try_into(self) -> Result<i64, Self::Error> {
        if let Self::I64(val) = self {
            Ok(val)
        } else {
            Err(self)
        }
    }
}

impl Value {
    pub fn to_string(&self) -> String {
        match self {
            Self::Str(s) => s.clone(),
            Self::I64(val) => val.to_string(),
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        if let Self::Str(s) = self {
            Some(s)
        } else {
            None
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        if let Self::I64(val) = self {
            Some(*val)
        } else {
            None
        }
    }
}
