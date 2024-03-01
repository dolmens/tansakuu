use arrow::datatypes::ArrowPrimitiveType;

use crate::schema::DataType;

use super::NativeType;

mod private {
    pub trait PrimitiveTypeSealed {}
}

pub trait PrimitiveType: private::PrimitiveTypeSealed + 'static {
    type Native: NativeType;
    type ArrowPrimitive: ArrowPrimitiveType<Native = Self::Native>;
    const DATA_TYPE: DataType;

    fn default_value() -> Self::Native {
        Default::default()
    }
}

macro_rules! make_type {
    ($name:ident, $native_ty:ty, $data_ty:expr, $doc_string: literal) => {
        #[derive(Debug)]
        #[doc = $doc_string]
        pub struct $name {}

        impl PrimitiveType for $name {
            type Native = $native_ty;
            type ArrowPrimitive = arrow::datatypes::$name;
            const DATA_TYPE: DataType = $data_ty;
        }

        impl private::PrimitiveTypeSealed for $name {}
    };
}

make_type!(
    Int64Type,
    i64,
    DataType::Int64,
    "A signed 64-bit integer type."
);
