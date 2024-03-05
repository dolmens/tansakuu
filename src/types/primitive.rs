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

make_type!(Int8Type, i8, DataType::Int8, "A signed 8-bit integer type.");
make_type!(
    Int16Type,
    i16,
    DataType::Int16,
    "A signed 16-bit integer type."
);
make_type!(
    Int32Type,
    i32,
    DataType::Int32,
    "A signed 32-bit integer type."
);
make_type!(
    Int64Type,
    i64,
    DataType::Int64,
    "A signed 64-bit integer type."
);
make_type!(
    UInt8Type,
    u8,
    DataType::UInt8,
    "An unsigned 8-bit integer type."
);
make_type!(
    UInt16Type,
    u16,
    DataType::UInt16,
    "An unsigned 16-bit integer type."
);
make_type!(
    UInt32Type,
    u32,
    DataType::UInt32,
    "An unsigned 32-bit integer type."
);
make_type!(
    UInt64Type,
    u64,
    DataType::UInt64,
    "An unsigned 64-bit integer type."
);

make_type!(
    Float32Type,
    f32,
    DataType::Float32,
    "A 32-bit floating point number type."
);
make_type!(
    Float64Type,
    f64,
    DataType::Float64,
    "A 64-bit floating point number type."
);
