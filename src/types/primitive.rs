use arrow::datatypes::ArrowPrimitiveType;

use super::NativeType;

mod private {
    pub trait PrimitiveTypeSealed {}
}

pub trait PrimitiveType: private::PrimitiveTypeSealed + 'static {
    type Native: NativeType;
    type ArrowPrimitive: ArrowPrimitiveType<Native = Self::Native>;

    fn default_value() -> Self::Native {
        Default::default()
    }
}

macro_rules! make_type {
    ($name:ident, $native_ty:ty, $doc_string: literal) => {
        #[derive(Debug)]
        #[doc = $doc_string]
        pub struct $name {}

        impl PrimitiveType for $name {
            type Native = $native_ty;
            type ArrowPrimitive = arrow::datatypes::$name;
        }

        impl private::PrimitiveTypeSealed for $name {}
    };
}

make_type!(Int8Type, i8, "A signed 8-bit integer type.");
make_type!(Int16Type, i16, "A signed 16-bit integer type.");
make_type!(Int32Type, i32, "A signed 32-bit integer type.");
make_type!(Int64Type, i64, "A signed 64-bit integer type.");
make_type!(UInt8Type, u8, "An unsigned 8-bit integer type.");
make_type!(UInt16Type, u16, "An unsigned 16-bit integer type.");
make_type!(UInt32Type, u32, "An unsigned 32-bit integer type.");
make_type!(UInt64Type, u64, "An unsigned 64-bit integer type.");

make_type!(Float32Type, f32, "A 32-bit floating point number type.");
make_type!(Float64Type, f64, "A 64-bit floating point number type.");
