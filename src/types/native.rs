use arrow::datatypes::ArrowNativeType;
use half::f16;

pub trait NativeType: ArrowNativeType {}

macro_rules! impl_native {
    ($type:ty) => {
        impl NativeType for $type {}
    };
}

impl_native!(i8);
impl_native!(i16);
impl_native!(i32);
impl_native!(i64);
impl_native!(i128);
impl_native!(u8);
impl_native!(u16);
impl_native!(u32);
impl_native!(u64);
impl_native!(f16);
impl_native!(f32);
impl_native!(f64);
