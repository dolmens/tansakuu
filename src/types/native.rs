use arrow::datatypes::ArrowNativeType;

pub trait NativeType: ArrowNativeType {}

impl NativeType for i64 {}
