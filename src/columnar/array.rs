use arrow::array::ArrayRef;

pub trait GetArrowArray {
    fn get_arrow_array(&self) -> ArrayRef;
}
