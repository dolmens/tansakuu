use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, Int8Array, StringArray};
use tansakuu::{
    columnar::{
        ListPrimitiveColumnReader, ListStringColumnReader, PrimitiveColumnReader,
        StringColumnReader,
    },
    doc,
    schema::{DataType, SchemaBuilder, COLUMNAR, MULTI, NOT_NULL},
    table::{Table, TableSettings},
    types::{Int16Type, Int64Type, Int8Type},
};

#[test]
fn test_column_string() {
    let mut schema_builder = SchemaBuilder::new();
    schema_builder.add_field("f1".to_string(), DataType::String, COLUMNAR);
    schema_builder.add_field("f2".to_string(), DataType::String, COLUMNAR | NOT_NULL);
    let schema = schema_builder.build();
    let settings = TableSettings::new();
    let table = Table::create(schema, settings);

    let mut writer = table.writer();

    let doc = doc!(f1 => "f1_0".to_string(), f2 => "f2_0".to_string());
    writer.add_document(doc);
    let doc = doc!();
    writer.add_document(doc);
    let doc = doc!(f1 => "f1_2".to_string());
    writer.add_document(doc);

    let reader = table.reader();
    let column_reader = reader.column_reader();
    let f1_reader = column_reader
        .typed_reader::<StringColumnReader>("f1")
        .unwrap();
    let f2_reader = column_reader
        .typed_reader::<StringColumnReader>("f2")
        .unwrap();

    assert_eq!(f1_reader.get(0), Some("f1_0"));
    assert_eq!(f2_reader.get(0), Some("f2_0"));

    assert_eq!(f1_reader.get(1), None);
    assert_eq!(f2_reader.get(1), Some(""));

    assert_eq!(f1_reader.get(2), Some("f1_2"));
    assert_eq!(f2_reader.get(2), Some(""));

    // serialize
    writer.new_segment();

    let doc = doc!(f2 => "f2_3");
    writer.add_document(doc);

    let reader = table.reader();
    let column_reader = reader.column_reader();
    let f1_reader = column_reader
        .typed_reader::<StringColumnReader>("f1")
        .unwrap();
    let f2_reader = column_reader
        .typed_reader::<StringColumnReader>("f2")
        .unwrap();

    assert_eq!(f1_reader.get(0), Some("f1_0"));
    assert_eq!(f2_reader.get(0), Some("f2_0"));

    assert_eq!(f1_reader.get(1), None);
    assert_eq!(f2_reader.get(1), Some(""));

    assert_eq!(f1_reader.get(2), Some("f1_2"));
    assert_eq!(f2_reader.get(2), Some(""));

    assert_eq!(f1_reader.get(3), None);
    assert_eq!(f2_reader.get(3), Some("f2_3"));
}

#[test]
fn test_column_i8() {
    let mut schema_builder = SchemaBuilder::new();
    schema_builder.add_field("f1".to_string(), DataType::Int8, COLUMNAR);
    schema_builder.add_field("f2".to_string(), DataType::Int8, COLUMNAR | NOT_NULL);
    let schema = schema_builder.build();
    let settings = TableSettings::new();
    let table = Table::create(schema, settings);

    let mut writer = table.writer();

    let doc = doc!(f1 => 0_i8, f2 => 0_i8);
    writer.add_document(doc);
    let doc = doc!(f1 => i8::MIN, f2 => i8::MAX);
    writer.add_document(doc);
    let doc = doc!();
    writer.add_document(doc);
    let doc = doc!(f1 => i8::MAX, f2 => i8::MIN);
    writer.add_document(doc);

    let reader = table.reader();
    let column_reader = reader.column_reader();
    let f1_reader = column_reader
        .typed_reader::<PrimitiveColumnReader<Int8Type>>("f1")
        .unwrap();
    let f2_reader = column_reader
        .typed_reader::<PrimitiveColumnReader<Int8Type>>("f2")
        .unwrap();

    assert_eq!(f1_reader.get(0), Some(0));
    assert_eq!(f2_reader.get(0), Some(0));

    assert_eq!(f1_reader.get(1), Some(i8::MIN));
    assert_eq!(f2_reader.get(1), Some(i8::MAX));

    assert_eq!(f1_reader.get(2), None);
    assert_eq!(f2_reader.get(2), Some(0));

    assert_eq!(f1_reader.get(3), Some(i8::MAX));
    assert_eq!(f2_reader.get(3), Some(i8::MIN));

    // serialize
    writer.new_segment();

    let doc = doc!(f1 => 4_i8, f2 => 5_i8);
    writer.add_document(doc);

    let reader = table.reader();
    let column_reader = reader.column_reader();
    let f1_reader = column_reader
        .typed_reader::<PrimitiveColumnReader<Int8Type>>("f1")
        .unwrap();
    let f2_reader = column_reader
        .typed_reader::<PrimitiveColumnReader<Int8Type>>("f2")
        .unwrap();

    assert_eq!(f1_reader.get(0), Some(0));
    assert_eq!(f2_reader.get(0), Some(0));

    assert_eq!(f1_reader.get(1), Some(i8::MIN));
    assert_eq!(f2_reader.get(1), Some(i8::MAX));

    assert_eq!(f1_reader.get(2), None);
    assert_eq!(f2_reader.get(2), Some(0));

    assert_eq!(f1_reader.get(3), Some(i8::MAX));
    assert_eq!(f2_reader.get(3), Some(i8::MIN));

    assert_eq!(f1_reader.get(4), Some(4));
    assert_eq!(f2_reader.get(4), Some(5));
}

#[test]
fn test_column_i16() {
    let mut schema_builder = SchemaBuilder::new();
    schema_builder.add_field("f1".to_string(), DataType::Int16, COLUMNAR);
    schema_builder.add_field("f2".to_string(), DataType::Int16, COLUMNAR | NOT_NULL);
    let schema = schema_builder.build();
    let settings = TableSettings::new();
    let table = Table::create(schema, settings);

    let mut writer = table.writer();

    let doc = doc!(f1 => 0_i16, f2 => 0_i16);
    writer.add_document(doc);
    let doc = doc!(f1 => i16::MIN, f2 => i16::MAX);
    writer.add_document(doc);
    let doc = doc!();
    writer.add_document(doc);
    let doc = doc!(f1 => i16::MAX, f2 => i16::MIN);
    writer.add_document(doc);

    let reader = table.reader();
    let column_reader = reader.column_reader();
    let f1_reader = column_reader
        .typed_reader::<PrimitiveColumnReader<Int16Type>>("f1")
        .unwrap();
    let f2_reader = column_reader
        .typed_reader::<PrimitiveColumnReader<Int16Type>>("f2")
        .unwrap();

    assert_eq!(f1_reader.get(0), Some(0));
    assert_eq!(f2_reader.get(0), Some(0));

    assert_eq!(f1_reader.get(1), Some(i16::MIN));
    assert_eq!(f2_reader.get(1), Some(i16::MAX));

    assert_eq!(f1_reader.get(2), None);
    assert_eq!(f2_reader.get(2), Some(0));

    assert_eq!(f1_reader.get(3), Some(i16::MAX));
    assert_eq!(f2_reader.get(3), Some(i16::MIN));

    writer.new_segment();

    let doc = doc!(f1 => 4_i16, f2 => 5_i16);
    writer.add_document(doc);

    let reader = table.reader();
    let column_reader = reader.column_reader();
    let f1_reader = column_reader
        .typed_reader::<PrimitiveColumnReader<Int16Type>>("f1")
        .unwrap();
    let f2_reader = column_reader
        .typed_reader::<PrimitiveColumnReader<Int16Type>>("f2")
        .unwrap();

    assert_eq!(f1_reader.get(0), Some(0));
    assert_eq!(f2_reader.get(0), Some(0));

    assert_eq!(f1_reader.get(1), Some(i16::MIN));
    assert_eq!(f2_reader.get(1), Some(i16::MAX));

    assert_eq!(f1_reader.get(2), None);
    assert_eq!(f2_reader.get(2), Some(0));

    assert_eq!(f1_reader.get(3), Some(i16::MAX));
    assert_eq!(f2_reader.get(3), Some(i16::MIN));

    assert_eq!(f1_reader.get(4), Some(4));
    assert_eq!(f2_reader.get(4), Some(5));
}

#[test]
fn test_column_i64() {
    let mut schema_builder = SchemaBuilder::new();
    schema_builder.add_field("f1".to_string(), DataType::Int64, COLUMNAR);
    schema_builder.add_field("f2".to_string(), DataType::Int64, COLUMNAR | NOT_NULL);
    let schema = schema_builder.build();
    let settings = TableSettings::new();
    let table = Table::create(schema, settings);

    let mut writer = table.writer();

    let doc = doc!(f1 => 0_i64, f2 => 0_i64);
    writer.add_document(doc);
    let doc = doc!(f1 => i64::MIN, f2 => i64::MAX);
    writer.add_document(doc);
    let doc = doc!();
    writer.add_document(doc);
    let doc = doc!(f1 => i64::MAX, f2 => i64::MIN);
    writer.add_document(doc);

    let reader = table.reader();
    let column_reader = reader.column_reader();
    let f1_reader = column_reader
        .typed_reader::<PrimitiveColumnReader<Int64Type>>("f1")
        .unwrap();
    let f2_reader = column_reader
        .typed_reader::<PrimitiveColumnReader<Int64Type>>("f2")
        .unwrap();

    assert_eq!(f1_reader.get(0), Some(0));
    assert_eq!(f2_reader.get(0), Some(0));

    assert_eq!(f1_reader.get(1), Some(i64::MIN));
    assert_eq!(f2_reader.get(1), Some(i64::MAX));

    assert_eq!(f1_reader.get(2), None);
    assert_eq!(f2_reader.get(2), Some(0));

    assert_eq!(f1_reader.get(3), Some(i64::MAX));
    assert_eq!(f2_reader.get(3), Some(i64::MIN));

    writer.new_segment();

    let doc = doc!(f1 => 4_i64, f2 => 5_i64);
    writer.add_document(doc);

    let reader = table.reader();
    let column_reader = reader.column_reader();
    let f1_reader = column_reader
        .typed_reader::<PrimitiveColumnReader<Int64Type>>("f1")
        .unwrap();
    let f2_reader = column_reader
        .typed_reader::<PrimitiveColumnReader<Int64Type>>("f2")
        .unwrap();

    assert_eq!(f1_reader.get(0), Some(0));
    assert_eq!(f2_reader.get(0), Some(0));

    assert_eq!(f1_reader.get(1), Some(i64::MIN));
    assert_eq!(f2_reader.get(1), Some(i64::MAX));

    assert_eq!(f1_reader.get(2), None);
    assert_eq!(f2_reader.get(2), Some(0));

    assert_eq!(f1_reader.get(3), Some(i64::MAX));
    assert_eq!(f2_reader.get(3), Some(i64::MIN));

    assert_eq!(f1_reader.get(4), Some(4));
    assert_eq!(f2_reader.get(4), Some(5));
}

#[test]
fn test_column_list_i8() {
    let mut schema_builder = SchemaBuilder::new();
    schema_builder.add_field("f1".to_string(), DataType::Int8, MULTI | COLUMNAR);
    schema_builder.add_field(
        "f2".to_string(),
        DataType::Int8,
        MULTI | COLUMNAR | NOT_NULL,
    );
    let schema = schema_builder.build();
    let settings = TableSettings::new();
    let table = Table::create(schema, settings);

    let mut writer = table.writer();

    let f1_0 = vec![1_i8, 3, 5];
    let f2_0 = vec![4_i8, 7];
    let doc = doc!(f1 => f1_0.clone(), f2 => f2_0.clone());
    writer.add_document(doc);
    let doc = doc!();
    writer.add_document(doc);

    let reader = table.reader();
    let column_reader = reader.column_reader();
    let f1_reader = column_reader
        .typed_reader::<ListPrimitiveColumnReader<Int8Type>>("f1")
        .unwrap();
    let f2_reader = column_reader
        .typed_reader::<ListPrimitiveColumnReader<Int8Type>>("f2")
        .unwrap();

    let f1_0_expect = Arc::new(Int8Array::from(f1_0.clone()));
    let f2_0_expect = Arc::new(Int8Array::from(f2_0.clone()));
    assert_eq!(f1_reader.get(0), Some(f1_0_expect as ArrayRef));
    assert_eq!(f2_reader.get(0), Some(f2_0_expect as ArrayRef));
    assert_eq!(f1_reader.get(1), None);
    assert_eq!(
        f2_reader.get(1),
        Some(Arc::new(Int8Array::from(Vec::<i8>::new())) as ArrayRef)
    );

    // serialize
    writer.new_segment();

    let reader = table.reader();
    let column_reader = reader.column_reader();
    let f1_reader = column_reader
        .typed_reader::<ListPrimitiveColumnReader<Int8Type>>("f1")
        .unwrap();
    let f2_reader = column_reader
        .typed_reader::<ListPrimitiveColumnReader<Int8Type>>("f2")
        .unwrap();

    let f1_0_expect = Arc::new(Int8Array::from(f1_0.clone()));
    let f2_0_expect = Arc::new(Int8Array::from(f2_0.clone()));
    assert_eq!(f1_reader.get(0), Some(f1_0_expect as ArrayRef));
    assert_eq!(f2_reader.get(0), Some(f2_0_expect as ArrayRef));
    assert_eq!(f1_reader.get(1), None);
    assert_eq!(
        f2_reader.get(1),
        Some(Arc::new(Int8Array::from(Vec::<i8>::new())) as ArrayRef)
    );
}

#[test]
fn test_column_list_i64() {
    let mut schema_builder = SchemaBuilder::new();
    schema_builder.add_field("f1".to_string(), DataType::Int64, MULTI | COLUMNAR);
    schema_builder.add_field(
        "f2".to_string(),
        DataType::Int64,
        MULTI | COLUMNAR | NOT_NULL,
    );
    let schema = schema_builder.build();
    let settings = TableSettings::new();
    let table = Table::create(schema, settings);

    let mut writer = table.writer();

    let f1_0 = vec![1_i64, 3, 5];
    let f2_0 = vec![4_i64, 7];
    let doc = doc!(f1 => f1_0.clone(), f2 => f2_0.clone());
    writer.add_document(doc);
    let doc = doc!();
    writer.add_document(doc);

    let reader = table.reader();
    let column_reader = reader.column_reader();
    let f1_reader = column_reader
        .typed_reader::<ListPrimitiveColumnReader<Int64Type>>("f1")
        .unwrap();
    let f2_reader = column_reader
        .typed_reader::<ListPrimitiveColumnReader<Int64Type>>("f2")
        .unwrap();

    let f1_0_expect = Arc::new(Int64Array::from(f1_0.clone()));
    let f2_0_expect = Arc::new(Int64Array::from(f2_0.clone()));
    assert_eq!(f1_reader.get(0), Some(f1_0_expect as ArrayRef));
    assert_eq!(f2_reader.get(0), Some(f2_0_expect as ArrayRef));
    assert_eq!(f1_reader.get(1), None);
    assert_eq!(
        f2_reader.get(1),
        Some(Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef)
    );

    // serialize
    writer.new_segment();

    let reader = table.reader();
    let column_reader = reader.column_reader();
    let f1_reader = column_reader
        .typed_reader::<ListPrimitiveColumnReader<Int64Type>>("f1")
        .unwrap();
    let f2_reader = column_reader
        .typed_reader::<ListPrimitiveColumnReader<Int64Type>>("f2")
        .unwrap();

    let f1_0_expect = Arc::new(Int64Array::from(f1_0.clone()));
    let f2_0_expect = Arc::new(Int64Array::from(f2_0.clone()));
    assert_eq!(f1_reader.get(0), Some(f1_0_expect as ArrayRef));
    assert_eq!(f2_reader.get(0), Some(f2_0_expect as ArrayRef));
    assert_eq!(f1_reader.get(1), None);
    assert_eq!(
        f2_reader.get(1),
        Some(Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef)
    );
}

#[test]
fn test_column_list_string() {
    let mut schema_builder = SchemaBuilder::new();
    schema_builder.add_field("f1".to_string(), DataType::String, MULTI | COLUMNAR);
    schema_builder.add_field(
        "f2".to_string(),
        DataType::String,
        MULTI | COLUMNAR | NOT_NULL,
    );
    let schema = schema_builder.build();
    let settings = TableSettings::new();
    let table = Table::create(schema, settings);

    let mut writer = table.writer();

    let f1_0 = vec!["hello".to_string(), "world".to_string()];
    let f2_0 = vec!["howdy".to_string()];
    let doc = doc!(f1 => f1_0.clone(), f2 => f2_0.clone());
    writer.add_document(doc);
    let doc = doc!();
    writer.add_document(doc);

    let reader = table.reader();
    let column_reader = reader.column_reader();
    let f1_reader = column_reader
        .typed_reader::<ListStringColumnReader>("f1")
        .unwrap();
    let f2_reader = column_reader
        .typed_reader::<ListStringColumnReader>("f2")
        .unwrap();

    let f1_0_expect = Arc::new(StringArray::from(f1_0.clone()));
    let f2_0_expect = Arc::new(StringArray::from(f2_0.clone()));
    assert_eq!(f1_reader.get(0), Some(f1_0_expect as ArrayRef));
    assert_eq!(f2_reader.get(0), Some(f2_0_expect as ArrayRef));
    assert_eq!(f1_reader.get(1), None);
    assert_eq!(
        f2_reader.get(1),
        Some(Arc::new(StringArray::from(Vec::<String>::new())) as ArrayRef)
    );

    // serialize
    writer.new_segment();

    let reader = table.reader();
    let column_reader = reader.column_reader();
    let f1_reader = column_reader
        .typed_reader::<ListStringColumnReader>("f1")
        .unwrap();
    let f2_reader = column_reader
        .typed_reader::<ListStringColumnReader>("f2")
        .unwrap();

    let f1_0_expect = Arc::new(StringArray::from(f1_0.clone()));
    let f2_0_expect = Arc::new(StringArray::from(f2_0.clone()));
    assert_eq!(f1_reader.get(0), Some(f1_0_expect as ArrayRef));
    assert_eq!(f2_reader.get(0), Some(f2_0_expect as ArrayRef));
    assert_eq!(f1_reader.get(1), None);
    assert_eq!(
        f2_reader.get(1),
        Some(Arc::new(StringArray::from(Vec::<String>::new())) as ArrayRef)
    );
}
