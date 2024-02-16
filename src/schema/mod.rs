mod facet;
// mod facet_options;
mod schema;

pub(crate) use facet::FACET_SEP_BYTE;
pub use facet::{Facet, FacetParseError};
// pub use facet_options::FacetOptions;
pub use schema::{
    Field, FieldRef, FieldType, Index, IndexRef, IndexType, Schema, SchemaBuilder, SchemaRef,
    TextIndexOptions, COLUMN, DEFAULT, INDEXED, MULTI, PRIMARY_KEY,
};
