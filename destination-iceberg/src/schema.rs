use std::sync::Arc;

use airbyte_protocol::schema::{Compound, Format, JsonSchema, Primitive, Type};
use arrow::datatypes::{DataType, Field, Schema, SchemaBuilder, TimeUnit};

use crate::error::Error;

pub fn schema_to_arrow(schema: &JsonSchema) -> Result<Schema, Error> {
    if let Type::Compound(Compound::Object(object)) = &schema.r#type {
        let mut builder = SchemaBuilder::new();
        for (name, r#type) in &object.properties {
            let field = field_to_arrow(r#type, name);
            builder.push(field);
        }
        Ok(builder.finish())
    } else {
        Err(Error::NoSchema)
    }
}

fn field_to_arrow(datatype: &Type, name: &String) -> Field {
    match datatype {
        Type::Primitive { r#type } => {
            Field::new(trim_name(name), primitive_to_arrow(r#type), false)
        }
        Type::Single { r#type } => {
            Field::new(trim_name(name), primitive_to_arrow(&r#type[0]), false)
        }
        Type::Variant { r#type } => {
            let value = match (&r#type[0], &r#type[1]) {
                (prim, Primitive::Null) => prim,
                (Primitive::Null, prim) => prim,
                (first, _) => first,
            };
            Field::new(trim_name(name), primitive_to_arrow(value), true)
        }
        Type::PrimitiveFormat { r#type: _, format } => {
            Field::new(trim_name(name), primitivedate_to_arrow(format), false)
        }
        Type::SingleFormat { r#type: _, format } => {
            Field::new(trim_name(name), primitivedate_to_arrow(format), false)
        }
        Type::VariantFormat { r#type: _, format } => {
            Field::new(trim_name(name), primitivedate_to_arrow(format), true)
        }
        Type::Compound(Compound::Object(object)) => Field::new(
            trim_name(name),
            DataType::Struct(
                object
                    .properties
                    .iter()
                    .fold(SchemaBuilder::new(), |mut acc, (name, datatype)| {
                        acc.push(field_to_arrow(datatype, name));
                        acc
                    })
                    .finish()
                    .fields,
            ),
            true,
        ),
        Type::Compound(Compound::Array(array)) => Field::new(
            trim_name(name),
            DataType::List(Arc::new(field_to_arrow(&*array.items, name))),
            true,
        ),
        Type::Empty(_) => Field::new(trim_name(name), DataType::Null, true),
    }
}

fn trim_name(name: &str) -> &str {
    name.trim().trim_start_matches("\"").trim_end_matches("\"")
}

#[inline]
pub fn primitive_to_arrow(input: &Primitive) -> DataType {
    match input {
        Primitive::Null => DataType::Null,
        Primitive::Boolean => DataType::Boolean,
        Primitive::Integer => DataType::Int32,
        Primitive::Number => DataType::Float32,
        Primitive::String => DataType::Utf8,
    }
}

#[inline]
pub fn primitivedate_to_arrow(input: &Format) -> DataType {
    match input {
        Format::Time => DataType::Time32(TimeUnit::Millisecond),
        Format::Date => DataType::Date32,
        Format::DateTime => DataType::Timestamp(TimeUnit::Microsecond, None),
    }
}
