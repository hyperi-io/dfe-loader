// DFE Fork: Nested type deserializer
// Nested type is stored as parallel arrays (when flatten_nested=1)
// This reads the parallel arrays and reconstructs Nested values

use crate::{io::ClickhouseRead, values::Value, KlickhouseError, Result};

use super::{Deserializer, DeserializerState, Type};

pub struct NestedDeserializer;

impl Deserializer for NestedDeserializer {
    async fn read_prefix<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        state: &mut DeserializerState,
    ) -> Result<()> {
        match type_ {
            Type::Nested(fields) => {
                // Read prefix for each field's array type
                for inner_type in fields.values() {
                    let array_type = Type::Array(Box::new(inner_type.clone()));
                    array_type.deserialize_prefix(reader, state).await?;
                }
                Ok(())
            }
            _ => Err(KlickhouseError::TypeParseError(
                "NestedDeserializer::read_prefix called with non-Nested type".to_string(),
            )),
        }
    }

    async fn read<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        rows: usize,
        state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        match type_ {
            Type::Nested(fields) => {
                if fields.is_empty() || rows == 0 {
                    return Ok(vec![
                        Value::Nested(indexmap::IndexMap::new());
                        rows
                    ]);
                }

                let field_names: Vec<String> = fields.keys().cloned().collect();
                let field_count = field_names.len();

                // Read each field as an Array column
                let mut columns: Vec<Vec<Value>> = Vec::with_capacity(field_count);
                for inner_type in fields.values() {
                    let array_type = Type::Array(Box::new(inner_type.clone()));
                    let column = array_type.deserialize_column(reader, rows, state).await?;
                    columns.push(column);
                }

                // Reconstruct Nested values row by row
                let mut result: Vec<Value> = Vec::with_capacity(rows);
                for row_idx in 0..rows {
                    let mut nested_fields: indexmap::IndexMap<String, Vec<Value>> =
                        indexmap::IndexMap::with_capacity(field_count);

                    for (field_idx, field_name) in field_names.iter().enumerate() {
                        match &columns[field_idx][row_idx] {
                            Value::Array(arr) => {
                                nested_fields.insert(field_name.clone(), arr.clone());
                            }
                            Value::Null => {
                                nested_fields.insert(field_name.clone(), Vec::new());
                            }
                            other => {
                                return Err(KlickhouseError::TypeParseError(format!(
                                    "Expected Array for Nested field '{}', got {:?}",
                                    field_name, other
                                )));
                            }
                        }
                    }

                    result.push(Value::Nested(nested_fields));
                }

                Ok(result)
            }
            _ => Err(KlickhouseError::TypeParseError(
                "NestedDeserializer::read called with non-Nested type".to_string(),
            )),
        }
    }
}
