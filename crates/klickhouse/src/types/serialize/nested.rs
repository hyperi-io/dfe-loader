// DFE Fork: Nested type serializer
// Nested type is stored as parallel arrays (when flatten_nested=1)
// or as Array of Tuples (when flatten_nested=0)
//
// Example: Nested(name String, version UInt32)
// With flatten_nested=1: stored as two columns: `col.name Array(String)`, `col.version Array(UInt32)`
// With flatten_nested=0: stored as `col Array(Tuple(name String, version UInt32))`
//
// This implementation uses the flattened parallel arrays format.

use crate::{io::ClickhouseWrite, values::Value, KlickhouseError, Result};

use super::{Serializer, SerializerState, Type};

pub struct NestedSerializer;

impl Serializer for NestedSerializer {
    async fn write_prefix<W: ClickhouseWrite>(
        type_: &Type,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        match type_ {
            Type::Nested(fields) => {
                // Write prefix for each field's array type
                for inner_type in fields.values() {
                    let array_type = Type::Array(Box::new(inner_type.clone()));
                    array_type.serialize_prefix(writer, state).await?;
                }
                Ok(())
            }
            _ => Err(KlickhouseError::TypeParseError(
                "NestedSerializer::write_prefix called with non-Nested type".to_string(),
            )),
        }
    }

    async fn write<W: ClickhouseWrite>(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        match type_ {
            Type::Nested(fields) => {
                if fields.is_empty() {
                    return Ok(());
                }

                let field_names: Vec<&String> = fields.keys().collect();
                let field_count = field_names.len();
                let row_count = values.len();

                // Validate and extract nested values
                // Each row should have a Value::Nested with matching field names
                let mut columns: Vec<Vec<Value>> = vec![Vec::with_capacity(row_count); field_count];

                for (row_idx, value) in values.into_iter().enumerate() {
                    match value {
                        Value::Nested(nested_fields) => {
                            // Extract each field's array of values
                            for (field_idx, field_name) in field_names.iter().enumerate() {
                                if let Some(field_values) = nested_fields.get(*field_name) {
                                    // Wrap as Array value
                                    columns[field_idx]
                                        .push(Value::Array(field_values.clone()));
                                } else {
                                    return Err(KlickhouseError::TypeParseError(format!(
                                        "Nested row {} missing field '{}'",
                                        row_idx, field_name
                                    )));
                                }
                            }
                        }
                        Value::Null => {
                            // NULL nested row = empty arrays for all fields
                            for column in columns.iter_mut() {
                                column.push(Value::Array(Vec::new()));
                            }
                        }
                        other => {
                            return Err(KlickhouseError::TypeParseError(format!(
                                "Expected Nested or Null value, got {:?}",
                                other
                            )));
                        }
                    }
                }

                // First pass: write array offsets for all columns
                // All arrays in a nested row must have the same length
                // We need to verify this and write the common offsets first

                // Collect array lengths per row to verify consistency
                let mut row_lengths: Vec<usize> = Vec::with_capacity(row_count);
                for row_idx in 0..row_count {
                    let first_len = match &columns[0][row_idx] {
                        Value::Array(arr) => arr.len(),
                        _ => 0,
                    };
                    row_lengths.push(first_len);

                    // Verify all fields have same array length
                    for (field_idx, column) in columns.iter().enumerate().skip(1) {
                        let len = match &column[row_idx] {
                            Value::Array(arr) => arr.len(),
                            _ => 0,
                        };
                        if len != first_len {
                            return Err(KlickhouseError::TypeParseError(format!(
                                "Nested row {} has inconsistent array lengths: field 0 has {}, field {} has {}",
                                row_idx, first_len, field_idx, len
                            )));
                        }
                    }
                }

                // Write each field as an Array column
                for (field_idx, (_field_name, inner_type)) in fields.iter().enumerate() {
                    let array_type = Type::Array(Box::new(inner_type.clone()));
                    let column_values = std::mem::take(&mut columns[field_idx]);
                    array_type
                        .serialize_column(column_values, writer, state)
                        .await?;
                }

                Ok(())
            }
            _ => Err(KlickhouseError::TypeParseError(
                "NestedSerializer::write called with non-Nested type".to_string(),
            )),
        }
    }
}
