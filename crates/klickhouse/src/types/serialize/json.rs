// DFE Fork: JSON type serializer
// JSON type is stored as paths flattened to subcolumns with Dynamic types
// For serialization over the wire, we use the simplified String-based format
// that ClickHouse added for client compatibility (PR #70312)

use crate::{io::ClickhouseWrite, values::Value, KlickhouseError, Result};

use super::{Serializer, SerializerState, Type};

pub struct JsonSerializer;

impl Serializer for JsonSerializer {
    async fn write_prefix<W: ClickhouseWrite>(
        type_: &Type,
        _writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        match type_ {
            Type::Json { .. } => {
                // JSON uses String serialization format for wire protocol
                // No special prefix needed
                Ok(())
            }
            _ => Err(KlickhouseError::TypeParseError(
                "JsonSerializer::write_prefix called with non-Json type".to_string(),
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
            Type::Json { .. } => {
                // Serialize JSON values as strings (Vec<u8>)
                // ClickHouse accepts JSON columns as String in native format
                let mut string_values: Vec<Value> = Vec::with_capacity(values.len());

                for value in values {
                    match value {
                        Value::Null => {
                            // NULL JSON is serialized as "null" string
                            string_values.push(Value::String(b"null".to_vec()));
                        }
                        #[cfg(feature = "serde")]
                        Value::Json(json_value) => {
                            // Serialize serde_json::Value to bytes
                            let json_bytes = serde_json::to_vec(&*json_value).map_err(|e| {
                                KlickhouseError::TypeParseError(format!(
                                    "Failed to serialize JSON value: {}",
                                    e
                                ))
                            })?;
                            string_values.push(Value::String(json_bytes));
                        }
                        Value::String(s) => {
                            // Already a byte string, assume it's valid JSON
                            string_values.push(Value::String(s));
                        }
                        other => {
                            return Err(KlickhouseError::TypeParseError(format!(
                                "Cannot serialize {:?} as JSON",
                                other
                            )));
                        }
                    }
                }

                // Use String serialization
                Type::String
                    .serialize_column(string_values, writer, state)
                    .await
            }
            _ => Err(KlickhouseError::TypeParseError(
                "JsonSerializer::write called with non-Json type".to_string(),
            )),
        }
    }
}
