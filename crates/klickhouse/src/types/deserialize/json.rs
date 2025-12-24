// DFE Fork: JSON type deserializer
// JSON type is deserialized from String format (simplified wire protocol)

use crate::{io::ClickhouseRead, values::Value, KlickhouseError, Result};

use super::{Deserializer, DeserializerState, Type};

pub struct JsonDeserializer;

impl Deserializer for JsonDeserializer {
    async fn read_prefix<R: ClickhouseRead>(
        type_: &Type,
        _reader: &mut R,
        _state: &mut DeserializerState,
    ) -> Result<()> {
        match type_ {
            Type::Json { .. } => {
                // JSON uses String serialization format for wire protocol
                // No special prefix to read
                Ok(())
            }
            _ => Err(KlickhouseError::TypeParseError(
                "JsonDeserializer::read_prefix called with non-Json type".to_string(),
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
            Type::Json { .. } => {
                // Read as String column
                let string_values = Type::String.deserialize_column(reader, rows, state).await?;

                // Parse each string as JSON
                let mut result: Vec<Value> = Vec::with_capacity(rows);
                for value in string_values {
                    match value {
                        Value::String(s) => {
                            #[cfg(feature = "serde")]
                            {
                                // Parse the JSON bytes
                                if s.is_empty() || s.as_slice() == b"null" {
                                    result.push(Value::Null);
                                } else {
                                    let json_value: serde_json::Value =
                                        serde_json::from_slice(&s).map_err(|e| {
                                            KlickhouseError::TypeParseError(format!(
                                                "Failed to parse JSON: {}",
                                                e
                                            ))
                                        })?;
                                    result.push(Value::Json(Box::new(json_value)));
                                }
                            }
                            #[cfg(not(feature = "serde"))]
                            {
                                // Without serde, just return as string
                                result.push(Value::String(s));
                            }
                        }
                        Value::Null => {
                            result.push(Value::Null);
                        }
                        other => {
                            return Err(KlickhouseError::TypeParseError(format!(
                                "Expected String for JSON deserialization, got {:?}",
                                other
                            )));
                        }
                    }
                }

                Ok(result)
            }
            _ => Err(KlickhouseError::TypeParseError(
                "JsonDeserializer::read called with non-Json type".to_string(),
            )),
        }
    }
}
