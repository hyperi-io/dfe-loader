// DFE Fork: Dynamic type deserializer
// Dynamic type can hold any value type, with type info stored per-value

use std::str::FromStr;
use tokio::io::AsyncReadExt;

use crate::{io::ClickhouseRead, values::Value, KlickhouseError, Result};

use super::{Deserializer, DeserializerState, Type};

/// NULL discriminator value
const NULL_DISCRIMINATOR: u8 = 255;

pub struct DynamicDeserializer;

impl Deserializer for DynamicDeserializer {
    async fn read_prefix<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        _state: &mut DeserializerState,
    ) -> Result<()> {
        match type_ {
            Type::Dynamic { .. } => {
                // Read structure version
                let _version = reader.read_u64_le().await?;
                // Read variant count in prefix (usually 0, actual types come with data)
                let _variant_count = reader.read_u64_le().await?;
                Ok(())
            }
            _ => Err(KlickhouseError::TypeParseError(
                "DynamicDeserializer::read_prefix called with non-Dynamic type".to_string(),
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
            Type::Dynamic { .. } => {
                if rows == 0 {
                    return Ok(Vec::new());
                }

                // Read discriminators
                let mut discriminators: Vec<u8> = Vec::with_capacity(rows);
                for _ in 0..rows {
                    discriminators.push(reader.read_u8().await?);
                }

                // Count values per type index
                let mut max_type_idx: Option<u8> = None;
                let mut type_counts: std::collections::HashMap<u8, usize> =
                    std::collections::HashMap::new();

                for d in &discriminators {
                    if *d != NULL_DISCRIMINATOR {
                        *type_counts.entry(*d).or_insert(0) += 1;
                        max_type_idx = Some(max_type_idx.map_or(*d, |m| m.max(*d)));
                    }
                }

                // Read type info and values for each type index
                let type_count = max_type_idx.map_or(0, |m| (m + 1) as usize);
                let mut types: Vec<Type> = Vec::with_capacity(type_count);
                let mut type_values: Vec<Vec<Value>> = Vec::with_capacity(type_count);

                for i in 0..type_count {
                    // Read type name
                    let type_name_len = read_varint(reader).await? as usize;
                    let mut type_name_bytes = vec![0u8; type_name_len];
                    reader.read_exact(&mut type_name_bytes).await?;
                    let type_name = String::from_utf8(type_name_bytes).map_err(|e| {
                        KlickhouseError::TypeParseError(format!("Invalid type name UTF-8: {}", e))
                    })?;

                    // Parse the type
                    let inner_type = Type::from_str(&type_name)?;
                    types.push(inner_type.clone());

                    // Read values for this type
                    let count = *type_counts.get(&(i as u8)).unwrap_or(&0);
                    if count > 0 {
                        let values = inner_type.deserialize_column(reader, count, state).await?;
                        type_values.push(values);
                    } else {
                        type_values.push(Vec::new());
                    }
                }

                // Track position in each type's value vector
                let mut type_positions: Vec<usize> = vec![0; type_count];

                // Reconstruct output values in row order
                let mut result: Vec<Value> = Vec::with_capacity(rows);
                for d in discriminators {
                    if d == NULL_DISCRIMINATOR {
                        result.push(Value::Null);
                    } else {
                        let idx = d as usize;
                        if idx >= types.len() {
                            return Err(KlickhouseError::TypeParseError(format!(
                                "Invalid Dynamic discriminator {} (only {} types)",
                                d,
                                types.len()
                            )));
                        }
                        let pos = type_positions[idx];
                        if pos >= type_values[idx].len() {
                            return Err(KlickhouseError::TypeParseError(format!(
                                "Not enough values for Dynamic type {} at position {}",
                                idx, pos
                            )));
                        }
                        let value = type_values[idx][pos].clone();
                        type_positions[idx] += 1;
                        result.push(Value::Dynamic(
                            Box::new(types[idx].clone()),
                            Box::new(value),
                        ));
                    }
                }

                Ok(result)
            }
            _ => Err(KlickhouseError::TypeParseError(
                "DynamicDeserializer::read called with non-Dynamic type".to_string(),
            )),
        }
    }
}

/// Read a variable-length integer (LEB128-like encoding)
async fn read_varint<R: ClickhouseRead>(reader: &mut R) -> Result<u64> {
    let mut result: u64 = 0;
    let mut shift = 0;
    loop {
        let byte = reader.read_u8().await?;
        result |= ((byte & 0x7F) as u64) << shift;
        if (byte & 0x80) == 0 {
            break;
        }
        shift += 7;
        if shift >= 64 {
            return Err(KlickhouseError::TypeParseError(
                "Varint overflow in Dynamic deserialization".to_string(),
            ));
        }
    }
    Ok(result)
}
