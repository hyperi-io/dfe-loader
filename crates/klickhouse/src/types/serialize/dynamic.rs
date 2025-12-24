// DFE Fork: Dynamic type serializer
// Dynamic type can hold any value type, with type info stored per-value
// Based on ClickHouse SerializationDynamic.cpp

use tokio::io::AsyncWriteExt;

use crate::{io::ClickhouseWrite, values::Value, KlickhouseError, Result};

use super::{Serializer, SerializerState, Type};

/// Dynamic serialization structure version
#[repr(u8)]
pub enum DynamicStructureVersion {
    /// Initial version
    V1 = 1,
}

pub struct DynamicSerializer;

impl Serializer for DynamicSerializer {
    async fn write_prefix<W: ClickhouseWrite>(
        type_: &Type,
        writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        match type_ {
            Type::Dynamic { .. } => {
                // Write structure version
                writer.write_u64_le(DynamicStructureVersion::V1 as u64).await?;
                // Write variant count (0 for prefix - actual types come with data)
                writer.write_u64_le(0).await?;
                Ok(())
            }
            _ => Err(KlickhouseError::TypeParseError(
                "DynamicSerializer::write_prefix called with non-Dynamic type".to_string(),
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
            Type::Dynamic { max_types } => {
                let max = max_types.unwrap_or(32);

                if values.is_empty() {
                    return Ok(());
                }

                // Group values by their type and collect type signatures
                let mut type_map: indexmap::IndexMap<String, (Type, Vec<(usize, Value)>)> =
                    indexmap::IndexMap::new();
                let mut null_indices: Vec<usize> = Vec::new();

                for (i, value) in values.iter().enumerate() {
                    match value {
                        Value::Null => {
                            null_indices.push(i);
                        }
                        Value::Dynamic(inner_type, inner_value) => {
                            let type_name = inner_type.to_string();
                            type_map
                                .entry(type_name)
                                .or_insert_with(|| ((**inner_type).clone(), Vec::new()))
                                .1
                                .push((i, (**inner_value).clone()));
                        }
                        other => {
                            // Auto-wrap non-Dynamic values
                            let inner_type = other.guess_type();
                            let type_name = inner_type.to_string();
                            type_map
                                .entry(type_name)
                                .or_insert_with(|| (inner_type, Vec::new()))
                                .1
                                .push((i, other.clone()));
                        }
                    }
                }

                // Check if we exceed max_types limit
                if type_map.len() > max {
                    return Err(KlickhouseError::TypeParseError(format!(
                        "Dynamic column has {} distinct types, exceeds max_types={}",
                        type_map.len(),
                        max
                    )));
                }

                let row_count = values.len();
                let type_count = type_map.len();

                // Build discriminator array and collect type info
                // 255 = NULL, 0..n-1 = type index
                let mut discriminators: Vec<u8> = vec![255; row_count];
                let mut types: Vec<&Type> = Vec::with_capacity(type_count);
                let mut type_values: Vec<Vec<Value>> = Vec::with_capacity(type_count);

                for (type_idx, (_type_name, (inner_type, indexed_values))) in
                    type_map.iter().enumerate()
                {
                    types.push(inner_type);
                    let mut vals = Vec::with_capacity(indexed_values.len());
                    for (row_idx, value) in indexed_values {
                        discriminators[*row_idx] = type_idx as u8;
                        vals.push(value.clone());
                    }
                    type_values.push(vals);
                }

                // Write discriminators
                for d in &discriminators {
                    writer.write_u8(*d).await?;
                }

                // Write each type's name and values
                for (i, inner_type) in types.iter().enumerate() {
                    // Write type name as string
                    let type_name = inner_type.to_string();
                    let type_bytes = type_name.as_bytes();
                    write_varint(writer, type_bytes.len() as u64).await?;
                    writer.write_all(type_bytes).await?;

                    // Write values for this type
                    let vals = std::mem::take(&mut type_values[i]);
                    inner_type.serialize_column(vals, writer, state).await?;
                }

                Ok(())
            }
            _ => Err(KlickhouseError::TypeParseError(
                "DynamicSerializer::write called with non-Dynamic type".to_string(),
            )),
        }
    }
}

/// Write a variable-length integer (LEB128-like encoding)
async fn write_varint<W: ClickhouseWrite>(writer: &mut W, mut value: u64) -> Result<()> {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        writer.write_u8(byte).await?;
        if value == 0 {
            break;
        }
    }
    Ok(())
}
