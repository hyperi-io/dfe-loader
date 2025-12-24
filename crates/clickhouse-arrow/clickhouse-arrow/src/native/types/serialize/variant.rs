// Project:   clickhouse-arrow (DFE fork)
// File:      variant.rs
// Purpose:   Variant type serialization for ClickHouse native protocol
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2025 HyperSec

//! Serializer for ClickHouse Variant type (introduced in ClickHouse 24.x).
//!
//! Binary format:
//! - Row-by-row: discriminator (u8) + variant_data (if not NULL)
//! - Discriminator 255 = NULL_DISCRIMINATOR (null value)
//! - Discriminators 0-254 map to variant types in order
//!
//! Reference: ClickHouse/src/DataTypes/Serializations/SerializationVariant.cpp

use tokio::io::AsyncWriteExt;

use super::{ClickHouseNativeSerializer, Serializer, SerializerState, Type};
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Error, Result, Value};

/// NULL discriminator value (255) - indicates NULL value in Variant
pub(crate) const NULL_DISCRIMINATOR: u8 = 255;

pub(crate) struct VariantSerializer;

impl Serializer for VariantSerializer {
    async fn write_prefix<W: ClickHouseWrite>(
        type_: &Type,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        let variants = match type_ {
            Type::Variant(v) => v,
            _ => {
                return Err(Error::SerializeError(format!(
                    "VariantSerializer called with non-variant type: {type_:?}"
                )));
            }
        };

        // Each variant type may need its own prefix
        for variant_type in variants {
            variant_type.serialize_prefix_async(writer, state).await?;
        }
        Ok(())
    }

    async fn write<W: ClickHouseWrite>(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        let variant_types = match type_ {
            Type::Variant(v) => v,
            _ => {
                return Err(Error::SerializeError(format!(
                    "VariantSerializer called with non-variant type: {type_:?}"
                )));
            }
        };

        // Collect discriminators and per-variant values
        let mut discriminators: Vec<u8> = Vec::with_capacity(values.len());
        let mut variant_columns: Vec<Vec<Value>> =
            vec![Vec::new(); variant_types.len()];

        for value in values {
            match value {
                Value::Null => {
                    discriminators.push(NULL_DISCRIMINATOR);
                }
                Value::Variant(discr, inner_value) => {
                    if discr as usize >= variant_types.len() {
                        return Err(Error::SerializeError(format!(
                            "Variant discriminator {} out of range (max {})",
                            discr,
                            variant_types.len() - 1
                        )));
                    }
                    discriminators.push(discr);
                    variant_columns[discr as usize].push(*inner_value);
                }
                _ => {
                    // Try to match the value to a variant type
                    let discr = find_matching_variant(&value, variant_types)?;
                    discriminators.push(discr);
                    variant_columns[discr as usize].push(value);
                }
            }
        }

        // Write discriminators column (basic mode - row by row)
        writer.write_all(&discriminators).await?;

        // Write each variant's values
        for (i, variant_type) in variant_types.iter().enumerate() {
            let column_values = std::mem::take(&mut variant_columns[i]);
            if !column_values.is_empty() {
                variant_type
                    .serialize_column(column_values, writer, state)
                    .await?;
            }
        }

        Ok(())
    }

    fn write_sync(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut impl ClickHouseBytesWrite,
        state: &mut SerializerState,
    ) -> Result<()> {
        let variant_types = match type_ {
            Type::Variant(v) => v,
            _ => {
                return Err(Error::SerializeError(format!(
                    "VariantSerializer called with non-variant type: {type_:?}"
                )));
            }
        };

        // Collect discriminators and per-variant values
        let mut discriminators: Vec<u8> = Vec::with_capacity(values.len());
        let mut variant_columns: Vec<Vec<Value>> =
            vec![Vec::new(); variant_types.len()];

        for value in values {
            match value {
                Value::Null => {
                    discriminators.push(NULL_DISCRIMINATOR);
                }
                Value::Variant(discr, inner_value) => {
                    if discr as usize >= variant_types.len() {
                        return Err(Error::SerializeError(format!(
                            "Variant discriminator {} out of range (max {})",
                            discr,
                            variant_types.len() - 1
                        )));
                    }
                    discriminators.push(discr);
                    variant_columns[discr as usize].push(*inner_value);
                }
                _ => {
                    // Try to match the value to a variant type
                    let discr = find_matching_variant(&value, variant_types)?;
                    discriminators.push(discr);
                    variant_columns[discr as usize].push(value);
                }
            }
        }

        // Write discriminators column (basic mode - row by row)
        writer.put_slice(&discriminators);

        // Write each variant's values
        for (i, variant_type) in variant_types.iter().enumerate() {
            let column_values = std::mem::take(&mut variant_columns[i]);
            if !column_values.is_empty() {
                variant_type.serialize_column_sync(column_values, writer, state)?;
            }
        }

        Ok(())
    }
}

/// Find the matching variant type for a value.
/// Returns the discriminator index if found.
fn find_matching_variant(value: &Value, variant_types: &[Type]) -> Result<u8> {
    for (i, variant_type) in variant_types.iter().enumerate() {
        if value_matches_type(value, variant_type) {
            return Ok(i as u8);
        }
    }
    Err(Error::SerializeError(format!(
        "Value {:?} does not match any variant type",
        value
    )))
}

/// Check if a value matches a type.
fn value_matches_type(value: &Value, type_: &Type) -> bool {
    match (value, type_) {
        (Value::Int8(_), Type::Int8) => true,
        (Value::Int16(_), Type::Int16) => true,
        (Value::Int32(_), Type::Int32) => true,
        (Value::Int64(_), Type::Int64) => true,
        (Value::Int128(_), Type::Int128) => true,
        (Value::Int256(_), Type::Int256) => true,
        (Value::UInt8(_), Type::UInt8) => true,
        (Value::UInt16(_), Type::UInt16) => true,
        (Value::UInt32(_), Type::UInt32) => true,
        (Value::UInt64(_), Type::UInt64) => true,
        (Value::UInt128(_), Type::UInt128) => true,
        (Value::UInt256(_), Type::UInt256) => true,
        (Value::Float32(_), Type::Float32) => true,
        (Value::Float64(_), Type::Float64) => true,
        (Value::String(_), Type::String | Type::FixedSizedString(_)) => true,
        (Value::Uuid(_), Type::Uuid) => true,
        (Value::Date(_), Type::Date) => true,
        (Value::Date32(_), Type::Date32) => true,
        (Value::DateTime(_), Type::DateTime(_)) => true,
        (Value::DateTime64(_), Type::DateTime64(_, _)) => true,
        (Value::Array(_), Type::Array(_)) => true,
        (Value::Tuple(_), Type::Tuple(_)) => true,
        (Value::Map(_, _), Type::Map(_, _)) => true,
        (Value::Null, Type::Nullable(_)) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_discriminator() {
        assert_eq!(NULL_DISCRIMINATOR, 255);
    }

    #[test]
    fn test_value_matches_type() {
        assert!(value_matches_type(&Value::Int32(42), &Type::Int32));
        assert!(value_matches_type(&Value::String(vec![]), &Type::String));
        assert!(!value_matches_type(&Value::Int32(42), &Type::String));
    }

    #[test]
    fn test_find_matching_variant() {
        let variants = vec![Type::String, Type::Int64, Type::Float64];

        let discr = find_matching_variant(&Value::String(b"test".to_vec()), &variants).unwrap();
        assert_eq!(discr, 0);

        let discr = find_matching_variant(&Value::Int64(42), &variants).unwrap();
        assert_eq!(discr, 1);

        let discr = find_matching_variant(&Value::Float64(3.14), &variants).unwrap();
        assert_eq!(discr, 2);
    }

    #[test]
    fn test_variant_serialization_basic() {
        use bytes::BytesMut;

        // Variant(Float64, Int64, String) - matching ClickHouse's alphabetical ordering
        let variant_type = Type::Variant(vec![Type::Float64, Type::Int64, Type::String]);

        let values = vec![
            Value::String(b"hello".to_vec()), // discriminator 2 (String)
            Value::Int64(42),                  // discriminator 1 (Int64)
            Value::Float64(3.14),              // discriminator 0 (Float64)
            Value::Null,                       // discriminator 255
        ];

        let mut buf = BytesMut::new();
        let mut state = SerializerState::default();

        let result = VariantSerializer::write_sync(&variant_type, values, &mut buf, &mut state);
        assert!(result.is_ok(), "Serialization failed: {:?}", result);

        // Verify discriminators are written first
        // Row 1: String (discr 2), Row 2: Int64 (discr 1), Row 3: Float64 (discr 0), Row 4: NULL (discr 255)
        assert_eq!(buf[0], 2, "First discriminator should be 2 (String)");
        assert_eq!(buf[1], 1, "Second discriminator should be 1 (Int64)");
        assert_eq!(buf[2], 0, "Third discriminator should be 0 (Float64)");
        assert_eq!(buf[3], 255, "Fourth discriminator should be 255 (NULL)");

        // After discriminators, variant data follows (Float64, then Int64, then String)
        // Float64: 3.14 as f64 little-endian (8 bytes)
        // Int64: 42 as i64 little-endian (8 bytes)
        // String: length-prefixed "hello" (1 byte len + 5 bytes)
    }

    #[test]
    fn test_variant_value_explicit() {
        use bytes::BytesMut;

        // Test using explicit Value::Variant
        let variant_type = Type::Variant(vec![Type::Int64, Type::String]);

        let values = vec![
            Value::Variant(0, Box::new(Value::Int64(100))),
            Value::Variant(1, Box::new(Value::String(b"test".to_vec()))),
        ];

        let mut buf = BytesMut::new();
        let mut state = SerializerState::default();

        let result = VariantSerializer::write_sync(&variant_type, values, &mut buf, &mut state);
        assert!(result.is_ok());

        // Discriminators: 0, 1
        assert_eq!(buf[0], 0);
        assert_eq!(buf[1], 1);
    }
}
