// Project:   clickhouse-arrow (DFE fork)
// File:      dynamic.rs
// Purpose:   Dynamic type serialization for ClickHouse native protocol
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2025 HyperSec

//! Serializer for ClickHouse Dynamic type (introduced in ClickHouse 24.x).
//!
//! Dynamic is similar to Variant but with runtime-discovered types.
//! The type information is stored in the structure prefix.
//!
//! Binary format:
//! - Structure prefix: version (u64) + num_types (varuint) + type_names...
//! - Data: variant discriminators + variant columns
//!
//! Reference: ClickHouse/src/DataTypes/Serializations/SerializationDynamic.cpp

use tokio::io::AsyncWriteExt;

use super::{Serializer, SerializerState, Type};
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Error, Result, Value};

use super::variant::NULL_DISCRIMINATOR;

/// Dynamic serialization version
const SERIALIZATION_VERSION_V2: u64 = 1;

pub(crate) struct DynamicSerializer;

impl Serializer for DynamicSerializer {
    async fn write_prefix<W: ClickHouseWrite>(
        type_: &Type,
        writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        let max_types = match type_ {
            Type::Dynamic { max_types } => max_types.unwrap_or(0),
            _ => {
                return Err(Error::SerializeError(format!(
                    "DynamicSerializer called with non-dynamic type: {type_:?}"
                )));
            }
        };

        // Write structure version (V2 by default)
        writer.write_u64_le(SERIALIZATION_VERSION_V2).await?;

        // For now, we write 0 types in prefix - types will be discovered from values
        // In practice, the types are written during the data phase
        write_varuint(writer, 0).await?;

        // Optionally write max_types hint
        if max_types > 0 {
            // This is used by ClickHouse to pre-allocate variant slots
        }

        Ok(())
    }

    async fn write<W: ClickHouseWrite>(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        let _max_types = match type_ {
            Type::Dynamic { max_types } => max_types.unwrap_or(0),
            _ => {
                return Err(Error::SerializeError(format!(
                    "DynamicSerializer called with non-dynamic type: {type_:?}"
                )));
            }
        };

        // Collect unique types and map values to discriminators
        let mut type_names: Vec<String> = Vec::new();
        let mut type_to_discr: std::collections::HashMap<String, u8> =
            std::collections::HashMap::new();
        let mut discriminators: Vec<u8> = Vec::with_capacity(values.len());
        let mut variant_columns: Vec<(String, Vec<Value>)> = Vec::new();

        for value in &values {
            match value {
                Value::Null => {
                    discriminators.push(NULL_DISCRIMINATOR);
                }
                Value::Dynamic(type_name, _inner_value) => {
                    let discr = if let Some(&d) = type_to_discr.get(type_name) {
                        d
                    } else {
                        let d = type_names.len() as u8;
                        type_names.push(type_name.clone());
                        let _ = type_to_discr.insert(type_name.clone(), d);
                        variant_columns.push((type_name.clone(), Vec::new()));
                        d
                    };
                    discriminators.push(discr);
                }
                _ => {
                    // Infer type name from value
                    let type_name = infer_type_name(value);
                    let discr = if let Some(&d) = type_to_discr.get(&type_name) {
                        d
                    } else {
                        let d = type_names.len() as u8;
                        type_names.push(type_name.clone());
                        let _ = type_to_discr.insert(type_name.clone(), d);
                        variant_columns.push((type_name, Vec::new()));
                        d
                    };
                    discriminators.push(discr);
                }
            }
        }

        // Build variant columns with actual values
        for value in values {
            match value {
                Value::Null => {}
                Value::Dynamic(type_name, inner_value) => {
                    let discr = type_to_discr[&type_name] as usize;
                    variant_columns[discr].1.push(*inner_value);
                }
                _ => {
                    let type_name = infer_type_name(&value);
                    let discr = type_to_discr[&type_name] as usize;
                    variant_columns[discr].1.push(value);
                }
            }
        }

        // Write number of discovered types
        write_varuint(writer, type_names.len() as u64).await?;

        // Write type names
        for type_name in &type_names {
            write_string(writer, type_name).await?;
        }

        // Write discriminators
        writer.write_all(&discriminators).await?;

        // Write each variant's values
        for (_type_name, column_values) in variant_columns {
            if !column_values.is_empty() {
                // For Dynamic, we need to serialize values based on their actual types
                // This is a simplified implementation - full impl needs type inference
                for value in column_values {
                    serialize_value_async(value, writer).await?;
                }
            }
        }

        Ok(())
    }

    fn write_sync(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut impl ClickHouseBytesWrite,
        _state: &mut SerializerState,
    ) -> Result<()> {
        let _max_types = match type_ {
            Type::Dynamic { max_types } => max_types.unwrap_or(0),
            _ => {
                return Err(Error::SerializeError(format!(
                    "DynamicSerializer called with non-dynamic type: {type_:?}"
                )));
            }
        };

        // Collect unique types and map values to discriminators
        let mut type_names: Vec<String> = Vec::new();
        let mut type_to_discr: std::collections::HashMap<String, u8> =
            std::collections::HashMap::new();
        let mut discriminators: Vec<u8> = Vec::with_capacity(values.len());
        let mut variant_columns: Vec<(String, Vec<Value>)> = Vec::new();

        for value in &values {
            match value {
                Value::Null => {
                    discriminators.push(NULL_DISCRIMINATOR);
                }
                Value::Dynamic(type_name, _inner_value) => {
                    let discr = if let Some(&d) = type_to_discr.get(type_name) {
                        d
                    } else {
                        let d = type_names.len() as u8;
                        type_names.push(type_name.clone());
                        let _ = type_to_discr.insert(type_name.clone(), d);
                        variant_columns.push((type_name.clone(), Vec::new()));
                        d
                    };
                    discriminators.push(discr);
                }
                _ => {
                    let type_name = infer_type_name(value);
                    let discr = if let Some(&d) = type_to_discr.get(&type_name) {
                        d
                    } else {
                        let d = type_names.len() as u8;
                        type_names.push(type_name.clone());
                        let _ = type_to_discr.insert(type_name.clone(), d);
                        variant_columns.push((type_name, Vec::new()));
                        d
                    };
                    discriminators.push(discr);
                }
            }
        }

        // Build variant columns with actual values
        for value in values {
            match value {
                Value::Null => {}
                Value::Dynamic(type_name, inner_value) => {
                    let discr = type_to_discr[&type_name] as usize;
                    variant_columns[discr].1.push(*inner_value);
                }
                _ => {
                    let type_name = infer_type_name(&value);
                    let discr = type_to_discr[&type_name] as usize;
                    variant_columns[discr].1.push(value);
                }
            }
        }

        // Write number of discovered types
        write_varuint_sync(writer, type_names.len() as u64);

        // Write type names
        for type_name in &type_names {
            write_string_sync(writer, type_name);
        }

        // Write discriminators
        writer.put_slice(&discriminators);

        // Write each variant's values
        for (_type_name, column_values) in variant_columns {
            if !column_values.is_empty() {
                for value in column_values {
                    serialize_value_sync(value, writer);
                }
            }
        }

        Ok(())
    }
}

/// Infer ClickHouse type name from a Value
fn infer_type_name(value: &Value) -> String {
    match value {
        Value::Int8(_) => "Int8".to_string(),
        Value::Int16(_) => "Int16".to_string(),
        Value::Int32(_) => "Int32".to_string(),
        Value::Int64(_) => "Int64".to_string(),
        Value::Int128(_) => "Int128".to_string(),
        Value::Int256(_) => "Int256".to_string(),
        Value::UInt8(_) => "UInt8".to_string(),
        Value::UInt16(_) => "UInt16".to_string(),
        Value::UInt32(_) => "UInt32".to_string(),
        Value::UInt64(_) => "UInt64".to_string(),
        Value::UInt128(_) => "UInt128".to_string(),
        Value::UInt256(_) => "UInt256".to_string(),
        Value::Float32(_) => "Float32".to_string(),
        Value::Float64(_) => "Float64".to_string(),
        Value::String(_) => "String".to_string(),
        Value::Uuid(_) => "UUID".to_string(),
        Value::Date(_) => "Date".to_string(),
        Value::Date32(_) => "Date32".to_string(),
        Value::DateTime(_) => "DateTime".to_string(),
        Value::DateTime64(dt) => format!("DateTime64({})", dt.2), // dt.2 is precision
        Value::Enum8(_, _) => "Enum8".to_string(),
        Value::Enum16(_, _) => "Enum16".to_string(),
        Value::Array(_) => "Array(Dynamic)".to_string(),
        Value::Tuple(_) => "Tuple(Dynamic)".to_string(),
        Value::Map(_, _) => "Map(Dynamic, Dynamic)".to_string(),
        Value::Null => "Nullable(Nothing)".to_string(),
        Value::Ipv4(_) => "IPv4".to_string(),
        Value::Ipv6(_) => "IPv6".to_string(),
        Value::Point(_) => "Point".to_string(),
        Value::Ring(_) => "Ring".to_string(),
        Value::Polygon(_) => "Polygon".to_string(),
        Value::MultiPolygon(_) => "MultiPolygon".to_string(),
        Value::Object(_) => "Object('json')".to_string(),
        Value::Decimal32(scale, _) => format!("Decimal32({})", scale),
        Value::Decimal64(scale, _) => format!("Decimal64({})", scale),
        Value::Decimal128(scale, _) => format!("Decimal128({})", scale),
        Value::Decimal256(scale, _) => format!("Decimal256({})", scale),
        Value::Variant(_, _) => "Variant".to_string(),
        Value::Dynamic(type_name, _) => type_name.clone(),
        // DFE Fork: Additional types
        Value::BFloat16(_) => "BFloat16".to_string(),
        Value::Time(_) => "Time".to_string(),
        Value::Time64(precision, _) => format!("Time64({precision})"),
        Value::AggregateFunction(_) => "AggregateFunction".to_string(),
        Value::SimpleAggregateFunction(inner) => {
            format!("SimpleAggregateFunction({})", infer_type_name(inner))
        }
    }
}

/// Write variable-length unsigned integer (async)
async fn write_varuint<W: ClickHouseWrite>(writer: &mut W, mut value: u64) -> Result<()> {
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

/// Write variable-length unsigned integer (sync)
fn write_varuint_sync(writer: &mut impl ClickHouseBytesWrite, mut value: u64) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        writer.put_u8(byte);
        if value == 0 {
            break;
        }
    }
}

/// Write string with length prefix (async)
async fn write_string<W: ClickHouseWrite>(writer: &mut W, s: &str) -> Result<()> {
    write_varuint(writer, s.len() as u64).await?;
    writer.write_all(s.as_bytes()).await?;
    Ok(())
}

/// Write string with length prefix (sync)
fn write_string_sync(writer: &mut impl ClickHouseBytesWrite, s: &str) {
    write_varuint_sync(writer, s.len() as u64);
    writer.put_slice(s.as_bytes());
}

/// Serialize a single value (async) - simplified implementation
async fn serialize_value_async<W: ClickHouseWrite>(value: Value, writer: &mut W) -> Result<()> {
    // This is a simplified implementation
    // Full implementation would use the type system to serialize properly
    match value {
        Value::Int8(v) => writer.write_i8(v).await?,
        Value::Int16(v) => writer.write_i16_le(v).await?,
        Value::Int32(v) => writer.write_i32_le(v).await?,
        Value::Int64(v) => writer.write_i64_le(v).await?,
        Value::UInt8(v) => writer.write_u8(v).await?,
        Value::UInt16(v) => writer.write_u16_le(v).await?,
        Value::UInt32(v) => writer.write_u32_le(v).await?,
        Value::UInt64(v) => writer.write_u64_le(v).await?,
        Value::Float32(v) => writer.write_f32_le(v).await?,
        Value::Float64(v) => writer.write_f64_le(v).await?,
        Value::String(v) => {
            write_varuint(writer, v.len() as u64).await?;
            writer.write_all(&v).await?;
        }
        _ => {
            return Err(Error::SerializeError(format!(
                "Unsupported value type in Dynamic: {:?}",
                value
            )));
        }
    }
    Ok(())
}

/// Serialize a single value (sync) - simplified implementation
fn serialize_value_sync(value: Value, writer: &mut impl ClickHouseBytesWrite) {
    match value {
        Value::Int8(v) => writer.put_i8(v),
        Value::Int16(v) => writer.put_i16_le(v),
        Value::Int32(v) => writer.put_i32_le(v),
        Value::Int64(v) => writer.put_i64_le(v),
        Value::UInt8(v) => writer.put_u8(v),
        Value::UInt16(v) => writer.put_u16_le(v),
        Value::UInt32(v) => writer.put_u32_le(v),
        Value::UInt64(v) => writer.put_u64_le(v),
        Value::Float32(v) => writer.put_f32_le(v),
        Value::Float64(v) => writer.put_f64_le(v),
        Value::String(v) => {
            write_varuint_sync(writer, v.len() as u64);
            writer.put_slice(&v);
        }
        _ => {
            // Skip unsupported types for now
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infer_type_name() {
        assert_eq!(infer_type_name(&Value::Int64(42)), "Int64");
        assert_eq!(infer_type_name(&Value::String(vec![])), "String");
        assert_eq!(infer_type_name(&Value::Float64(3.14)), "Float64");
    }

    #[test]
    fn test_varuint_encoding() {
        let mut buf = bytes::BytesMut::new();
        write_varuint_sync(&mut buf, 0);
        assert_eq!(&buf[..], &[0]);

        buf.clear();
        write_varuint_sync(&mut buf, 127);
        assert_eq!(&buf[..], &[127]);

        buf.clear();
        write_varuint_sync(&mut buf, 128);
        assert_eq!(&buf[..], &[0x80, 0x01]);

        buf.clear();
        write_varuint_sync(&mut buf, 300);
        assert_eq!(&buf[..], &[0xAC, 0x02]);
    }
}
