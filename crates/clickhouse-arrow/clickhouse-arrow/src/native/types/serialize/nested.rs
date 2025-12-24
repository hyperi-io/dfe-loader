// Project:   clickhouse-arrow (DFE fork)
// File:      nested.rs
// Purpose:   Nested type serialization for ClickHouse native protocol
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2025 HyperSec

//! Serializer for ClickHouse Nested type.
//!
//! Nested is syntactic sugar for Array(Tuple(...)).
//! For example: `Nested(a UInt32, b String)` is stored as `Array(Tuple(a UInt32, b String))`
//!
//! This means Nested doesn't need its own serialization - it uses Array + Tuple serializers.
//! However, we provide this module for documentation and potential future optimizations.
//!
//! Binary format: Same as Array(Tuple(...))
//! - Offsets: u64 array of cumulative sizes
//! - Data: Tuple columns (each field serialized as a column)
//!
//! Reference: ClickHouse/src/DataTypes/DataTypeNested.cpp

use super::{ClickHouseNativeSerializer, Serializer, SerializerState, Type};
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Error, Result, Value};

pub(crate) struct NestedSerializer;

impl Serializer for NestedSerializer {
    async fn write_prefix<W: ClickHouseWrite>(
        type_: &Type,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        let fields = match type_ {
            Type::Nested(f) => f,
            _ => {
                return Err(Error::SerializeError(format!(
                    "NestedSerializer called with non-nested type: {type_:?}"
                )));
            }
        };

        // Convert to Array(Tuple(...)) and delegate
        let tuple_type = Type::Tuple(fields.iter().map(|(_, t)| t.clone()).collect());
        let array_type = Type::Array(Box::new(tuple_type));

        array_type.serialize_prefix_async(writer, state).await
    }

    async fn write<W: ClickHouseWrite>(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        let fields = match type_ {
            Type::Nested(f) => f,
            _ => {
                return Err(Error::SerializeError(format!(
                    "NestedSerializer called with non-nested type: {type_:?}"
                )));
            }
        };

        // Convert to Array(Tuple(...)) and delegate
        let tuple_type = Type::Tuple(fields.iter().map(|(_, t)| t.clone()).collect());
        let array_type = Type::Array(Box::new(tuple_type));

        array_type.serialize_column(values, writer, state).await
    }

    fn write_sync(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut impl ClickHouseBytesWrite,
        state: &mut SerializerState,
    ) -> Result<()> {
        let fields = match type_ {
            Type::Nested(f) => f,
            _ => {
                return Err(Error::SerializeError(format!(
                    "NestedSerializer called with non-nested type: {type_:?}"
                )));
            }
        };

        // Convert to Array(Tuple(...)) and delegate
        let tuple_type = Type::Tuple(fields.iter().map(|(_, t)| t.clone()).collect());
        let array_type = Type::Array(Box::new(tuple_type));

        array_type.serialize_column_sync(values, writer, state)
    }
}

/// Convert Nested type to its equivalent Array(Tuple(...)) representation
#[allow(dead_code)]
pub(crate) fn nested_to_array_tuple(fields: &[(String, Type)]) -> Type {
    let tuple_type = Type::Tuple(fields.iter().map(|(_, t)| t.clone()).collect());
    Type::Array(Box::new(tuple_type))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nested_to_array_tuple() {
        let fields = vec![
            ("a".to_string(), Type::UInt32),
            ("b".to_string(), Type::String),
        ];

        let result = nested_to_array_tuple(&fields);

        match result {
            Type::Array(inner) => match *inner {
                Type::Tuple(types) => {
                    assert_eq!(types.len(), 2);
                    assert!(matches!(types[0], Type::UInt32));
                    assert!(matches!(types[1], Type::String));
                }
                _ => panic!("Expected Tuple"),
            },
            _ => panic!("Expected Array"),
        }
    }
}
