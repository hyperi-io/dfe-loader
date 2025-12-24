// DFE Fork: Variant type serializer
// Variant is a discriminated union of types, stored as:
// - Discriminator column (UInt8 per row, 255 = NULL)
// - Value columns for each variant type (sparse/dense depending on mode)

use tokio::io::AsyncWriteExt;

use crate::{io::ClickhouseWrite, values::Value, KlickhouseError, Result};

use super::{Serializer, SerializerState, Type};

/// NULL discriminator value (special sentinel)
pub const NULL_DISCRIMINATOR: u8 = 255;

pub struct VariantSerializer;

impl Serializer for VariantSerializer {
    async fn write_prefix<W: ClickhouseWrite>(
        type_: &Type,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        let variants = match type_ {
            Type::Variant(v) => v,
            _ => {
                return Err(KlickhouseError::TypeParseError(
                    "VariantSerializer::write_prefix called with non-Variant type".to_string(),
                ))
            }
        };

        // Write prefix for each variant type
        for variant in variants {
            variant.serialize_prefix(writer, state).await?;
        }

        Ok(())
    }

    async fn write<W: ClickhouseWrite>(
        type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        let variants = match type_ {
            Type::Variant(v) => v,
            _ => {
                return Err(KlickhouseError::TypeParseError(
                    "VariantSerializer::write called with non-Variant type".to_string(),
                ))
            }
        };

        if variants.is_empty() {
            return Err(KlickhouseError::TypeParseError(
                "Variant type must have at least one variant".to_string(),
            ));
        }

        let row_count = values.len();

        // Collect discriminators and group values by variant type
        let mut discriminators: Vec<u8> = Vec::with_capacity(row_count);
        let mut variant_values: Vec<Vec<Value>> = vec![Vec::new(); variants.len()];

        for value in values {
            match value {
                Value::Null => {
                    discriminators.push(NULL_DISCRIMINATOR);
                    // NULL values don't contribute to any variant column
                }
                Value::Variant(discriminator, inner) => {
                    if (discriminator as usize) >= variants.len() {
                        return Err(KlickhouseError::TypeParseError(format!(
                            "Variant discriminator {} out of range (max {})",
                            discriminator,
                            variants.len() - 1
                        )));
                    }
                    discriminators.push(discriminator);
                    variant_values[discriminator as usize].push(*inner);
                }
                other => {
                    return Err(KlickhouseError::TypeParseError(format!(
                        "Expected Variant or Null value, got {:?}",
                        other
                    )));
                }
            }
        }

        // Write discriminators as UInt8 column
        for d in &discriminators {
            writer.write_u8(*d).await?;
        }

        // Write each variant's values column (sparse - only non-null values)
        for (i, variant_type) in variants.iter().enumerate() {
            let values = std::mem::take(&mut variant_values[i]);
            if !values.is_empty() {
                variant_type.serialize_column(values, writer, state).await?;
            }
        }

        Ok(())
    }
}
