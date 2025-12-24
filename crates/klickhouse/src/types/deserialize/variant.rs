// DFE Fork: Variant type deserializer
// Variant is a discriminated union of types, stored as:
// - Discriminator column (UInt8 per row, 255 = NULL)
// - Value columns for each variant type (sparse/dense depending on mode)

use tokio::io::AsyncReadExt;

use crate::{io::ClickhouseRead, values::Value, KlickhouseError, Result};

use super::{Deserializer, DeserializerState, Type};

/// NULL discriminator value (special sentinel)
pub const NULL_DISCRIMINATOR: u8 = 255;

pub struct VariantDeserializer;

impl Deserializer for VariantDeserializer {
    async fn read_prefix<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        state: &mut DeserializerState,
    ) -> Result<()> {
        let variants = match type_ {
            Type::Variant(v) => v,
            _ => {
                return Err(KlickhouseError::TypeParseError(
                    "VariantDeserializer::read_prefix called with non-Variant type".to_string(),
                ))
            }
        };

        // Read prefix for each variant type
        for variant in variants {
            variant.deserialize_prefix(reader, state).await?;
        }

        Ok(())
    }

    async fn read<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        rows: usize,
        state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        let variants = match type_ {
            Type::Variant(v) => v,
            _ => {
                return Err(KlickhouseError::TypeParseError(
                    "VariantDeserializer::read called with non-Variant type".to_string(),
                ))
            }
        };

        if variants.is_empty() {
            return Err(KlickhouseError::TypeParseError(
                "Variant type must have at least one variant".to_string(),
            ));
        }

        // Read discriminators
        let mut discriminators: Vec<u8> = Vec::with_capacity(rows);
        for _ in 0..rows {
            discriminators.push(reader.read_u8().await?);
        }

        // Count values per variant type to know how many to read
        let mut variant_counts: Vec<usize> = vec![0; variants.len()];
        for d in &discriminators {
            if *d != NULL_DISCRIMINATOR && (*d as usize) < variants.len() {
                variant_counts[*d as usize] += 1;
            }
        }

        // Read each variant's values column
        let mut variant_values: Vec<Vec<Value>> = Vec::with_capacity(variants.len());
        for (i, variant_type) in variants.iter().enumerate() {
            let count = variant_counts[i];
            if count > 0 {
                let values = variant_type.deserialize_column(reader, count, state).await?;
                variant_values.push(values);
            } else {
                variant_values.push(Vec::new());
            }
        }

        // Track position in each variant's value vector
        let mut variant_positions: Vec<usize> = vec![0; variants.len()];

        // Reconstruct the output values in row order
        let mut result: Vec<Value> = Vec::with_capacity(rows);
        for d in discriminators {
            if d == NULL_DISCRIMINATOR {
                result.push(Value::Null);
            } else {
                let idx = d as usize;
                if idx >= variants.len() {
                    return Err(KlickhouseError::TypeParseError(format!(
                        "Invalid discriminator {} for Variant with {} variants",
                        d,
                        variants.len()
                    )));
                }
                let pos = variant_positions[idx];
                if pos >= variant_values[idx].len() {
                    return Err(KlickhouseError::TypeParseError(format!(
                        "Not enough values for variant {} at position {}",
                        idx, pos
                    )));
                }
                let value = variant_values[idx][pos].clone();
                variant_positions[idx] += 1;
                result.push(Value::Variant(d, Box::new(value)));
            }
        }

        Ok(result)
    }
}
