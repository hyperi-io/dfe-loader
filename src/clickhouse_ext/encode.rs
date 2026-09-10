// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      src/clickhouse_ext/encode.rs
// Purpose:   Dynamic Map<String, Value> to RowBinary encoder
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Production dynamic RowBinary encoder.
//!
//! Turns a runtime-shaped `serde_json::Map<String, Value>` plus its resolved
//! ClickHouse column schema into RowBinary wire bytes, and feeds those bytes
//! through the new clickhouse-rs chain via the proven `DynamicRow: Row +
//! Serialize` seam (see `crate::spike` for why this seam is correct).
//!
//! # Why a pure byte function under the serde seam
//!
//! The per-column wire encoding is byte-exact and type-driven, so it is
//! written as a plain `encode_row(...) -> Vec<u8>` (mirroring the hand-rolled
//! encoder it is ported from). The `Serialize` impl then pushes that whole
//! pre-computed RowBinary row through the serializer verbatim, with no extra
//! framing. This keeps the wire logic testable at the byte level with zero
//! server dependency: the bytes `encode_row` returns are exactly what crosses
//! the wire.
//!
//! ## Pushing raw bytes through the serializer without a length prefix
//!
//! The new chain's RowBinary serializer runs with a no-op validator on the
//! `with_columns` path, so most serde calls (`serialize_str`, the integer
//! methods, `serialize_seq`, ...) emit exactly the RowBinary bytes ClickHouse
//! expects. But there is no serde call that writes an arbitrary byte run with
//! NO length prefix -- `serialize_bytes` always prepends a leb128 length.
//!
//! The serializer special-cases `serialize_newtype_struct` whose name starts
//! with its internal 256-bit-integer module path: that path writes the inner
//! `serialize_bytes` payload with no length prefix. We reuse exactly that
//! contract to emit our already-framed row bytes unchanged. The name we pass
//! must therefore start with that module path; see `RAW_PASSTHROUGH_NAME`.
//!
//! # Coercions
//!
//! This encoder folds in the DFE coercions that ClickHouse's JSONEachRow path
//! cannot do server-side: epoch magnitude detection (ms/us/ns) and ISO-8601
//! 'T'-separator handling for date/time types, hyphen-less UUID hex, and
//! integer-to-dotted IPv4. All timestamps are treated as UTC; non-zero
//! timezone offsets on datetime strings are rejected rather than silently
//! dropped.

use std::borrow::Cow;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use serde::ser::{Serialize, Serializer};
use serde_json::{Map, Value};

use super::error::DynamicError;
use super::parsed_type::{ParsedType, TypeTag};

use clickhouse::_priv::RowKind;
use clickhouse::Row;

/// A single resolved column for a dynamic insert.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    /// Column name, used to look up the value in the row map.
    pub name: String,
    /// Parsed ClickHouse type, drives wire encoding.
    pub ty: ParsedType,
    /// Canonical ClickHouse type string, fed to
    /// `InsertNative::with_columns` so the server builds the right header.
    pub type_string: String,
    /// Default kind from system.columns: "", "DEFAULT", "MATERIALIZED",
    /// "ALIAS", "EPHEMERAL". Empty for columns without a server-side default.
    pub default_kind: String,
    /// True when the column has a server-side default and may be omitted from
    /// the INSERT column list (e.g. `_uuid`, `_timestamp_load`).
    pub has_default: bool,
}

impl ColumnDef {
    /// Build a column from a name and a ClickHouse type string. The column is
    /// treated as required (no server-side default).
    ///
    /// The type string is parsed once here; `type_string` keeps the original
    /// canonical form for the insert header.
    #[must_use]
    pub fn new(name: impl Into<String>, type_string: impl Into<String>) -> Self {
        let type_string = type_string.into();
        let ty = ParsedType::parse(&type_string);
        Self {
            name: name.into(),
            ty,
            type_string,
            default_kind: String::new(),
            has_default: false,
        }
    }

    /// Build a column carrying its `system.columns` default kind. `has_default`
    /// is set when `default_kind` is non-empty, marking the column omittable
    /// from the INSERT list.
    #[must_use]
    pub fn with_default_kind(
        name: impl Into<String>,
        type_string: impl Into<String>,
        default_kind: impl Into<String>,
    ) -> Self {
        let default_kind = default_kind.into();
        let has_default = !default_kind.is_empty();
        let mut column = Self::new(name, type_string);
        column.default_kind = default_kind;
        column.has_default = has_default;
        column
    }
}

/// The serde newtype name that routes a payload through the serializer's
/// raw-bytes-without-length-prefix path. It must start with the chain's
/// internal 256-bit-integer module path for the special case to trigger.
const RAW_PASSTHROUGH_NAME: &str = "clickhouse::types::int256::__dfe_raw_row";

/// Runtime-dynamic row: a borrowed JSON object plus the resolved column
/// schema it is encoded against, with optional raw passthrough for one named
/// JSON column.
///
/// Implements `clickhouse::Row + serde::Serialize` so it can be written
/// through `InsertNative::with_columns` exactly like a derived row.
pub struct DynamicRow<'a> {
    row: &'a Map<String, Value>,
    columns: &'a [ColumnDef],
    /// When set, `(column_name, raw_bytes)`: the named JSON column is written
    /// from `raw_bytes` as a length-prefixed string rather than from `row`.
    raw: Option<(&'a str, &'a [u8])>,
}

impl<'a> DynamicRow<'a> {
    /// Encode a row, taking each column's value from `row` by name.
    #[must_use]
    pub fn new(row: &'a Map<String, Value>, columns: &'a [ColumnDef]) -> Self {
        Self {
            row,
            columns,
            raw: None,
        }
    }

    /// Same as [`DynamicRow::new`], but the named JSON column (e.g. `_json`)
    /// is written from `raw` bytes -- a zero-copy passthrough of the original
    /// payload as a JSON string -- instead of from `row`.
    #[must_use]
    pub fn with_raw(
        row: &'a Map<String, Value>,
        columns: &'a [ColumnDef],
        raw: &'a [u8],
        json_col: &'a str,
    ) -> Self {
        Self {
            row,
            columns,
            raw: Some((json_col, raw)),
        }
    }

    /// Encode this row to RowBinary wire bytes.
    ///
    /// These are exactly the bytes that cross the wire for this row; the
    /// `Serialize` impl pushes them through the serializer verbatim.
    ///
    /// # Errors
    /// Returns [`DynamicError`] if any column value cannot be encoded for its
    /// target type.
    pub fn encode(&self) -> Result<Vec<u8>, DynamicError> {
        let mut buf = Vec::with_capacity(256);
        for col in self.columns {
            self.encode_column(col, &mut buf)?;
        }
        Ok(buf)
    }

    /// Encode a single column's value to RowBinary, appending to `buf`.
    ///
    /// Honours the raw `_json` passthrough for the named JSON column.
    fn encode_column(&self, col: &ColumnDef, buf: &mut Vec<u8>) -> Result<(), DynamicError> {
        if let Some((name, bytes)) = self.raw
            && name == col.name
        {
            // Raw passthrough for a JSON/String column: Nullable not-null
            // marker if needed, then the bytes as a length-prefixed string.
            if col.ty.nullable {
                buf.push(0);
            }
            write_string(bytes, buf);
            return Ok(());
        }
        let value = self.row.get(&col.name).unwrap_or(&Value::Null);
        encode_value(value, &col.ty, &col.name, buf)
    }
}

impl Row for DynamicRow<'_> {
    // Consts are unused on the HTTP `with_columns` path: the column layout
    // comes from the runtime headers, and the no-op validator does not consult
    // a derived shape.
    const NAME: &'static str = "DynamicRow";
    const COLUMN_NAMES: &'static [&'static str] = &[];
    const COLUMN_COUNT: usize = 0;
    const KIND: RowKind = RowKind::Struct;
    type Value<'a> = DynamicRow<'a>;
}

impl Serialize for DynamicRow<'_> {
    fn serialize<S: Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{Error as _, SerializeStruct};
        // One field PER COLUMN. InsertNative frames the columnar Native block
        // from the per-field calls, so the whole row must NOT be emitted as a
        // single blob -- that mis-frames multi-row blocks (the server reads a
        // wrong byte count). Each field is the column's RowBinary value pushed
        // through the no-length-prefix raw-bytes path.
        let mut st = ser.serialize_struct("DynamicRow", self.columns.len())?;
        let mut col = Vec::with_capacity(32);
        for c in self.columns {
            col.clear();
            self.encode_column(c, &mut col).map_err(S::Error::custom)?;
            st.serialize_field("c", &ColumnRaw(&col))?;
        }
        st.end()
    }
}

/// A single column's RowBinary value, emitted with no extra framing: the raw
/// newtype path writes the inner `serialize_bytes` payload without a length
/// prefix, so the column's pre-computed bytes cross the wire verbatim.
struct ColumnRaw<'a>(&'a [u8]);

impl Serialize for ColumnRaw<'_> {
    fn serialize<S: Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_newtype_struct(RAW_PASSTHROUGH_NAME, &RawBytes(self.0))
    }
}

/// Inner payload for the raw-passthrough newtype: emits its bytes via
/// `serialize_bytes`, which -- under the raw newtype path -- writes them with
/// no length prefix.
struct RawBytes<'a>(&'a [u8]);

impl Serialize for RawBytes<'_> {
    fn serialize<S: Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_bytes(self.0)
    }
}

// ---------------------------------------------------------------------------
// Per-value encoding
// ---------------------------------------------------------------------------

fn encode_value(
    value: &Value,
    pt: &ParsedType,
    col: &str,
    buf: &mut Vec<u8>,
) -> Result<(), DynamicError> {
    // Nullable wrapper: one level only (ClickHouse forbids Nullable(Nullable)).
    if pt.nullable {
        if value.is_null() {
            buf.push(1);
            return Ok(());
        }
        buf.push(0);
    } else if value.is_null() {
        write_default(pt, buf);
        return Ok(());
    }

    encode_typed(value, pt, col, buf)
}

#[allow(clippy::too_many_lines)]
fn encode_typed(
    value: &Value,
    pt: &ParsedType,
    col: &str,
    buf: &mut Vec<u8>,
) -> Result<(), DynamicError> {
    // A bare `Decimal(P, S)` carries tag Unknown; resolve it to a concrete
    // width by precision so it hits the right Decimal arm below.
    let tag = if pt.base == "Decimal" {
        decimal_tag_for_precision(pt.precision.unwrap_or(38))
    } else {
        pt.tag
    };

    match tag {
        TypeTag::String => {
            let s = value_to_str(value);
            write_string(s.as_bytes(), buf);
        }
        TypeTag::FixedString => {
            let s = value_to_str(value);
            let n = pt.fixed_size.unwrap_or(1);
            let bytes = s.as_bytes();
            // CH FixedString is a raw N-byte run: pad short values with NULs,
            // truncate long ones. No length prefix.
            if bytes.len() <= n {
                buf.extend_from_slice(bytes);
                buf.resize(buf.len() + (n - bytes.len()), 0);
            } else {
                buf.extend_from_slice(&bytes[..n]);
            }
        }
        TypeTag::Bool => {
            buf.push(u8::from(as_bool(value, col)?));
        }
        TypeTag::UInt8 => {
            buf.push(as_u64(value, col)? as u8);
        }
        TypeTag::UInt16 => {
            buf.extend_from_slice(&(as_u64(value, col)? as u16).to_le_bytes());
        }
        TypeTag::UInt32 => {
            buf.extend_from_slice(&(as_u64(value, col)? as u32).to_le_bytes());
        }
        TypeTag::UInt64 => {
            buf.extend_from_slice(&as_u64(value, col)?.to_le_bytes());
        }
        TypeTag::UInt128 => {
            buf.extend_from_slice(&as_u128(value, col)?.to_le_bytes());
        }
        TypeTag::UInt256 => {
            buf.extend_from_slice(&as_u256_le(value, col)?);
        }
        TypeTag::Int8 => {
            buf.extend_from_slice(&(as_i64(value, col)? as i8).to_le_bytes());
        }
        TypeTag::Int16 => {
            buf.extend_from_slice(&(as_i64(value, col)? as i16).to_le_bytes());
        }
        TypeTag::Enum8 => {
            let v = enum_discriminant(value, &pt.raw, col)?;
            let v = i8::try_from(v).map_err(|_| enc_err(col, "Enum8 value out of range"))?;
            buf.extend_from_slice(&v.to_le_bytes());
        }
        TypeTag::Enum16 => {
            let v = enum_discriminant(value, &pt.raw, col)?;
            let v = i16::try_from(v).map_err(|_| enc_err(col, "Enum16 value out of range"))?;
            buf.extend_from_slice(&v.to_le_bytes());
        }
        TypeTag::Int32 => {
            buf.extend_from_slice(&(as_i64(value, col)? as i32).to_le_bytes());
        }
        TypeTag::Int64 => {
            buf.extend_from_slice(&as_i64(value, col)?.to_le_bytes());
        }
        TypeTag::Int128 => {
            buf.extend_from_slice(&as_i128(value, col)?.to_le_bytes());
        }
        TypeTag::Int256 => {
            buf.extend_from_slice(&as_i256_le(value, col)?);
        }
        TypeTag::Float32 => {
            buf.extend_from_slice(&(as_f64(value, col)? as f32).to_le_bytes());
        }
        TypeTag::Float64 => {
            buf.extend_from_slice(&as_f64(value, col)?.to_le_bytes());
        }
        TypeTag::Date => {
            // Date is UInt16 days since 1970-01-01.
            let days = to_epoch_days(value, col)?;
            let days = u16::try_from(days)
                .map_err(|_| enc_err(col, "Date out of range for UInt16 days"))?;
            buf.extend_from_slice(&days.to_le_bytes());
        }
        TypeTag::Date32 => {
            // Date32 is Int32 days since 1970-01-01 (can be negative).
            let days = to_epoch_days(value, col)?;
            buf.extend_from_slice(&days.to_le_bytes());
        }
        TypeTag::DateTime => {
            // DateTime is UInt32 seconds since epoch.
            let secs = to_epoch_seconds(value, col)?;
            let secs = u32::try_from(secs)
                .map_err(|_| enc_err(col, "DateTime out of range for UInt32 seconds"))?;
            buf.extend_from_slice(&secs.to_le_bytes());
        }
        TypeTag::DateTime64 => {
            let precision = pt.precision.unwrap_or(3);
            let ticks = datetime64_to_ticks(value, precision, col)?;
            buf.extend_from_slice(&ticks.to_le_bytes());
        }
        TypeTag::Decimal32 => {
            let scale = pt.scale.unwrap_or(0);
            let backing = decimal_backing_i128(value, scale, col)?;
            let v = i32::try_from(backing).map_err(|_| enc_err(col, "Decimal32 overflow"))?;
            buf.extend_from_slice(&v.to_le_bytes());
        }
        TypeTag::Decimal64 => {
            let scale = pt.scale.unwrap_or(0);
            let backing = decimal_backing_i128(value, scale, col)?;
            let v = i64::try_from(backing).map_err(|_| enc_err(col, "Decimal64 overflow"))?;
            buf.extend_from_slice(&v.to_le_bytes());
        }
        TypeTag::Decimal128 => {
            let scale = pt.scale.unwrap_or(0);
            let backing = decimal_backing_i128(value, scale, col)?;
            buf.extend_from_slice(&backing.to_le_bytes());
        }
        TypeTag::Decimal256 => {
            // Decimal256 backing is a 256-bit signed integer; the loader's
            // f64-scaled magnitudes fit in i128, so sign-extend to 32 bytes.
            let scale = pt.scale.unwrap_or(0);
            let backing = decimal_backing_i128(value, scale, col)?;
            buf.extend_from_slice(&i128_to_i256_le(backing));
        }
        TypeTag::UUID => encode_uuid(value, col, buf)?,
        TypeTag::IPv4 => encode_ipv4(value, col, buf)?,
        TypeTag::IPv6 => encode_ipv6(value, col, buf)?,
        TypeTag::Array => {
            let elem = pt
                .array_element
                .as_ref()
                .ok_or_else(|| enc_err(col, "Array without element type"))?;
            encode_array(value, elem, col, buf)?;
        }
        TypeTag::Map => {
            let (kt, vt) = pt
                .map_types
                .as_ref()
                .ok_or_else(|| enc_err(col, "Map without key/value types"))?;
            encode_map(value, kt, vt, col, buf)?;
        }
        TypeTag::JSON => {
            write_string(&json_column_text(value), buf);
        }
        // Geo Point and any type the parser left as Unknown are not part of
        // the supported dynamic-insert surface. Tuple/Variant/Dynamic/Nested
        // were not handled by the source encoder either; reject explicitly so
        // a schema using them fails loudly instead of writing wrong bytes.
        TypeTag::Point | TypeTag::Tuple | TypeTag::Unknown => {
            return Err(DynamicError::UnsupportedType {
                column: col.to_string(),
                type_str: pt.raw.clone(),
            });
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// JSON column shaping
// ---------------------------------------------------------------------------

/// Key an array landing in a JSON column is stored under. `list` is the key the
/// deployed dfe-docker VRL workaround already wraps ECS `tags` with, so stored
/// data keeps one shape once that workaround is removed.
const JSON_LIST_KEY: &str = "list";

/// Key a scalar landing in a JSON column is stored under.
const JSON_SCALAR_KEY: &str = "value";

/// What a value must become for a `ClickHouse` JSON column to accept it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsonShape {
    /// An object already -- store it as it stands.
    AsIs,
    /// Missing or empty -- becomes `{}`, since JSON cannot be Nullable.
    Empty,
    /// A JSON container carried as text -- wrap what that text holds.
    WrapText(&'static str),
    /// Not an object -- wrap the value itself.
    WrapValue(&'static str),
}

/// The single shaping rule for a value landing in a JSON column, which only
/// accepts an object at the top level -- anything else is code 117.
///
/// Both insert paths decide here: `json_column_text` renders the decision as
/// RowBinary wire text, `shape_json_value` applies it to the row map the
/// JSONEachRow body is serialised from.
///
/// A `Value::String` in a JSON column carries JSON TEXT, so the text decides
/// rather than the Rust type -- but only once it parses. Text that does not
/// parse is stored as the string it is, because `{not json` reaching the column
/// verbatim is the same code 117 this rule exists to stop.
fn json_shape(value: &Value) -> JsonShape {
    match value {
        Value::Null => JsonShape::Empty,
        Value::Object(_) => JsonShape::AsIs,
        Value::Array(_) => JsonShape::WrapValue(JSON_LIST_KEY),
        Value::Bool(_) | Value::Number(_) => JsonShape::WrapValue(JSON_SCALAR_KEY),
        Value::String(s) => match s.trim_start().as_bytes().first() {
            None => JsonShape::Empty,
            Some(b'{') if is_whole_json(s) => JsonShape::AsIs,
            Some(b'[') if is_whole_json(s) => JsonShape::WrapText(JSON_LIST_KEY),
            _ => JsonShape::WrapValue(JSON_SCALAR_KEY),
        },
    }
}

/// Whether the text is one complete JSON value and nothing after it.
/// `IgnoredAny` validates the syntax without building a document.
fn is_whole_json(text: &str) -> bool {
    sonic_rs::from_str::<serde::de::IgnoredAny>(text).is_ok()
}

/// Render a value as the JSON text a `ClickHouse` JSON column accepts.
///
/// Shaping at the write rather than in one producer covers every rule that
/// fills a JSON column. Object text is written verbatim (the zero-copy `_json`
/// case) and array text is wrapped without a re-parse.
fn json_column_text(value: &Value) -> Cow<'_, [u8]> {
    match (json_shape(value), value) {
        (JsonShape::Empty, _) => Cow::Borrowed("{}".as_bytes()),
        (JsonShape::AsIs, Value::String(s)) => Cow::Borrowed(s.as_bytes()),
        (JsonShape::AsIs, _) => Cow::Owned(value.to_string().into_bytes()),
        (JsonShape::WrapText(key), Value::String(s)) => Cow::Owned(wrap_json_text(key, s)),
        (JsonShape::WrapText(key) | JsonShape::WrapValue(key), _) => {
            Cow::Owned(wrap_json_text(key, &value.to_string()))
        }
    }
}

/// Apply the JSON-column shaping rule to a value in place -- the JSONEachRow
/// counterpart of `json_column_text`, for the path that serialises the row map
/// itself rather than encoding it column by column.
///
/// The value path stores structure, not text: the JSONEachRow body has no
/// read-json-as-string setting, so JSON text is parsed back before it is stored
/// or wrapped, and both paths end with the same value in the column.
pub fn shape_json_value(value: &mut Value) {
    let shape = json_shape(value);
    let shaped = match (shape, value.take()) {
        (JsonShape::AsIs, Value::String(s)) => json_text_to_value(s),
        (JsonShape::AsIs, taken) => taken,
        (JsonShape::Empty, _) => Value::Object(Map::new()),
        (JsonShape::WrapText(key), Value::String(s)) => wrap_value(key, json_text_to_value(s)),
        (JsonShape::WrapText(key) | JsonShape::WrapValue(key), taken) => wrap_value(key, taken),
    };
    *value = shaped;
}

/// Parse JSON text that [`json_shape`] has already validated; the fallback
/// keeps a string that parses here but not there rather than dropping it.
fn json_text_to_value(text: String) -> Value {
    sonic_rs::from_str(&text).unwrap_or(Value::String(text))
}

/// Build `{"<key>": <value>}`.
fn wrap_value(key: &str, inner: Value) -> Value {
    let mut wrapped = Map::with_capacity(1);
    wrapped.insert(key.to_string(), inner);
    Value::Object(wrapped)
}

/// Shape every JSON position inside a value, at the same depths the RowBinary
/// encoder writes JSON text: a JSON column, and a JSON nested in an `Array` or
/// a `Map` value, which `ParsedType::contains_json` reports.
pub fn shape_json_for_type(value: &mut Value, ty: &ParsedType) {
    if ty.nullable && value.is_null() {
        return;
    }
    match ty.tag {
        TypeTag::JSON => shape_json_value(value),
        TypeTag::Array => {
            if let (Some(element), Value::Array(items)) = (ty.array_element.as_ref(), &mut *value) {
                for item in items {
                    shape_json_for_type(item, element);
                }
            }
        }
        TypeTag::Map => {
            if let (Some((_, value_type)), Value::Object(entries)) =
                (ty.map_types.as_ref(), &mut *value)
            {
                for (_, entry) in entries {
                    shape_json_for_type(entry, value_type);
                }
            }
        }
        _ => {}
    }
}

/// Whether [`shape_json_for_type`] would change this value -- the check that
/// keeps the JSONEachRow path from cloning a row it does not need to touch.
#[must_use]
pub fn json_shaping_changes(value: &Value, ty: &ParsedType) -> bool {
    if ty.nullable && value.is_null() {
        return false;
    }
    match ty.tag {
        TypeTag::JSON => !value.is_object(),
        TypeTag::Array => match (ty.array_element.as_ref(), value) {
            (Some(element), Value::Array(items)) => {
                items.iter().any(|item| json_shaping_changes(item, element))
            }
            _ => false,
        },
        TypeTag::Map => match (ty.map_types.as_ref(), value) {
            (Some((_, value_type)), Value::Object(entries)) => entries
                .iter()
                .any(|(_, entry)| json_shaping_changes(entry, value_type)),
            _ => false,
        },
        _ => false,
    }
}

/// Build `{"<key>": <json>}` from JSON text, without parsing it back.
fn wrap_json_text(key: &str, json: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(json.len() + key.len() + 6);
    out.extend_from_slice(b"{\"");
    out.extend_from_slice(key.as_bytes());
    out.extend_from_slice(b"\":");
    out.extend_from_slice(json.as_bytes());
    out.push(b'}');
    out
}

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

fn write_string(bytes: &[u8], buf: &mut Vec<u8>) {
    write_varint(bytes.len() as u64, buf);
    buf.extend_from_slice(bytes);
}

fn write_varint(mut value: u64, buf: &mut Vec<u8>) {
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            buf.push(byte);
            break;
        }
        buf.push(byte | 0x80);
    }
}

fn write_default(pt: &ParsedType, buf: &mut Vec<u8>) {
    // A bare `Decimal(P, S)` has no fixed_byte_size by base name; resolve it.
    if pt.base == "Decimal" {
        let size = match decimal_tag_for_precision(pt.precision.unwrap_or(38)) {
            TypeTag::Decimal32 => 4,
            TypeTag::Decimal64 => 8,
            TypeTag::Decimal256 => 32,
            _ => 16,
        };
        buf.resize(buf.len() + size, 0);
        return;
    }
    // JSON's no-value is the empty object: the column parser rejects empty
    // input (code 117) and JSON cannot be Nullable.
    if pt.tag == TypeTag::JSON {
        write_string(b"{}", buf);
        return;
    }
    if let Some(size) = pt.fixed_byte_size() {
        buf.resize(buf.len() + size, 0);
    } else {
        // Variable-length default: empty string / array / map.
        write_varint(0, buf);
    }
}

fn enc_err(col: &str, msg: &str) -> DynamicError {
    DynamicError::EncodingError {
        column: col.to_string(),
        message: msg.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Scalar coercions
// ---------------------------------------------------------------------------

/// Borrow the string directly when possible; allocate only for non-string
/// values that need stringification.
fn value_to_str(value: &Value) -> Cow<'_, str> {
    match value {
        Value::String(s) => Cow::Borrowed(s.as_str()),
        Value::Number(n) => Cow::Owned(n.to_string()),
        Value::Bool(b) => Cow::Borrowed(if *b { "true" } else { "false" }),
        Value::Null => Cow::Borrowed(""),
        other => Cow::Owned(other.to_string()),
    }
}

fn as_u64(value: &Value, col: &str) -> Result<u64, DynamicError> {
    match value {
        Value::Number(n) => n
            .as_u64()
            .or_else(|| n.as_i64().map(|v| v as u64))
            .or_else(|| n.as_f64().map(|v| v as u64))
            .ok_or_else(|| enc_err(col, "not a valid unsigned integer")),
        Value::Bool(b) => Ok(u64::from(*b)),
        Value::String(s) => s
            .trim()
            .parse::<u64>()
            .map_err(|_| enc_err(col, "string not parseable as u64")),
        _ => Err(enc_err(col, "expected number")),
    }
}

fn as_i64(value: &Value, col: &str) -> Result<i64, DynamicError> {
    match value {
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_u64().map(|v| v as i64))
            .or_else(|| n.as_f64().map(|v| v as i64))
            .ok_or_else(|| enc_err(col, "not a valid integer")),
        Value::Bool(b) => Ok(i64::from(*b)),
        Value::String(s) => s
            .trim()
            .parse::<i64>()
            .map_err(|_| enc_err(col, "string not parseable as i64")),
        _ => Err(enc_err(col, "expected number")),
    }
}

fn as_u128(value: &Value, col: &str) -> Result<u128, DynamicError> {
    match value {
        Value::Number(n) => n
            .as_u64()
            .map(u128::from)
            .or_else(|| n.as_i64().map(|v| v as u128))
            .ok_or_else(|| enc_err(col, "not a valid u128")),
        Value::Bool(b) => Ok(u128::from(*b)),
        Value::String(s) => s
            .trim()
            .parse::<u128>()
            .map_err(|_| enc_err(col, "string not parseable as u128")),
        _ => Err(enc_err(col, "expected number")),
    }
}

fn as_i128(value: &Value, col: &str) -> Result<i128, DynamicError> {
    match value {
        Value::Number(n) => n
            .as_i64()
            .map(i128::from)
            .or_else(|| n.as_u64().map(i128::from))
            .ok_or_else(|| enc_err(col, "not a valid i128")),
        Value::Bool(b) => Ok(i128::from(*b)),
        Value::String(s) => s
            .trim()
            .parse::<i128>()
            .map_err(|_| enc_err(col, "string not parseable as i128")),
        _ => Err(enc_err(col, "expected number")),
    }
}

fn as_f64(value: &Value, col: &str) -> Result<f64, DynamicError> {
    match value {
        Value::Number(n) => n.as_f64().ok_or_else(|| enc_err(col, "not a valid float")),
        Value::String(s) => s
            .trim()
            .parse::<f64>()
            .map_err(|_| enc_err(col, "string not parseable as f64")),
        _ => Err(enc_err(col, "expected number")),
    }
}

fn as_bool(value: &Value, col: &str) -> Result<bool, DynamicError> {
    match value {
        Value::Bool(b) => Ok(*b),
        Value::Number(n) => Ok(n.as_i64().unwrap_or(0) != 0),
        Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
            "true" | "yes" | "1" => Ok(true),
            "false" | "no" | "0" | "" => Ok(false),
            _ => Err(enc_err(col, "string not parseable as bool")),
        },
        Value::Null => Ok(false),
        _ => Err(enc_err(col, "expected bool")),
    }
}

/// Sign-extend a 128-bit signed backing value to a 32-byte little-endian
/// 256-bit integer (for `Int256` / `Decimal256`).
fn i128_to_i256_le(v: i128) -> [u8; 32] {
    let mut out = [if v < 0 { 0xFFu8 } else { 0x00u8 }; 32];
    out[..16].copy_from_slice(&v.to_le_bytes());
    out
}

/// Zero-extend a 128-bit unsigned value to a 32-byte little-endian 256-bit
/// integer (for `UInt256`).
fn u128_to_u256_le(v: u128) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[..16].copy_from_slice(&v.to_le_bytes());
    out
}

fn as_i256_le(value: &Value, col: &str) -> Result<[u8; 32], DynamicError> {
    Ok(i128_to_i256_le(as_i128(value, col)?))
}

fn as_u256_le(value: &Value, col: &str) -> Result<[u8; 32], DynamicError> {
    Ok(u128_to_u256_le(as_u128(value, col)?))
}

// ---------------------------------------------------------------------------
// Decimal
// ---------------------------------------------------------------------------

/// Resolve a bare `Decimal(P, S)` to its concrete backing width by precision.
/// ClickHouse uses Decimal32 for P<=9, Decimal64 for P<=18, Decimal128 for
/// P<=38, and Decimal256 above that.
fn decimal_tag_for_precision(precision: u8) -> TypeTag {
    match precision {
        0..=9 => TypeTag::Decimal32,
        10..=18 => TypeTag::Decimal64,
        19..=38 => TypeTag::Decimal128,
        _ => TypeTag::Decimal256,
    }
}

/// Compute the backing integer for a `Decimal(P, S)` by scaling the value by
/// `10^scale`. Mirrors the loader's f64-based scaling; the concrete width is
/// applied by the caller.
fn decimal_backing_i128(value: &Value, scale: u8, col: &str) -> Result<i128, DynamicError> {
    let factor = 10f64.powi(i32::from(scale));
    let f = match value {
        Value::Number(n) => n.as_f64().ok_or_else(|| enc_err(col, "invalid decimal"))?,
        Value::String(s) => s
            .trim()
            .parse::<f64>()
            .map_err(|_| enc_err(col, "string not parseable as decimal"))?,
        Value::Bool(b) => f64::from(u8::from(*b)),
        _ => return Err(enc_err(col, "expected decimal number")),
    };
    Ok((f * factor).round() as i128)
}

// ---------------------------------------------------------------------------
// Date / time
// ---------------------------------------------------------------------------

/// Scale a numeric epoch value down to whole seconds, detecting ms/us/ns by
/// magnitude. Matches the loader's coercer thresholds.
fn epoch_to_seconds(ts: i64) -> i64 {
    if ts > 1_000_000_000_000_000_000 {
        ts / 1_000_000_000
    } else if ts > 1_000_000_000_000_000 {
        ts / 1_000_000
    } else if ts > 1_000_000_000_000 {
        ts / 1_000
    } else {
        ts
    }
}

/// Resolve a JSON value to whole epoch seconds (UTC). Accepts a date/datetime
/// string, a numeric-string epoch, or a number (with magnitude detection).
fn to_epoch_seconds(value: &Value, col: &str) -> Result<i64, DynamicError> {
    match value {
        Value::Number(n) => {
            let raw = n
                .as_i64()
                .or_else(|| n.as_f64().map(|f| f as i64))
                .ok_or_else(|| enc_err(col, "invalid epoch number"))?;
            Ok(epoch_to_seconds(raw))
        }
        Value::String(s) => {
            let s = s.trim();
            if let Ok(raw) = s.parse::<i64>() {
                return Ok(epoch_to_seconds(raw));
            }
            let (secs, _) = parse_datetime_str(s).map_err(|m| enc_err(col, &m))?;
            Ok(secs)
        }
        _ => Err(enc_err(col, "expected datetime string or epoch number")),
    }
}

/// Resolve a JSON value to days since 1970-01-01 (UTC). Accepts a date string,
/// a numeric-string epoch, or a number (with magnitude detection).
fn to_epoch_days(value: &Value, col: &str) -> Result<i32, DynamicError> {
    // A bare "YYYY-MM-DD" has no time component; parse it directly.
    if let Value::String(s) = value {
        let s = s.trim();
        if s.len() == 10 && s.as_bytes().get(4) == Some(&b'-') {
            let year: i32 = s[0..4]
                .parse()
                .map_err(|_| enc_err(col, "invalid Date year"))?;
            let month: u32 = s[5..7]
                .parse()
                .map_err(|_| enc_err(col, "invalid Date month"))?;
            let day: u32 = s[8..10]
                .parse()
                .map_err(|_| enc_err(col, "invalid Date day"))?;
            if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
                return Err(enc_err(col, "invalid Date components"));
            }
            let days = civil_days_from_epoch(year, month, day);
            return i32::try_from(days).map_err(|_| enc_err(col, "Date out of range"));
        }
    }
    let secs = to_epoch_seconds(value, col)?;
    let days = secs.div_euclid(86_400);
    i32::try_from(days).map_err(|_| enc_err(col, "Date out of range"))
}

/// Convert a JSON value to `DateTime64` epoch ticks at the given precision.
fn datetime64_to_ticks(value: &Value, precision: u8, col: &str) -> Result<i64, DynamicError> {
    match value {
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_f64().map(|f| f as i64))
            .ok_or_else(|| enc_err(col, "invalid DateTime64 number")),
        Value::String(s) => {
            let s = s.trim();
            // A numeric string is treated as an already-scaled tick count.
            if let Ok(n) = s.parse::<i64>() {
                return Ok(n);
            }
            let (secs, frac_nanos) = parse_datetime_str(s).map_err(|m| enc_err(col, &m))?;
            let multiplier = 10i64.pow(u32::from(precision));
            let base = secs
                .checked_mul(multiplier)
                .ok_or_else(|| enc_err(col, "DateTime64 epoch overflow"))?;
            let frac_scaled =
                i64::from(frac_nanos) / 10i64.pow(9u32.saturating_sub(u32::from(precision)));
            base.checked_add(frac_scaled)
                .ok_or_else(|| enc_err(col, "DateTime64 epoch overflow"))
        }
        _ => Err(enc_err(col, "expected number or datetime string")),
    }
}

/// Parse `YYYY-MM-DD[ T]HH:MM:SS[.frac][Z|+00:00]` into `(unix_seconds,
/// fractional_nanoseconds)`. UTC only; non-zero offsets are rejected.
fn parse_datetime_str(s: &str) -> Result<(i64, u32), String> {
    let bytes = s.as_bytes();
    let len = bytes.len();

    if len < 19 {
        return Err("invalid DateTime string".into());
    }
    let sep = bytes[10];
    if sep != b' ' && sep != b'T' {
        return Err("invalid DateTime string".into());
    }

    let mut pos = 19;

    let frac_nanos = if pos < len && bytes[pos] == b'.' {
        pos += 1;
        let frac_start = pos;
        while pos < len && bytes[pos].is_ascii_digit() {
            pos += 1;
        }
        let frac_digits = pos - frac_start;
        if frac_digits == 0 {
            return Err("invalid DateTime string".into());
        }
        let clamped = frac_digits.min(9);
        let frac_slice = &s[frac_start..frac_start + clamped];
        let mut frac: u32 = frac_slice
            .parse()
            .map_err(|_| "invalid fractional seconds".to_string())?;
        if clamped < 9 {
            frac *= 10u32.pow(9 - clamped as u32);
        }
        frac
    } else {
        0u32
    };

    if pos < len {
        if bytes[pos] == b'Z' {
            pos += 1;
        } else if (bytes[pos] == b'+' || bytes[pos] == b'-') && pos + 6 <= len {
            let sign = bytes[pos];
            let tz_slice = &s[pos + 1..pos + 6];
            if tz_slice.len() == 5
                && tz_slice.as_bytes()[2] == b':'
                && tz_slice[..2].bytes().all(|b| b.is_ascii_digit())
                && tz_slice[3..].bytes().all(|b| b.is_ascii_digit())
            {
                if sign == b'-' || &tz_slice[..2] != "00" || &tz_slice[3..] != "00" {
                    return Err(format!(
                        "non-UTC timezone offset '{}{}' not supported; convert to UTC first",
                        sign as char, tz_slice
                    ));
                }
                pos += 6;
            }
        }
    }

    if pos != len {
        return Err("invalid DateTime string".into());
    }

    let year: i32 = s[0..4].parse().map_err(|_| "invalid year".to_string())?;
    let month: u32 = s[5..7].parse().map_err(|_| "invalid month".to_string())?;
    let day: u32 = s[8..10].parse().map_err(|_| "invalid day".to_string())?;
    let hour: u32 = s[11..13].parse().map_err(|_| "invalid hour".to_string())?;
    let min: u32 = s[14..16]
        .parse()
        .map_err(|_| "invalid minute".to_string())?;
    let sec: u32 = s[17..19]
        .parse()
        .map_err(|_| "invalid second".to_string())?;

    if !(1..=12).contains(&month) || !(1..=31).contains(&day) || hour > 23 || min > 59 || sec > 59 {
        return Err("invalid DateTime string".into());
    }

    let days = civil_days_from_epoch(year, month, day);
    let secs = days * 86_400 + i64::from(hour) * 3600 + i64::from(min) * 60 + i64::from(sec);
    Ok((secs, frac_nanos))
}

/// Days since 1970-01-01 for a civil date (Howard Hinnant's algorithm).
fn civil_days_from_epoch(year: i32, month: u32, day: u32) -> i64 {
    let y = i64::from(if month <= 2 { year - 1 } else { year });
    let m = i64::from(if month <= 2 { month + 9 } else { month - 3 });
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let doy = (153 * m + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

// ---------------------------------------------------------------------------
// Enum
// ---------------------------------------------------------------------------

/// Resolve an enum value to its signed discriminant. A numeric value is used
/// directly; a string is mapped via the `Enum8(...)` / `Enum16(...)` member
/// list parsed from the type string.
fn enum_discriminant(value: &Value, type_str: &str, col: &str) -> Result<i64, DynamicError> {
    match value {
        Value::Number(_) | Value::Bool(_) => as_i64(value, col),
        Value::String(s) => {
            // A numeric string is treated as the discriminant directly.
            if let Ok(n) = s.trim().parse::<i64>() {
                return Ok(n);
            }
            enum_member_value(type_str, s)
                .ok_or_else(|| enc_err(col, "enum string not found in type definition"))
        }
        _ => Err(enc_err(col, "expected enum value")),
    }
}

/// Look up `name` in an `Enum8(...)`/`Enum16(...)` definition, returning its
/// integer discriminant. Members look like `'name' = 1` separated by commas.
fn enum_member_value(type_str: &str, name: &str) -> Option<i64> {
    let open = type_str.find('(')?;
    let close = type_str.rfind(')')?;
    if open >= close {
        return None;
    }
    let inner = &type_str[open + 1..close];
    for member in inner.split(',') {
        let (label, num) = member.split_once('=')?;
        let label = label.trim().trim_matches('\'').trim_matches('"');
        if label == name {
            return num.trim().parse::<i64>().ok();
        }
    }
    None
}

// ---------------------------------------------------------------------------
// UUID / IP
// ---------------------------------------------------------------------------

fn encode_uuid(value: &Value, col: &str, buf: &mut Vec<u8>) -> Result<(), DynamicError> {
    let s = value_to_str(value);
    // Accept hyphen-less hex or any punctuation by keeping only hex digits.
    let hex: String = s.chars().filter(char::is_ascii_hexdigit).collect();
    if hex.len() != 32 {
        return Err(enc_err(col, "invalid UUID length"));
    }
    // ClickHouse RowBinary UUID: two little-endian u64, high word first.
    let high = u64::from_str_radix(&hex[..16], 16).map_err(|_| enc_err(col, "invalid UUID hex"))?;
    let low = u64::from_str_radix(&hex[16..], 16).map_err(|_| enc_err(col, "invalid UUID hex"))?;
    buf.extend_from_slice(&high.to_le_bytes());
    buf.extend_from_slice(&low.to_le_bytes());
    Ok(())
}

fn encode_ipv4(value: &Value, col: &str, buf: &mut Vec<u8>) -> Result<(), DynamicError> {
    // Integer input is interpreted as the host-order address.
    if let Value::Number(n) = value
        && let Some(u) = n.as_u64()
    {
        let addr = Ipv4Addr::from(u as u32);
        buf.extend_from_slice(&u32::from(addr).to_le_bytes());
        return Ok(());
    }
    let s = value_to_str(value);
    let addr: Ipv4Addr = s.parse().map_err(|_| enc_err(col, "invalid IPv4"))?;
    // ClickHouse stores IPv4 as UInt32 little-endian.
    buf.extend_from_slice(&u32::from(addr).to_le_bytes());
    Ok(())
}

fn encode_ipv6(value: &Value, col: &str, buf: &mut Vec<u8>) -> Result<(), DynamicError> {
    let s = value_to_str(value);
    // An IPv6 column accepts an IPv4 literal -- `toIPv6('172.17.3.4')` is
    // `::ffff:172.17.3.4` -- so both families parse and V4 maps to the same form.
    let addr: Ipv6Addr = match s.parse::<IpAddr>() {
        Ok(IpAddr::V6(v6)) => v6,
        Ok(IpAddr::V4(v4)) => v4.to_ipv6_mapped(),
        Err(_) => return Err(enc_err(col, "invalid IP address")),
    };
    // 16 octets in network byte order, written verbatim.
    buf.extend_from_slice(&addr.octets());
    Ok(())
}

// ---------------------------------------------------------------------------
// Array / Map
// ---------------------------------------------------------------------------

fn encode_array(
    value: &Value,
    elem: &ParsedType,
    col: &str,
    buf: &mut Vec<u8>,
) -> Result<(), DynamicError> {
    let arr = match value {
        Value::Array(a) => a,
        _ => return Err(enc_err(col, "expected array")),
    };
    write_varint(arr.len() as u64, buf);
    for item in arr {
        encode_value(item, elem, col, buf)?;
    }
    Ok(())
}

fn encode_map(
    value: &Value,
    _key: &ParsedType,
    val: &ParsedType,
    col: &str,
    buf: &mut Vec<u8>,
) -> Result<(), DynamicError> {
    let obj = match value {
        Value::Object(m) => m,
        _ => return Err(enc_err(col, "expected object for Map")),
    };
    write_varint(obj.len() as u64, buf);
    // ClickHouse Map keys are String on this path; write the key text directly.
    for (k, v) in obj {
        write_string(k.as_bytes(), buf);
        encode_value(v, val, col, buf)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Encode a one-row map against the given columns and return wire bytes.
    fn enc(row: Value, cols: &[(&str, &str)]) -> Vec<u8> {
        let columns: Vec<ColumnDef> = cols.iter().map(|(n, t)| ColumnDef::new(*n, *t)).collect();
        let obj = row.as_object().unwrap().clone();
        DynamicRow::new(&obj, &columns).encode().unwrap()
    }

    fn enc_err_of(row: Value, cols: &[(&str, &str)]) -> DynamicError {
        let columns: Vec<ColumnDef> = cols.iter().map(|(n, t)| ColumnDef::new(*n, *t)).collect();
        let obj = row.as_object().unwrap().clone();
        DynamicRow::new(&obj, &columns).encode().unwrap_err()
    }

    // ---- String / FixedString ----

    #[test]
    fn string() {
        assert_eq!(
            enc(json!({"s": "hello"}), &[("s", "String")]),
            vec![5, b'h', b'e', b'l', b'l', b'o']
        );
    }

    #[test]
    fn string_from_number() {
        assert_eq!(
            enc(json!({"s": 42}), &[("s", "String")]),
            vec![2, b'4', b'2']
        );
    }

    #[test]
    fn fixed_string_pad() {
        assert_eq!(
            enc(json!({"f": "ab"}), &[("f", "FixedString(4)")]),
            vec![b'a', b'b', 0, 0]
        );
    }

    #[test]
    fn fixed_string_truncate() {
        assert_eq!(
            enc(json!({"f": "abcdef"}), &[("f", "FixedString(3)")]),
            vec![b'a', b'b', b'c']
        );
    }

    // ---- Integers ----

    #[test]
    fn unsigned_widths() {
        assert_eq!(enc(json!({"x": 7}), &[("x", "UInt8")]), vec![7]);
        assert_eq!(
            enc(json!({"x": 300}), &[("x", "UInt16")]),
            300u16.to_le_bytes()
        );
        assert_eq!(
            enc(json!({"x": 42}), &[("x", "UInt32")]),
            42u32.to_le_bytes()
        );
        assert_eq!(
            enc(json!({"x": 42}), &[("x", "UInt64")]),
            42u64.to_le_bytes()
        );
    }

    #[test]
    fn signed_widths() {
        assert_eq!(
            enc(json!({"x": -5}), &[("x", "Int8")]),
            (-5i8).to_le_bytes()
        );
        assert_eq!(
            enc(json!({"x": -300}), &[("x", "Int16")]),
            (-300i16).to_le_bytes()
        );
        assert_eq!(
            enc(json!({"x": -1}), &[("x", "Int32")]),
            (-1i32).to_le_bytes()
        );
        assert_eq!(
            enc(json!({"x": -100}), &[("x", "Int64")]),
            (-100i64).to_le_bytes()
        );
    }

    #[test]
    fn int128_uint128() {
        assert_eq!(
            enc(json!({"x": -100}), &[("x", "Int128")]),
            (-100i128).to_le_bytes()
        );
        assert_eq!(
            enc(json!({"x": 100}), &[("x", "UInt128")]),
            100u128.to_le_bytes()
        );
        // From numeric string (large value beyond i64).
        let v: i128 = 170141183460469231731687303715884105727;
        assert_eq!(
            enc(json!({"x": v.to_string()}), &[("x", "Int128")]),
            v.to_le_bytes()
        );
    }

    #[test]
    fn int256_uint256() {
        let mut expect_neg = [0xFFu8; 32];
        expect_neg[..16].copy_from_slice(&(-1i128).to_le_bytes());
        assert_eq!(enc(json!({"x": -1}), &[("x", "Int256")]), expect_neg);

        let mut expect_pos = [0u8; 32];
        expect_pos[..16].copy_from_slice(&5u128.to_le_bytes());
        assert_eq!(enc(json!({"x": 5}), &[("x", "UInt256")]), expect_pos);
    }

    // ---- Floats / Bool ----

    #[test]
    fn floats() {
        assert_eq!(
            enc(json!({"f": 2.5}), &[("f", "Float64")]),
            2.5f64.to_le_bytes()
        );
        assert_eq!(
            enc(json!({"f": 1.5}), &[("f", "Float32")]),
            1.5f32.to_le_bytes()
        );
    }

    #[test]
    fn bool_native() {
        assert_eq!(enc(json!({"b": true}), &[("b", "Bool")]), vec![1]);
        assert_eq!(enc(json!({"b": false}), &[("b", "Bool")]), vec![0]);
    }

    #[test]
    fn bool_coercions() {
        for (v, want) in [
            (json!(1), 1u8),
            (json!(0), 0),
            (json!("true"), 1),
            (json!("false"), 0),
            (json!("yes"), 1),
            (json!("no"), 0),
            (json!("1"), 1),
            (json!("0"), 0),
        ] {
            assert_eq!(enc(json!({"b": v}), &[("b", "Bool")]), vec![want]);
        }
    }

    // ---- Date / DateTime ----

    #[test]
    fn date_from_string() {
        // 2024-12-25 = day 20082 since epoch.
        let days = civil_days_from_epoch(2024, 12, 25) as u16;
        assert_eq!(
            enc(json!({"d": "2024-12-25"}), &[("d", "Date")]),
            days.to_le_bytes()
        );
    }

    #[test]
    fn date_epoch_string() {
        assert_eq!(
            enc(json!({"d": "1970-01-01"}), &[("d", "Date")]),
            0u16.to_le_bytes()
        );
    }

    #[test]
    fn date32_from_string() {
        let days = civil_days_from_epoch(2024, 12, 25) as i32;
        assert_eq!(
            enc(json!({"d": "2024-12-25"}), &[("d", "Date32")]),
            days.to_le_bytes()
        );
    }

    #[test]
    fn datetime_from_string() {
        // 2024-12-25 10:30:00 UTC = 1735122600.
        assert_eq!(
            enc(json!({"ts": "2024-12-25 10:30:00"}), &[("ts", "DateTime")]),
            1_735_122_600u32.to_le_bytes()
        );
    }

    #[test]
    fn datetime_from_number_seconds() {
        assert_eq!(
            enc(json!({"ts": 1_735_122_600u64}), &[("ts", "DateTime")]),
            1_735_122_600u32.to_le_bytes()
        );
    }

    #[test]
    fn datetime_from_epoch_ms_magnitude() {
        // ms value gets scaled down to seconds.
        assert_eq!(
            enc(json!({"ts": 1_735_122_600_000i64}), &[("ts", "DateTime")]),
            1_735_122_600u32.to_le_bytes()
        );
    }

    // ---- DateTime64 ----

    #[test]
    fn datetime64_from_string_ms() {
        let expected: i64 = 1_775_545_380_095;
        assert_eq!(
            enc(
                json!({"ts": "2026-04-07 07:03:00.095"}),
                &[("ts", "DateTime64(3)")]
            ),
            expected.to_le_bytes()
        );
    }

    #[test]
    fn datetime64_iso8601_t_separator() {
        let expected: i64 = 1_775_545_380_095;
        assert_eq!(
            enc(
                json!({"ts": "2026-04-07T07:03:00.095Z"}),
                &[("ts", "DateTime64(3)")]
            ),
            expected.to_le_bytes()
        );
    }

    #[test]
    fn datetime64_from_number_passthrough() {
        let v: i64 = 1_775_545_380_095;
        assert_eq!(
            enc(json!({"ts": v}), &[("ts", "DateTime64(3)")]),
            v.to_le_bytes()
        );
    }

    #[test]
    fn datetime64_precision_6_and_9() {
        assert_eq!(
            enc(
                json!({"ts": "2026-04-07 07:03:00.095123"}),
                &[("ts", "DateTime64(6)")]
            ),
            1_775_545_380_095_123i64.to_le_bytes()
        );
        assert_eq!(
            enc(
                json!({"ts": "2026-04-07 07:03:00.095123456"}),
                &[("ts", "DateTime64(9)")]
            ),
            1_775_545_380_095_123_456i64.to_le_bytes()
        );
    }

    #[test]
    fn datetime64_rejects_nonzero_offset() {
        let e = enc_err_of(
            json!({"ts": "2026-04-07 07:03:00+05:30"}),
            &[("ts", "DateTime64(3)")],
        );
        assert!(format!("{e}").contains("non-UTC"), "got: {e}");
    }

    // ---- Decimal ----

    #[test]
    fn decimal64() {
        // 123.45 at scale 2 -> backing 12345.
        assert_eq!(
            enc(json!({"d": 123.45}), &[("d", "Decimal(18, 2)")]),
            12_345i64.to_le_bytes()
        );
    }

    #[test]
    fn decimal_concrete_widths() {
        assert_eq!(
            enc(json!({"d": 1.5}), &[("d", "Decimal32(2)")]),
            150i32.to_le_bytes()
        );
        assert_eq!(
            enc(json!({"d": 1.5}), &[("d", "Decimal64(2)")]),
            150i64.to_le_bytes()
        );
        assert_eq!(
            enc(json!({"d": 1.5}), &[("d", "Decimal128(2)")]),
            150i128.to_le_bytes()
        );
        let mut expect256 = [0u8; 32];
        expect256[..16].copy_from_slice(&150i128.to_le_bytes());
        assert_eq!(enc(json!({"d": 1.5}), &[("d", "Decimal256(2)")]), expect256);
    }

    // ---- UUID / IP ----

    #[test]
    fn uuid_hyphenated() {
        let bytes = enc(
            json!({"id": "12345678-1234-5678-1234-567812345678"}),
            &[("id", "UUID")],
        );
        let high = 0x1234_5678_1234_5678u64;
        let low = 0x1234_5678_1234_5678u64;
        let mut expected = high.to_le_bytes().to_vec();
        expected.extend_from_slice(&low.to_le_bytes());
        assert_eq!(bytes, expected);
    }

    #[test]
    fn uuid_hyphenless_matches_hyphenated() {
        let with = enc(
            json!({"id": "12345678-1234-5678-1234-567812345678"}),
            &[("id", "UUID")],
        );
        let without = enc(
            json!({"id": "12345678123456781234567812345678"}),
            &[("id", "UUID")],
        );
        assert_eq!(with, without);
    }

    #[test]
    fn ipv4_dotted() {
        let bytes = enc(json!({"ip": "1.2.3.4"}), &[("ip", "IPv4")]);
        let addr: Ipv4Addr = "1.2.3.4".parse().unwrap();
        assert_eq!(bytes, u32::from(addr).to_le_bytes());
    }

    #[test]
    fn ipv4_from_integer() {
        let addr: Ipv4Addr = "1.2.3.4".parse().unwrap();
        let int = u32::from(addr);
        let bytes = enc(json!({"ip": int}), &[("ip", "IPv4")]);
        assert_eq!(bytes, int.to_le_bytes());
    }

    #[test]
    fn ipv6() {
        let bytes = enc(json!({"ip": "::1"}), &[("ip", "IPv6")]);
        let addr: Ipv6Addr = "::1".parse().unwrap();
        assert_eq!(bytes, addr.octets());
    }

    #[test]
    fn ipv6_accepts_ipv4_literal() {
        // The column takes it -- toIPv6('172.17.3.4') is ::ffff:172.17.3.4 --
        // and rejecting it dropped every filebeat event carrying source.ip.
        let bytes = enc(json!({"ip": "172.17.3.4"}), &[("ip", "IPv6")]);
        let mapped: Ipv6Addr = "::ffff:172.17.3.4".parse().unwrap();
        assert_eq!(bytes, mapped.octets());
    }

    #[test]
    fn ipv6_v4_mapped_literal_matches_the_v4_form() {
        let from_v4 = enc(json!({"ip": "172.17.3.4"}), &[("ip", "IPv6")]);
        let from_mapped = enc(json!({"ip": "::ffff:172.17.3.4"}), &[("ip", "IPv6")]);
        assert_eq!(from_v4, from_mapped);
    }

    #[test]
    fn ipv6_nullable_accepts_ipv4_literal() {
        let bytes = enc(json!({"ip": "10.0.0.1"}), &[("ip", "Nullable(IPv6)")]);
        let mapped: Ipv6Addr = "::ffff:10.0.0.1".parse().unwrap();
        let mut expected = vec![0u8];
        expected.extend_from_slice(&mapped.octets());
        assert_eq!(bytes, expected);
    }

    #[test]
    fn ipv6_accepts_the_v4_mapped_all_zeroes_literal() {
        let bytes = enc(json!({"ip": "::ffff:0.0.0.0"}), &[("ip", "IPv6")]);
        let mapped: Ipv6Addr = "::ffff:0.0.0.0".parse().unwrap();
        assert_eq!(bytes, mapped.octets());
    }

    #[test]
    fn ipv6_rejects_a_non_address() {
        let err = enc_err_of(json!({"ip": "not-an-ip"}), &[("ip", "IPv6")]);
        assert!(err.to_string().contains("invalid IP address"), "{err}");
    }

    #[test]
    fn ipv6_rejects_bracketed_and_port_suffixed_forms() {
        // `[::1]` and `host:port` are URL spellings, not addresses.
        for literal in ["[::1]", "172.17.3.4:80"] {
            let err = enc_err_of(json!({"ip": literal}), &[("ip", "IPv6")]);
            assert!(
                err.to_string().contains("invalid IP address"),
                "{literal}: {err}"
            );
        }
    }

    #[test]
    fn ipv6_nullable_rejects_an_empty_string() {
        // Reachable only in Delta mode, where IPv6 is off the coercer's
        // allow-list; Full mode maps "" to null via the null_strings list.
        let err = enc_err_of(json!({"ip": ""}), &[("ip", "Nullable(IPv6)")]);
        assert!(err.to_string().contains("invalid IP address"), "{err}");
    }

    // ---- Enum ----

    #[test]
    fn enum8_and_enum16_numeric() {
        // Enum values are written as the underlying int discriminant. The
        // numeric discriminant is taken from the value directly.
        assert_eq!(
            enc(json!({"e": 2}), &[("e", "Enum8('a'=1,'b'=2)")]),
            vec![2]
        );
        assert_eq!(
            enc(json!({"e": 200}), &[("e", "Enum16('x'=100,'y'=200)")]),
            200i16.to_le_bytes()
        );
    }

    #[test]
    fn enum8_string_mapping() {
        // String maps to its discriminant from the type definition.
        assert_eq!(
            enc(
                json!({"e": "high"}),
                &[("e", "Enum8('low'=1, 'medium'=2, 'high'=3)")]
            ),
            vec![3]
        );
        assert_eq!(
            enc(json!({"e": "y"}), &[("e", "Enum16('x'=100, 'y'=200)")]),
            200i16.to_le_bytes()
        );
    }

    #[test]
    fn enum_unknown_string_errors() {
        let e = enc_err_of(json!({"e": "nope"}), &[("e", "Enum8('a'=1)")]);
        assert!(
            matches!(e, DynamicError::EncodingError { .. }),
            "got: {e:?}"
        );
    }

    // ---- Nullable ----

    #[test]
    fn nullable_null() {
        assert_eq!(
            enc(json!({"n": null}), &[("n", "Nullable(String)")]),
            vec![1]
        );
    }

    #[test]
    fn nullable_non_null() {
        assert_eq!(
            enc(json!({"n": "hi"}), &[("n", "Nullable(String)")]),
            vec![0, 2, b'h', b'i']
        );
    }

    #[test]
    fn nullable_datetime64() {
        let mut expected = vec![0u8];
        expected.extend_from_slice(&1_775_545_380_095i64.to_le_bytes());
        assert_eq!(
            enc(
                json!({"ts": "2026-04-07 07:03:00.095"}),
                &[("ts", "Nullable(DateTime64(3))")]
            ),
            expected
        );
    }

    #[test]
    fn non_nullable_null_gets_default() {
        assert_eq!(enc(json!({}), &[("x", "UInt32")]), 0u32.to_le_bytes());
        // Variable-length default is an empty string.
        assert_eq!(enc(json!({}), &[("s", "String")]), vec![0]);
    }

    // ---- LowCardinality ----

    #[test]
    fn low_cardinality_encodes_inner() {
        // LowCardinality(String) on the INSERT path is just the inner String.
        assert_eq!(
            enc(json!({"c": "x"}), &[("c", "LowCardinality(String)")]),
            vec![1, b'x']
        );
    }

    #[test]
    fn low_cardinality_nullable() {
        assert_eq!(
            enc(
                json!({"c": null}),
                &[("c", "LowCardinality(Nullable(String))")]
            ),
            vec![1]
        );
        assert_eq!(
            enc(
                json!({"c": "x"}),
                &[("c", "LowCardinality(Nullable(String))")]
            ),
            vec![0, 1, b'x']
        );
    }

    // ---- Array / Map ----

    #[test]
    fn array_uint32() {
        let mut expected = vec![3u8];
        for n in [1u32, 2, 3] {
            expected.extend_from_slice(&n.to_le_bytes());
        }
        assert_eq!(
            enc(json!({"a": [1, 2, 3]}), &[("a", "Array(UInt32)")]),
            expected
        );
    }

    #[test]
    fn array_nested() {
        // Array(Array(UInt8)): outer len 2, each inner len + bytes.
        let bytes = enc(json!({"a": [[1, 2], [3]]}), &[("a", "Array(Array(UInt8))")]);
        assert_eq!(bytes, vec![2, /*inner0*/ 2, 1, 2, /*inner1*/ 1, 3]);
    }

    #[test]
    fn array_nullable_elements() {
        // Array(Nullable(UInt8)): len 2, then per element null-marker + value.
        let bytes = enc(json!({"a": [5, null]}), &[("a", "Array(Nullable(UInt8))")]);
        assert_eq!(bytes, vec![2, 0, 5, 1]);
    }

    #[test]
    fn map_string_uint32() {
        let bytes = enc(json!({"m": {"a": 1}}), &[("m", "Map(String, UInt32)")]);
        let mut expected = vec![1u8]; // count
        expected.extend_from_slice(&[1, b'a']); // key "a"
        expected.extend_from_slice(&1u32.to_le_bytes()); // value
        assert_eq!(bytes, expected);
    }

    // ---- JSON ----

    #[test]
    fn json_from_object() {
        let bytes = enc(
            json!({"data": {"key": "value", "num": 42}}),
            &[("data", "JSON")],
        );
        let json_str = r#"{"key":"value","num":42}"#;
        let mut expected = Vec::new();
        write_varint(json_str.len() as u64, &mut expected);
        expected.extend_from_slice(json_str.as_bytes());
        assert_eq!(bytes, expected);
    }

    #[test]
    fn json_from_string_verbatim() {
        let payload = r#"{"event":"login","user":"alice"}"#;
        let bytes = enc(json!({"data": payload}), &[("data", "JSON")]);
        let mut expected = Vec::new();
        write_varint(payload.len() as u64, &mut expected);
        expected.extend_from_slice(payload.as_bytes());
        assert_eq!(bytes, expected);
    }

    #[test]
    fn json_missing_value_becomes_empty_object() {
        assert_eq!(enc(json!({}), &[("data", "JSON")]), vec![2, b'{', b'}']);
    }

    #[test]
    fn json_explicit_null_becomes_empty_object() {
        assert_eq!(
            enc(json!({"data": null}), &[("data", "JSON")]),
            vec![2, b'{', b'}']
        );
    }

    #[test]
    fn json_empty_string_becomes_empty_object() {
        assert_eq!(
            enc(json!({"data": ""}), &[("data", "JSON")]),
            vec![2, b'{', b'}']
        );
    }

    /// Assert the wire text a JSON column gets for one value.
    fn assert_json_text(value: Value, expected: &str) {
        let described = value.to_string();
        let bytes = enc(json!({ "data": value }), &[("data", "JSON")]);
        let mut want = Vec::new();
        write_varint(expected.len() as u64, &mut want);
        want.extend_from_slice(expected.as_bytes());
        assert_eq!(
            bytes, want,
            "JSON column text for {described} must be {expected}"
        );
    }

    #[test]
    fn json_array_is_wrapped_under_list() {
        assert_json_text(
            json!(["preserve_original_event", "forwarded"]),
            r#"{"list":["preserve_original_event","forwarded"]}"#,
        );
    }

    #[test]
    fn json_array_text_is_wrapped_under_list() {
        assert_json_text(json!(r#"["forwarded"]"#), r#"{"list":["forwarded"]}"#);
    }

    #[test]
    fn json_bare_string_is_wrapped_under_value() {
        assert_json_text(json!("forwarded"), r#"{"value":"forwarded"}"#);
    }

    #[test]
    fn json_number_is_wrapped_under_value() {
        assert_json_text(json!(42), r#"{"value":42}"#);
    }

    #[test]
    fn json_bool_is_wrapped_under_value() {
        assert_json_text(json!(true), r#"{"value":true}"#);
    }

    #[test]
    fn json_object_is_not_wrapped() {
        assert_json_text(json!({"env": "prod"}), r#"{"env":"prod"}"#);
    }

    #[test]
    fn json_broken_object_text_is_wrapped_under_value() {
        // Text that opens like an object but does not parse is stored as the
        // string it is; written verbatim it is the code 117 this rule stops.
        assert_json_text(json!("{not json"), r#"{"value":"{not json"}"#);
    }

    #[test]
    fn json_broken_array_text_is_wrapped_under_value() {
        assert_json_text(json!("[not json"), r#"{"value":"[not json"}"#);
    }

    #[test]
    fn json_object_text_with_trailing_content_is_wrapped_under_value() {
        assert_json_text(json!(r#"{"a":1} tail"#), r#"{"value":"{\"a\":1} tail"}"#);
    }

    // ------------------------------------------------------------------
    // The same rule at value level, for the JSONEachRow path
    // ------------------------------------------------------------------

    /// Assert what shaping leaves in the row map for one value.
    fn assert_shaped(value: Value, expected: Value) {
        let described = value.to_string();
        let mut shaped = value;
        shape_json_value(&mut shaped);
        assert_eq!(shaped, expected, "shaping {described} must give {expected}");
    }

    #[test]
    fn shaped_array_is_wrapped_under_list() {
        assert_shaped(
            json!(["preserve_original_event", "forwarded"]),
            json!({"list": ["preserve_original_event", "forwarded"]}),
        );
    }

    #[test]
    fn shaped_scalars_are_wrapped_under_value() {
        assert_shaped(json!("forwarded"), json!({"value": "forwarded"}));
        assert_shaped(json!(42), json!({"value": 42}));
        assert_shaped(json!(true), json!({"value": true}));
    }

    #[test]
    fn shaped_object_is_left_alone() {
        assert_shaped(json!({"env": "prod"}), json!({"env": "prod"}));
    }

    #[test]
    fn shaped_null_becomes_an_empty_object() {
        assert_shaped(Value::Null, json!({}));
        assert_shaped(json!(""), json!({}));
    }

    #[test]
    fn shaped_json_text_is_parsed_back() {
        // The JSONEachRow body carries structure, not text: a string would
        // reach the column as a string.
        assert_shaped(json!(r#"{"env":"prod"}"#), json!({"env": "prod"}));
        assert_shaped(json!(r#"["forwarded"]"#), json!({"list": ["forwarded"]}));
    }

    #[test]
    fn shaped_broken_object_text_is_wrapped_under_value() {
        assert_shaped(json!("{not json"), json!({"value": "{not json"}));
    }

    #[test]
    fn shaped_json_column_shapes_nested_json() {
        // Array(JSON) carries a JSON per element, so each element is shaped --
        // wrapping the array itself would bury every element under one key.
        let ty = ParsedType::parse("Array(JSON)");
        let mut value = json!([["forwarded"], {"env": "prod"}, 42]);
        assert!(json_shaping_changes(&value, &ty));
        shape_json_for_type(&mut value, &ty);
        assert_eq!(
            value,
            json!([{"list": ["forwarded"]}, {"env": "prod"}, {"value": 42}])
        );
    }

    #[test]
    fn shaped_map_of_json_shapes_each_value() {
        let ty = ParsedType::parse("Map(String, JSON)");
        let mut value = json!({"a": ["forwarded"], "b": {"env": "prod"}});
        assert!(json_shaping_changes(&value, &ty));
        shape_json_for_type(&mut value, &ty);
        assert_eq!(
            value,
            json!({"a": {"list": ["forwarded"]}, "b": {"env": "prod"}})
        );
    }

    #[test]
    fn shaping_a_parameterised_json_column_wraps_an_array() {
        let ty = ParsedType::parse("JSON(max_dynamic_paths=2048)");
        let mut value = json!(["forwarded"]);
        assert!(json_shaping_changes(&value, &ty));
        shape_json_for_type(&mut value, &ty);
        assert_eq!(value, json!({"list": ["forwarded"]}));
    }

    #[test]
    fn shaping_does_not_change_an_object_or_a_nullable_null() {
        let json = ParsedType::parse("JSON");
        assert!(!json_shaping_changes(&json!({"env": "prod"}), &json));

        // Nullable(JSON) keeps its null -- the encoder writes the null marker
        // and never reaches the JSON text.
        let nullable = ParsedType::parse("Nullable(JSON)");
        assert!(!json_shaping_changes(&Value::Null, &nullable));
        let mut value = Value::Null;
        shape_json_for_type(&mut value, &nullable);
        assert_eq!(value, Value::Null);

        // A plain String column is never shaped.
        let string = ParsedType::parse("String");
        assert!(!json_shaping_changes(&json!("[forwarded]"), &string));
    }

    #[test]
    fn nullable_json_null_and_non_null() {
        assert_eq!(enc(json!({"t": null}), &[("t", "Nullable(JSON)")]), vec![1]);
        let bytes = enc(json!({"t": {"env": "prod"}}), &[("t", "Nullable(JSON)")]);
        let json_str = r#"{"env":"prod"}"#;
        let mut expected = vec![0u8];
        write_varint(json_str.len() as u64, &mut expected);
        expected.extend_from_slice(json_str.as_bytes());
        assert_eq!(bytes, expected);
    }

    // ---- Raw passthrough ----

    #[test]
    fn raw_passthrough_for_json_column() {
        let columns = vec![
            ColumnDef::new("id", "UInt32"),
            ColumnDef::new("_json", "Nullable(JSON)"),
        ];
        let row = json!({"id": 1}).as_object().unwrap().clone();
        let raw = br#"{"event":"login","user":"alice"}"#;
        let bytes = DynamicRow::with_raw(&row, &columns, raw, "_json")
            .encode()
            .unwrap();

        let mut expected = Vec::new();
        expected.extend_from_slice(&1u32.to_le_bytes());
        expected.push(0); // not null
        write_varint(raw.len() as u64, &mut expected);
        expected.extend_from_slice(raw);
        assert_eq!(bytes, expected);
    }

    #[test]
    fn raw_passthrough_matches_value_path() {
        let columns = vec![ColumnDef::new("data", "JSON")];
        let payload = br#"{"key":"value","num":42}"#;

        let row_value = json!({"data": std::str::from_utf8(payload).unwrap()})
            .as_object()
            .unwrap()
            .clone();
        let via_value = DynamicRow::new(&row_value, &columns).encode().unwrap();

        let row_empty = json!({}).as_object().unwrap().clone();
        let via_raw = DynamicRow::with_raw(&row_empty, &columns, payload, "data")
            .encode()
            .unwrap();

        assert_eq!(via_value, via_raw);
    }

    // ---- Multi-column ----

    #[test]
    fn multi_column_order() {
        let bytes = enc(
            json!({"id": 42, "name": "test"}),
            &[("id", "UInt32"), ("name", "String")],
        );
        let mut expected = Vec::new();
        expected.extend_from_slice(&42u32.to_le_bytes());
        expected.extend_from_slice(&[4, b't', b'e', b's', b't']);
        assert_eq!(bytes, expected);
    }

    // ---- Unsupported types ----

    #[test]
    fn tuple_is_unsupported() {
        let e = enc_err_of(json!({"t": [1, 2]}), &[("t", "Tuple(UInt8, UInt8)")]);
        assert!(
            matches!(e, DynamicError::UnsupportedType { .. }),
            "got: {e:?}"
        );
    }

    #[test]
    fn variant_is_unsupported() {
        let e = enc_err_of(json!({"v": 1}), &[("v", "Variant(UInt8, String)")]);
        assert!(
            matches!(e, DynamicError::UnsupportedType { .. }),
            "got: {e:?}"
        );
    }

    #[test]
    fn point_is_unsupported() {
        let e = enc_err_of(json!({"p": [1.0, 2.0]}), &[("p", "Point")]);
        assert!(
            matches!(e, DynamicError::UnsupportedType { .. }),
            "got: {e:?}"
        );
    }

    // ---- varint ----

    #[test]
    fn varint_boundaries() {
        let mut buf = Vec::new();
        write_varint(0, &mut buf);
        assert_eq!(buf, vec![0]);
        buf.clear();
        write_varint(127, &mut buf);
        assert_eq!(buf, vec![127]);
        buf.clear();
        write_varint(128, &mut buf);
        assert_eq!(buf, vec![0x80, 0x01]);
        buf.clear();
        write_varint(300, &mut buf);
        assert_eq!(buf, vec![0xAC, 0x02]);
    }

    // ---- civil days ----

    #[test]
    fn civil_days_known() {
        assert_eq!(civil_days_from_epoch(1970, 1, 1), 0);
        assert_eq!(civil_days_from_epoch(2026, 4, 7), 20_550);
        assert_eq!(civil_days_from_epoch(1969, 12, 31), -1);
    }
}
