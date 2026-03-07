// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Remap file loading for field mapping rules.
//!
//! Supports three formats:
//! - **CSV** (ecs-mapper compatible): `source_field,destination_field,copy_action`
//! - **YAML**: structured with multi-source-per-destination support
//! - **JSON**: same structure as YAML
//!
//! Also provides built-in presets embedded at compile time.

use std::collections::HashMap;
use std::path::Path;

use serde::Deserialize;

use super::field_mapping::{FieldMappingRule, MappingAction, RuleOrigin};

// ============================================================================
// Built-in Presets (embedded at compile time)
// ============================================================================

const BUILTIN_ECS: &str = include_str!("../../mappings/ecs.yaml");
const BUILTIN_OCSF: &str = include_str!("../../mappings/ocsf.yaml");
const BUILTIN_CIM_TO_ECS: &str = include_str!("../../mappings/cim_to_ecs.csv");
const BUILTIN_BEATS_LEGACY: &str = include_str!("../../mappings/beats_legacy.csv");

/// Built-in mapping presets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuiltinPreset {
    /// Common raw field names → ECS
    Ecs,
    /// Common raw field names → OCSF
    Ocsf,
    /// Splunk CIM → ECS crosswalk
    Cim,
    /// Pre-7.0 Beats → ECS
    Beats,
}

impl BuiltinPreset {
    /// Parse from config string.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "ecs" => Some(Self::Ecs),
            "ocsf" => Some(Self::Ocsf),
            "cim" | "splunk" | "splunk_cim" => Some(Self::Cim),
            "beats" | "beats_legacy" => Some(Self::Beats),
            _ => None,
        }
    }
}

/// Load rules from a built-in preset.
pub fn load_builtin(
    preset: BuiltinPreset,
    default_action: MappingAction,
) -> crate::Result<Vec<FieldMappingRule>> {
    let origin_name = match preset {
        BuiltinPreset::Ecs => "builtin:ecs",
        BuiltinPreset::Ocsf => "builtin:ocsf",
        BuiltinPreset::Cim => "builtin:cim",
        BuiltinPreset::Beats => "builtin:beats",
    };

    match preset {
        BuiltinPreset::Ecs => load_yaml_str(BUILTIN_ECS, default_action, origin_name),
        BuiltinPreset::Ocsf => load_yaml_str(BUILTIN_OCSF, default_action, origin_name),
        BuiltinPreset::Cim => load_csv_str(BUILTIN_CIM_TO_ECS, default_action, origin_name),
        BuiltinPreset::Beats => load_csv_str(BUILTIN_BEATS_LEGACY, default_action, origin_name),
    }
}

// ============================================================================
// File Loading (auto-detect format by extension)
// ============================================================================

/// Load mapping rules from a file, auto-detecting format by extension.
///
/// - `.csv` → ecs-mapper CSV format
/// - `.yaml` / `.yml` → YAML format
/// - `.json` → JSON format
pub fn load_file(
    path: &str,
    default_action: MappingAction,
) -> crate::Result<Vec<FieldMappingRule>> {
    let content = std::fs::read_to_string(path).map_err(|e| {
        crate::Error::Config(format!("Failed to read remap file '{}': {}", path, e))
    })?;

    let ext = Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");

    match ext {
        "csv" => load_csv_str(&content, default_action, path),
        "yaml" | "yml" => load_yaml_str(&content, default_action, path),
        "json" => load_json_str(&content, default_action, path),
        _ => Err(crate::Error::Config(format!(
            "Unsupported remap file format '{}' for '{}'. Use .csv, .yaml, .yml, or .json",
            ext, path
        ))),
    }
}

// ============================================================================
// CSV Loading (ecs-mapper compatible)
// ============================================================================

/// CSV remap row (matches elastic/ecs-mapper format).
#[derive(Debug, Deserialize)]
struct CsvRow {
    source_field: String,
    destination_field: String,
    #[serde(default)]
    copy_action: Option<String>,
}

/// Load rules from CSV string (ecs-mapper format).
///
/// Columns: `source_field`, `destination_field`, `copy_action` (optional).
/// Multiple rows with the same destination create a multi-source rule (first() semantics).
fn load_csv_str(
    content: &str,
    default_action: MappingAction,
    origin: &str,
) -> crate::Result<Vec<FieldMappingRule>> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .trim(csv::Trim::All)
        .from_reader(content.as_bytes());

    // Group rows by destination for multi-source (first()) semantics
    let mut dest_map: HashMap<String, (Vec<String>, MappingAction)> = HashMap::new();
    // Track insertion order
    let mut dest_order: Vec<String> = Vec::new();

    for result in reader.deserialize() {
        let row: CsvRow = result
            .map_err(|e| crate::Error::Config(format!("CSV parse error in '{}': {}", origin, e)))?;

        // Skip rows with empty source or destination
        if row.source_field.trim().is_empty() || row.destination_field.trim().is_empty() {
            continue;
        }

        let action = row
            .copy_action
            .as_deref()
            .map(MappingAction::parse)
            .unwrap_or(default_action);

        let dest = row.destination_field.trim().to_string();
        let source = row.source_field.trim().to_string();

        let entry = dest_map.entry(dest.clone()).or_insert_with(|| {
            dest_order.push(dest.clone());
            (Vec::new(), action)
        });
        entry.0.push(source);
    }

    // Build rules in insertion order
    let rules = dest_order
        .into_iter()
        .filter_map(|dest| {
            dest_map
                .remove(&dest)
                .map(|(sources, action)| FieldMappingRule {
                    source_fields: sources,
                    destination: dest,
                    action,
                    origin: RuleOrigin::ExternalFile(origin.to_string()),
                })
        })
        .collect();

    Ok(rules)
}

// ============================================================================
// YAML Loading
// ============================================================================

/// YAML remap file structure.
#[derive(Debug, Deserialize)]
struct YamlRemapFile {
    mappings: HashMap<String, YamlRemapEntry>,
}

/// Single entry in a YAML remap file.
#[derive(Debug, Deserialize)]
struct YamlRemapEntry {
    sources: Vec<String>,
    #[serde(default)]
    action: Option<String>,
}

/// Load rules from YAML string.
fn load_yaml_str(
    content: &str,
    default_action: MappingAction,
    origin: &str,
) -> crate::Result<Vec<FieldMappingRule>> {
    let file: YamlRemapFile = serde_yaml_ng::from_str(content)
        .map_err(|e| crate::Error::Config(format!("YAML parse error in '{}': {}", origin, e)))?;

    let rules = file
        .mappings
        .into_iter()
        .map(|(dest, entry)| FieldMappingRule {
            source_fields: entry.sources,
            destination: dest,
            action: entry
                .action
                .as_deref()
                .map(MappingAction::parse)
                .unwrap_or(default_action),
            origin: RuleOrigin::ExternalFile(origin.to_string()),
        })
        .collect();

    Ok(rules)
}

// ============================================================================
// JSON Loading
// ============================================================================

/// Load rules from JSON string (same structure as YAML).
fn load_json_str(
    content: &str,
    default_action: MappingAction,
    origin: &str,
) -> crate::Result<Vec<FieldMappingRule>> {
    let file: YamlRemapFile = serde_json::from_str(content)
        .map_err(|e| crate::Error::Config(format!("JSON parse error in '{}': {}", origin, e)))?;

    let rules = file
        .mappings
        .into_iter()
        .map(|(dest, entry)| FieldMappingRule {
            source_fields: entry.sources,
            destination: dest,
            action: entry
                .action
                .as_deref()
                .map(MappingAction::parse)
                .unwrap_or(default_action),
            origin: RuleOrigin::ExternalFile(origin.to_string()),
        })
        .collect();

    Ok(rules)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_csv_ecs_mapper_format() {
        let csv = "\
source_field,destination_field,copy_action
src_ip,source.ip,rename
srcip,source.ip,rename
dest_ip,destination.ip,copy
";
        let rules = load_csv_str(csv, MappingAction::Rename, "test.csv").unwrap();

        // src_ip and srcip should be merged into one rule for source.ip
        assert_eq!(rules.len(), 2);

        let source_rule = rules.iter().find(|r| r.destination == "source.ip").unwrap();
        assert_eq!(source_rule.source_fields, vec!["src_ip", "srcip"]);
        assert_eq!(source_rule.action, MappingAction::Rename);

        let dest_rule = rules
            .iter()
            .find(|r| r.destination == "destination.ip")
            .unwrap();
        assert_eq!(dest_rule.source_fields, vec!["dest_ip"]);
        assert_eq!(dest_rule.action, MappingAction::Copy);
    }

    #[test]
    fn test_load_csv_default_action() {
        let csv = "\
source_field,destination_field
src_ip,source.ip
";
        let rules = load_csv_str(csv, MappingAction::Copy, "test.csv").unwrap();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].action, MappingAction::Copy);
    }

    #[test]
    fn test_load_csv_skip_empty_rows() {
        let csv = "\
source_field,destination_field,copy_action
src_ip,source.ip,rename
,,
,destination.ip,rename
";
        let rules = load_csv_str(csv, MappingAction::Rename, "test.csv").unwrap();
        assert_eq!(rules.len(), 1);
    }

    #[test]
    fn test_load_yaml_format() {
        let yaml = r#"
mappings:
  source.ip:
    sources: [src_ip, srcip, source_ip]
    action: rename
  destination.ip:
    sources: [dst_ip, dstip]
"#;
        let rules = load_yaml_str(yaml, MappingAction::Rename, "test.yaml").unwrap();
        assert_eq!(rules.len(), 2);

        let source_rule = rules.iter().find(|r| r.destination == "source.ip").unwrap();
        assert_eq!(
            source_rule.source_fields,
            vec!["src_ip", "srcip", "source_ip"]
        );
        assert_eq!(source_rule.action, MappingAction::Rename);
    }

    #[test]
    fn test_load_json_format() {
        let json = r#"{
  "mappings": {
    "source.ip": {
      "sources": ["src_ip", "srcip"],
      "action": "copy"
    }
  }
}"#;
        let rules = load_json_str(json, MappingAction::Rename, "test.json").unwrap();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].action, MappingAction::Copy);
    }

    #[test]
    fn test_builtin_ecs() {
        let rules = load_builtin(BuiltinPreset::Ecs, MappingAction::Rename).unwrap();
        assert!(!rules.is_empty(), "ECS preset should have rules");

        // Verify a known mapping exists
        let has_source_ip = rules.iter().any(|r| r.destination == "source.ip");
        assert!(has_source_ip, "ECS preset should map source.ip");
    }

    #[test]
    fn test_builtin_cim() {
        let rules = load_builtin(BuiltinPreset::Cim, MappingAction::Rename).unwrap();
        assert!(!rules.is_empty(), "CIM preset should have rules");
    }

    #[test]
    fn test_builtin_beats() {
        let rules = load_builtin(BuiltinPreset::Beats, MappingAction::Rename).unwrap();
        assert!(!rules.is_empty(), "Beats preset should have rules");
    }

    #[test]
    fn test_builtin_ocsf() {
        let rules = load_builtin(BuiltinPreset::Ocsf, MappingAction::Rename).unwrap();
        assert!(!rules.is_empty(), "OCSF preset should have rules");

        // Verify known OCSF mappings exist
        let has_src_endpoint_ip = rules.iter().any(|r| r.destination == "src_endpoint.ip");
        assert!(
            has_src_endpoint_ip,
            "OCSF preset should map src_endpoint.ip"
        );

        let has_dst_endpoint_ip = rules.iter().any(|r| r.destination == "dst_endpoint.ip");
        assert!(
            has_dst_endpoint_ip,
            "OCSF preset should map dst_endpoint.ip"
        );

        let has_actor_user_name = rules.iter().any(|r| r.destination == "actor.user.name");
        assert!(
            has_actor_user_name,
            "OCSF preset should map actor.user.name"
        );
    }

    #[test]
    fn test_builtin_preset_parse() {
        assert_eq!(BuiltinPreset::parse("ecs"), Some(BuiltinPreset::Ecs));
        assert_eq!(BuiltinPreset::parse("ECS"), Some(BuiltinPreset::Ecs));
        assert_eq!(BuiltinPreset::parse("ocsf"), Some(BuiltinPreset::Ocsf));
        assert_eq!(BuiltinPreset::parse("OCSF"), Some(BuiltinPreset::Ocsf));
        assert_eq!(BuiltinPreset::parse("cim"), Some(BuiltinPreset::Cim));
        assert_eq!(BuiltinPreset::parse("splunk"), Some(BuiltinPreset::Cim));
        assert_eq!(BuiltinPreset::parse("beats"), Some(BuiltinPreset::Beats));
        assert_eq!(BuiltinPreset::parse("none"), None);
        assert_eq!(BuiltinPreset::parse(""), None);
    }
}
