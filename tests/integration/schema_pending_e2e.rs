// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! End-to-end regression test for GitHub issue #36 against a real ClickHouse.
//!
//! Proves the full path that the bug broke: a real `@renamed` column COMMENT
//! in ClickHouse DDL is fetched, parsed into a directive, and applied by the
//! `HeaderExtractor` (the json_primary path) so the renamed column is
//! populated. The bug: a schema cache miss silently used the transformer
//! path, which ignores `@renamed`, leaving the column NULL.

use dfe_loader::column_meta::{ColumnDirectivesConfig, ColumnMetaCache, parse_directives};
use dfe_loader::config::{MetadataConfig, RoutingConfig};
use dfe_loader::transform::HeaderExtractor;
use rustc_hash::FxHashMap;

use crate::common::{create_http_test_client, drop_http_test_table, unique_table_name};
use crate::skip_if_no_clickhouse;

/// Real DDL `@renamed` comment → real schema/comment fetch → directive parse →
/// extractor applies the rename. This is the data that #36 was NULLing.
#[tokio::test]
async fn renamed_directive_applied_end_to_end_36() {
    skip_if_no_clickhouse!();

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };

    let table = unique_table_name("pending_renamed");
    let oc = crate::common::on_cluster_clause();

    // `dst_field` is renamed from the source field `src_field` via a DDL
    // column COMMENT — exactly how DFE schemas express `@renamed`.
    let ddl = format!(
        "CREATE TABLE {table}{oc} (
            id UInt64,
            dst_field String COMMENT '@renamed: src_field'
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("create table");

    // Fetch the real schema + column comments, exactly as the pre-warm /
    // background resolver does.
    let schema = client
        .fetch_table_schema(&table)
        .await
        .expect("fetch schema");
    let comments = client
        .fetch_column_comments(&table)
        .await
        .expect("fetch column comments");

    // Sanity: ClickHouse actually returned our @renamed comment.
    assert!(
        comments
            .iter()
            .any(|(col, c)| col == "dst_field" && c.contains("@renamed")),
        "expected dst_field @renamed comment from ClickHouse, got {comments:?}"
    );

    // Build the column-meta cache the extractor reads (DDL layer), parsing the
    // real comments into directives.
    let col_meta = ColumnMetaCache::new(ColumnDirectivesConfig::default());
    let directives: FxHashMap<String, _> = comments
        .into_iter()
        .map(|(col, comment)| (col, parse_directives(&comment)))
        .collect();
    col_meta.apply_ddl(&table, directives);

    // Run the json_primary extractor path on a message carrying the SOURCE
    // field name. Pre-#36-fix this column would be NULL (transformer path).
    let extractor = HeaderExtractor::new(&MetadataConfig::default(), &RoutingConfig::default());
    let raw = br#"{"src_field":"hello","id":7}"#;
    let map = extractor.extract(raw, &table, &schema, &col_meta).fields;

    assert_eq!(
        map.get("dst_field"),
        Some(&serde_json::Value::String("hello".to_string())),
        "real @renamed column comment must map src_field → dst_field"
    );
    assert!(
        !map.contains_key("src_field"),
        "source field should be renamed away, not promoted verbatim"
    );

    drop_http_test_table(&client, &table).await;
}
