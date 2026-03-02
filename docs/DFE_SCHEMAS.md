# dfe-schemas — Shared Schema Definitions

Shared YAML definitions for common headers, hunt detection columns, and
other DFE-wide data structures. Lives in the
[`hyperi-io/dfe-schemas`](https://github.com/hyperi-io/dfe-schemas) repo,
consumed as a git submodule by both **dfe-engine** (Python) and
**dfe-loader** (Rust).

## Why a shared repo?

dfe-engine and dfe-loader must agree on the common header schema. Previously
each project carried its own copy under `schemas/profiles/`. A single
submodule eliminates drift — change the profile once, both consumers pick it
up on next `git submodule update`.

## Repo structure

```
dfe-schemas/
├── common-header/
│   ├── timeseries.yaml   # 9 columns — default for event ingestion
│   ├── minimal.yaml      # 4 columns — high-volume structured data
│   └── passthrough.yaml  # 4 columns — transparent bridge mode
├── hunt-results/
│   └── detection.yaml    # 6 hunt detection output columns
├── VERSION                # SemVer (e.g. 1.0.0)
└── README.md
```

### Column YAML format

```yaml
columns:
  - name: _timestamp
    type: datetime
    use_case: range
    order: 1
    comment: "@source: timestamp | now()"
```

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Column name (underscore prefix for system fields) |
| `type` | Yes | Primitive type: `string`, `text`, `integer`, `float`, `boolean`, `datetime`, `timestamp`, `date`, `ip`, `uuid`, `json`, `geo_point`, `enum` |
| `use_case` | No | `range`, `dimension`, `text_search`, `fulltext` |
| `attribute` | No | List: `[lowcardinality]` |
| `order` | No | Position in ORDER BY (0-based) |
| `default` | No | ClickHouse DEFAULT expression |
| `comment` | No | Source mapping annotation |

## Adding the submodule to dfe-loader

From the dfe-loader repo root:

```bash
git submodule add https://github.com/hyperi-io/dfe-schemas.git schemas
git commit -m "chore: add dfe-schemas submodule"
```

After cloning dfe-loader:

```bash
git submodule update --init --recursive
```

The profiles will be available at `schemas/common-header/*.yaml`.

## Using in Rust (dfe-loader)

The loader already reads profile YAML from `schemas/profiles/`. To switch
to the submodule:

1. Point the profile loader at `schemas/common-header/` (the submodule path)
   instead of the legacy `schemas/profiles/`.

2. Keep `schemas/profiles/` as a bundled fallback for cases where the
   submodule isn't checked out (e.g. `cargo install` from crates.io).

Resolution order:

```
1. DFE_SCHEMAS_DIR env var  →  {dir}/common-header/
2. schemas/common-header/   →  submodule checkout
3. schemas/profiles/        →  bundled fallback
```

Example Rust resolution:

```rust
fn resolve_profiles_dir() -> PathBuf {
    // 1. Env var
    if let Ok(dir) = std::env::var("DFE_SCHEMAS_DIR") {
        let candidate = PathBuf::from(dir).join("common-header");
        if candidate.is_dir() {
            return candidate;
        }
    }

    // 2. Submodule
    let submodule = PathBuf::from("schemas/common-header");
    if submodule.is_dir() {
        return submodule;
    }

    // 3. Bundled fallback
    PathBuf::from("schemas/profiles")
}
```

## Shipped schemas are read-only

The files in `dfe-schemas` are **shipped defaults** — they should not be
modified in place. If a user needs different common header behaviour:

- Create a custom profile YAML in a separate directory
- Point `profiles.custom_dir` (in loader config) to that directory
- Reference the custom profile by name

dfe-engine enforces this via `is_shipped_schema()` which checks whether a
path resolves inside the submodule or bundled profiles directory.

## Updating the schemas

Changes go through the `hyperi-io/dfe-schemas` repo:

```bash
cd schemas                     # enter the submodule
git checkout -b my-change
# edit YAML files
git commit -am "feat: add _source_ip to timeseries"
git push origin my-change
# open PR on hyperi-io/dfe-schemas, merge to main
```

Then update the submodule pin in each consumer:

```bash
cd /projects/dfe-loader        # or dfe-engine
git submodule update --remote schemas
git add schemas
git commit -m "chore: update dfe-schemas to latest"
```

## Keeping bundled profiles in sync

Both dfe-engine and dfe-loader keep a **bundled copy** of the profiles
(inside the package) as a fallback when the submodule isn't checked out.
After updating `dfe-schemas`, copy the changed YAML files to the bundled
location:

- **dfe-engine:** `src/dfe_engine/schema/profiles/`
- **dfe-loader:** `schemas/profiles/`

This ensures `pip install dfe-engine` and `cargo install dfe-loader` work
without requiring a submodule checkout.

## Version coordination

`dfe-schemas/VERSION` contains the SemVer version. Both consumers should
pin to the same commit/version. Breaking changes (column removal, type
change) bump the major version — consumers must coordinate upgrades.

| Change type | Version bump | Example |
|-------------|-------------|---------|
| Add column | Minor | Add `_source_ip` to timeseries |
| Change default | Minor | Change `_uuid` default expression |
| Remove column | **Major** | Remove `_tags` from timeseries |
| Change type | **Major** | `_json` from `JSON` to `String` |

## Environment variable

`DFE_SCHEMAS_DIR` overrides the submodule path. Useful for:

- CI environments where submodules aren't checked out
- Development against a local fork of dfe-schemas
- Docker builds with schemas mounted at a non-standard path

```bash
export DFE_SCHEMAS_DIR=/opt/dfe/schemas
```

## Related docs

- [COMMON-HEADER.md](./COMMON-HEADER.md) — detailed common header column reference
- [dfe-schemas README](https://github.com/hyperi-io/dfe-schemas)
