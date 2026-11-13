# Contributing

We welcome contributions to this project. By contributing, you agree to the
terms outlined below.

## Commit Message Format

HyperI projects use [Conventional Commits](https://www.conventionalcommits.org/)
and [semantic-release](https://semantic-release.gitbook.io/) for automated
versioning and changelog generation. All commits must follow this format:

```text
<type>(<scope>): <subject>

[optional body]

[optional footer(s)]
```

### Types

| Type | Description | Version Bump |
|------|-------------|--------------|
| `feat` | A new feature | Minor (0.X.0) |
| `fix` | A bug fix | Patch (0.0.X) |
| `docs` | Documentation only | None |
| `style` | Code style (formatting, semicolons, etc.) | None |
| `refactor` | Code change that neither fixes a bug nor adds a feature | None |
| `perf` | Performance improvement | Patch (0.0.X) |
| `test` | Adding or correcting tests | None |
| `build` | Changes to build system or dependencies | None |
| `ci` | Changes to CI configuration | None |
| `chore` | Other changes that don't modify src or test files | None |
| `revert` | Reverts a previous commit | Varies |

### Breaking Changes

For breaking changes that require a major version bump, add `!` after the type
or include `BREAKING CHANGE:` in the footer:

```text
feat!: remove deprecated API endpoints

BREAKING CHANGE: The /v1/users endpoint has been removed. Use /v2/users instead.
```

### Examples

```text
feat(auth): add OAuth2 support for Google login

fix(api): handle null response from upstream service

docs: update installation instructions for Windows

refactor(core)!: restructure module exports

BREAKING CHANGE: Named exports are now used instead of default exports.
```

### Scope

Scope is optional but recommended. Use it to indicate the area of the codebase
affected (e.g., `api`, `auth`, `core`, `cli`, `docs`).

## Semantic Versioning

This project follows [Semantic Versioning 2.0.0](https://semver.org/):

- **MAJOR** (X.0.0): Breaking changes that require users to modify their code
- **MINOR** (0.X.0): New features that are backwards-compatible
- **PATCH** (0.0.X): Bug fixes and minor improvements

Versions are automatically determined by semantic-release based on commit
messages. Do not manually update version numbers.

## Developer Certificate of Origin

This project uses the Developer Certificate of Origin (DCO) to ensure that
contributors have the right to submit their contributions.

By making a contribution to this project, you certify that:

1. The contribution was created in whole or in part by you and you have the
   right to submit it under the license indicated in the file; or

2. The contribution is based upon previous work that, to the best of your
   knowledge, is covered under an appropriate open source license and you
   have the right under that license to submit that work with modifications;
   or

3. The contribution was provided directly to me by some other person who
   certified (1), (2) or (3) and you have not modified it.

4. You understand and agree that this project and the contribution are public
   and that a record of the contribution (including all personal information
   you submit with it, including your sign-off) is maintained indefinitely
   and may be redistributed consistent with this project or the license(s)
   involved.

## How to Sign Off Your Commits

You must sign off each commit to indicate your acceptance of the DCO. Combine
the signoff with your conventional commit message:

```bash
git commit --signoff -m "feat(auth): add two-factor authentication"
```

This produces:

```text
feat(auth): add two-factor authentication

Signed-off-by: Your Name <your.email@example.com>
```

Make sure your Git configuration has your correct name and email:

```bash
git config --global user.name "Your Name"
git config --global user.email "your.email@example.com"
```

## License for Contributions

All contributions to this project are licensed under the Business Source
License 1.1 (BUSL-1.1), the same license that covers the project.

Each version of the software (including your contributions) will automatically
become available under the Apache License, Version 2.0 on the third
anniversary of its release.

## How to Contribute

1. **Fork the repository** and create your branch from `main`
2. **Make your changes** following the commit message format above
3. **Sign off your commits** with the DCO
4. **Test your changes** to ensure they work as expected
5. **Submit a pull request** with a clear description of what you've done

### Pull Request Checklist

- [ ] Commits follow the conventional commit format
- [ ] All commits are signed off (DCO)
- [ ] Tests pass (if applicable)
- [ ] Documentation is updated (if applicable)

## Run the checks locally before you push

HyperI projects gate every push through CI (lint, format, tests, secret scan,
dependency audit, build). Run the SAME checks locally first so your change lands
green instead of bouncing:

```bash
hyperi-ci check
```

`hyperi-ci` is the HyperI CI CLI (public on PyPI). If you do not have it, install
it once with `uv tool install hyperi-ci` (or `pipx install hyperi-ci`), or run it
ad hoc with `uvx hyperi-ci check`. It runs the project's full local validation --
the same suite CI runs -- and reports what to fix. Run it before every push and before opening a
pull request. It is the single best way to give your change the best chance of
surviving CI.

## CI/CD Workflow

When your pull request is merged to `main`:

1. **semantic-release** analyses commit messages since the last release
2. Determines the next version number based on commit types
3. Generates/updates the CHANGELOG
4. Creates a new GitHub release with release notes
5. Publishes the package (if applicable)

This happens automatically - no manual intervention required.

## Questions

If you have questions about contributing, please open an issue or contact us.

## Context

### What this is

dfe-loader writes rows into ClickHouse -- the last hop of the DFE core data path and the only component that writes there.

NOT a transform stage, NOT a schema manager. Its only outbound topic is the DLQ. dfe-engine creates the destination tables, and the loader holds records pending schema rather than issuing DDL. The dynamic insert comes from `dfe-clickhouse`'s `dynamic` module, because the `clickhouse` crate keeps insertion compile-time typed.

### Where things live

| Path | Holds |
|------|-------|
| `src/main.rs`, `src/config/` | CLI entry, config structs, `Config::validate()`, flat env |
| `src/pipeline/` | Orchestrator and per-message processor -- the hot path |
| `src/routing/`, `src/transform/`, `src/enrich/` | Routing, coercion and capture, enrichment |
| `src/buffer/`, `src/clickhouse/` | Per-table buffers, and the query client, schema cache and inserter |
| `tests/` | `smoke.rs`, `integration/`, `e2e/` -- see `tests/TESTING.md` |

### Commands that prove a change

```bash
make check                                  # hyperi-ci check -- the full local gate
cargo nextest run --lib                     # unit, no infra
cargo nextest run --test smoke              # startup smoke, no infra
cargo nextest run --test integration_tests  # integration, some cases need ClickHouse
cargo nextest run -- --ignored              # e2e, needs Kafka AND ClickHouse
```

Green lies three ways: `skip_if_no_clickhouse!()` and `skip_if_no_kafka!()` return early and PASS when the datastore or broker is absent, the e2e suite is `#[ignore]` so a plain run never touches the real insert path, and `default = []` compiles a feature-gated test out while CI still reports green. `ci.yml` sets `paths-ignore` for `docs/**` and `**.md`, so a docs-only push runs no jobs.

### What tends to bite

| Don't | Do | Why |
|-------|----|-----|
| Bump scalo and leave `release.helm.library` behind | Move `release.helm.library` in `.hyperi-ci.yaml` to the same scalo version | The release assembles the chart from the emitted contract on that scalo-service version, and a scalo-service release ships the schema for only the contract version its scalo release writes |
| Gate a test behind a cargo feature without adding it to `.hyperi-ci.yaml` | Add `default,<feature>` to the feature-set list | `default = []` (`Cargo.toml`), so it compiles out of every CI run and CI still reports green. `helm_contract.rs:64` asserts `transport-memory` and `testcontainers` stay listed |
| Inject a nested config key as `DFE_LOADER_SECTION_FIELD` | `DFE_LOADER__SECTION__FIELD` | figment strips exactly `DFE_LOADER_` and the double underscore separates nested keys. A single-underscore nested name matches no field and is dropped, which `tests/integration/config_reachability.rs` checks |
| Set `clickhouse.protocol: native` with `insert_format: json_each_row` | `rowbinary` over native, or `http` for JSONEachRow | JSONEachRow is an HTTP body format. `validate()` rejects the pair by name |
| Name `clickhouse.tls.ca_cert_file`, `cert_file`, `key_file` or `skip_verify` | Set `tls.enabled`, mount the CA into the trust store | Only `enabled` reaches a client, and `validate()` rejects the certificate-file and `skip_verify` keys by name |
| Widen the `cel` range past scalo's | Keep it on `>=0.14.5, <0.15` | `cel::Program` crosses the scalo boundary. Wider resolves two semver-incompatible `cel` crates and `Program` stops being the same type (`Cargo.toml:46-51`) |
| Take `clickhouse` from git or a `[patch]` | The crates.io release | `deny.toml` allows no git source, and `dfe-clickhouse` builds against the published `clickhouse` only |
| Mechanically sync dfe-engine's loader validation to `Config::validate()` | Read both, keep the divergence | dfe-engine scopes the broker check to the Kafka transport and adds a `grpc.listen` check this side lacks. A blind sync rejects valid gRPC-only configs at author time |

### Where this sits

Inbound, declared in `dfe-infra/suite.yaml`:

- **scalo-rs -> dfe-loader** (`cargo-dep`) -- `Cargo.toml:36` takes `scalo` by range for transports, config cascade, CLI, metrics, deployment contract and DLQ. A scalo release reaches this repo here.
- **scalo-rs -> dfe-loader** (`generated-file`, lockstep) -- `Dockerfile` is emitted by `scalo::deployment::generate_dockerfile()` at schema version 4. Regenerate and commit the diff.
Outbound, so what a change here can break:

- **dfe-loader -> dfe-infra** (`image-pin`, lockstep) -- `dfe-infra/versions.yaml` pins this image and the thin chart released here, and Argo deploys that published chart.
- **dfe-loader -> dfe-engine** (`mirrored-logic`) -- dfe-engine hand-reimplements this repo's config validation in `plugins_builtin/loader.py` to catch errors when it authors a config. Nothing is copied, so no script compares them.

Runtime, not build edges: dfe-engine's `dfe-schema` entry point creates every destination table before the loader can insert, and producers reach the loader over Kafka (`*_land` / `*_load` topics) or direct gRPC (default topic `main_land`) -- in the suite that producer is dfe-receiver.
