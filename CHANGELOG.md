## [1.13.3](https://github.com/hyperi-io/dfe-loader/compare/v1.13.2...v1.13.3) (2026-03-05)


### Bug Fixes

* remove ubuntu user before creating appuser in Ubuntu 24.04 image ([b475506](https://github.com/hyperi-io/dfe-loader/commit/b475506ad82e0f53d2f679a6bc71b32d70f2a029))

## [1.13.2](https://github.com/hyperi-io/dfe-loader/compare/v1.13.1...v1.13.2) (2026-03-05)


### Bug Fixes

* add geoip dir and update ci submodule for Docker staging ([8a6a2be](https://github.com/hyperi-io/dfe-loader/commit/8a6a2be01440fefe2dc6d9235da6fdf4f7558afc))

## [1.13.1](https://github.com/hyperi-io/dfe-loader/compare/v1.13.0...v1.13.1) (2026-03-05)


### Bug Fixes

* update ci/ai submodules and publish workflow for JFrog registries ([f7a5c1c](https://github.com/hyperi-io/dfe-loader/commit/f7a5c1c0b5d5d7e87c022d75d4c6c7971de88c36))

# [1.13.0](https://github.com/hyperi-io/dfe-loader/compare/v1.12.1...v1.13.0) (2026-03-04)


### Features

* add gRPC transport receiving (dfe-receiver → dfe-loader) ([52d890a](https://github.com/hyperi-io/dfe-loader/commit/52d890a67e037fb197c2d5381ec0b5cfd8300cee))

## [1.12.1](https://github.com/hyperi-io/dfe-loader/compare/v1.12.0...v1.12.1) (2026-03-04)


### Bug Fixes

* exclude .claude, ai, ci, docs dirs from cargo publish package ([c04d001](https://github.com/hyperi-io/dfe-loader/commit/c04d00119aa17e8306b9b3f677a83beb1af6b1f0))

# [1.12.0](https://github.com/hyperi-io/dfe-loader/compare/v1.11.0...v1.12.0) (2026-03-04)


### Bug Fixes

* cargo fmt and update ci submodule ([582e023](https://github.com/hyperi-io/dfe-loader/commit/582e02306edee694d419a2d18a582b44eb3f0795))
* resolve typos CI failures from ci submodule v1.14.5 upgrade ([79b8094](https://github.com/hyperi-io/dfe-loader/commit/79b8094e84480ac3916e2188106bef043644062f))
* wire enrichment pipeline into orchestrator (not yet compiled) ([18063cc](https://github.com/hyperi-io/dfe-loader/commit/18063cc856364a34de0faf4bd4b65ddb2ac59cad))


### Features

* multi-provider GeoIP auto-download with continent enrichment ([52804d7](https://github.com/hyperi-io/dfe-loader/commit/52804d7294721ae83625d97b6e2ffaf29d9728c4))
* wire enrichment pipeline into orchestrator ([69f8e6c](https://github.com/hyperi-io/dfe-loader/commit/69f8e6cc00adcecb2d7e01b6eeadacbeb1a4b57d))

# [1.11.0](https://github.com/hyperi-io/dfe-loader/compare/v1.10.1...v1.11.0) (2026-03-03)


### Features

* add CEL expressions, DfeApp CLI framework, ubuntu 24.04 base ([6d06c7b](https://github.com/hyperi-io/dfe-loader/commit/6d06c7b22896b1fca1be13b2ccf26709307ea2cc))

## [1.10.1](https://github.com/hyperi-io/dfe-loader/compare/v1.10.0...v1.10.1) (2026-03-03)


### Bug Fixes

* gate semantic-release on CI, add rustlib cli+top features ([61d0f59](https://github.com/hyperi-io/dfe-loader/commit/61d0f59b888e164e7665e5c280d4ddaef1512ba9))

# [1.10.0](https://github.com/hyperi-io/dfe-loader/compare/v1.9.7...v1.10.0) (2026-03-03)


### Bug Fixes

* gate semantic-release on CI, add rustlib cli+top features ([e72ffb4](https://github.com/hyperi-io/dfe-loader/commit/e72ffb47cb95e11e930e9f4a77f53cc618b70316))
* resolve clippy warnings and test routing assertions ([b888c57](https://github.com/hyperi-io/dfe-loader/commit/b888c57d6bb9fc06c248d4ab7417f1771cb89b79))


### Features

* replace hand-written Dockerfile and chart with generated artifacts ([10e1005](https://github.com/hyperi-io/dfe-loader/commit/10e1005ff3effe12ac812259f30fb6c415cd73ce))

## [1.9.7](https://github.com/hyperi-io/dfe-loader/compare/v1.9.6...v1.9.7) (2026-03-02)


### Bug Fixes

* replace bespoke DLQ with unified rustlib dlq module ([42a4dbc](https://github.com/hyperi-io/dfe-loader/commit/42a4dbc92b43ab33ebdc490459412f0d21d5e8c2))

## [1.9.6](https://github.com/hyperi-io/dfe-loader/compare/v1.9.5...v1.9.6) (2026-03-02)


### Bug Fixes

* remove schema/profile/DDL system — table creation moves to dfe-engine ([9cd56b5](https://github.com/hyperi-io/dfe-loader/commit/9cd56b57de67b532647ecef27ca89c1750aa17b3))

## [1.9.5](https://github.com/hyperi-io/dfe-loader/compare/v1.9.4...v1.9.5) (2026-03-02)


### Bug Fixes

* add scaling pressure integration for KEDA autoscaling ([f668864](https://github.com/hyperi-io/dfe-loader/commit/f6688648050ba5e61a2f661cd05a5eaa9f6992b2))

## [1.9.4](https://github.com/hyperi-io/dfe-loader/compare/v1.9.3...v1.9.4) (2026-03-02)


### Bug Fixes

* update rustlib to v1.8.1, remove zenoh transport, update ci/ai submodules ([acdacc2](https://github.com/hyperi-io/dfe-loader/commit/acdacc2d39d6b839623454f06d8a9c6db5b2e105))
* use jfrog registry for container and helm publishing ([62b3268](https://github.com/hyperi-io/dfe-loader/commit/62b3268068dd4fe7d057f72ea41ff715e2e99394))

## [1.9.3](https://github.com/hyperi-io/dfe-loader/compare/v1.9.2...v1.9.3) (2026-02-28)


### Bug Fixes

* prevent auto-commit race with semantic-release ([95645b0](https://github.com/hyperi-io/dfe-loader/commit/95645b007dd7dc63fd1c2c231bd88461d09f6d74))

## [1.9.2](https://github.com/hyperi-io/dfe-loader/compare/v1.9.1...v1.9.2) (2026-02-28)


### Bug Fixes

* cargo fmt and rustfmt formatting ([f1a4b74](https://github.com/hyperi-io/dfe-loader/commit/f1a4b749fe8d0cf97cca8c85c28ac487edb62a19))

## [1.9.1](https://github.com/hyperi-io/dfe-loader/compare/v1.9.0...v1.9.1) (2026-02-28)


### Bug Fixes

* switch hyperi-rustlib back to artifactory registry ([8929ce6](https://github.com/hyperi-io/dfe-loader/commit/8929ce6bc5e503c27232971df34b4f667cee0d92))
* use GH_RUNNER_DEFAULT variable for runner selection ([829306d](https://github.com/hyperi-io/dfe-loader/commit/829306dddedd873c6c370825f69920cddd383c24))

# [1.9.0](https://github.com/hyperi-io/dfe-loader/compare/v1.8.3...v1.9.0) (2026-02-28)


### Features

* helm chart, dockerfile, version check, config cascade improvements ([c757338](https://github.com/hyperi-io/dfe-loader/commit/c7573384379e0a69569ef2439e0ffd48fd6fea1e))

## [1.8.3](https://github.com/hyperi-io/dfe-loader/compare/v1.8.2...v1.8.3) (2026-02-26)


### Bug Fixes

* Add comments as COMMENT to columns and allow formating of timestamps with tz ([5ca6bad](https://github.com/hyperi-io/dfe-loader/commit/5ca6badf43c5beccb80773b2aafc5af39cde0bae))

## [1.8.2](https://github.com/hyperi-io/dfe-loader/compare/v1.8.1...v1.8.2) (2026-02-25)


### Bug Fixes

* Point hyperi-rustlib back to artifactory ([9e895d8](https://github.com/hyperi-io/dfe-loader/commit/9e895d8032b3a29a1ac7c784e0c3b0f0495cfaf3))

## [1.8.1](https://github.com/hyperi-io/dfe-loader/compare/v1.8.0...v1.8.1) (2026-02-25)


### Bug Fixes

* Use of replicating merge tree when unavailable ([c2535bd](https://github.com/hyperi-io/dfe-loader/commit/c2535bdecc60878f3746c458ba8c374c718e8b3e))

# [1.8.0](https://github.com/hyperi-io/dfe-loader/compare/v1.7.1...v1.8.0) (2026-02-25)


### Features

* source field, common header, figment config cascade, config-reload abstractions ([638f181](https://github.com/hyperi-io/dfe-loader/commit/638f18199e26ea65fbc3f60094ebe564cadf6541))

## [1.7.1](https://github.com/hyperi-io/dfe-loader/compare/v1.7.0...v1.7.1) (2026-02-24)


### Bug Fixes

* Engine identification always returning 1 row (0 or 1) causing always true statements ([e2e359b](https://github.com/hyperi-io/dfe-loader/commit/e2e359bb2213990a9ec8002749b174a041effd40))

# [1.7.0](https://github.com/hyperi-io/dfe-loader/compare/v1.6.13...v1.7.0) (2026-02-19)


### Features

* wire zenoh transport backend behind feature gate ([3a59fec](https://github.com/hyperi-io/dfe-loader/commit/3a59fec2a2f00f54432e0556cb4b553b2b3f4172))

## [1.6.13](https://github.com/hyperi-io/dfe-loader/compare/v1.6.12...v1.6.13) (2026-02-19)


### Bug Fixes

* update ci with binary publish permission fix ([8397acc](https://github.com/hyperi-io/dfe-loader/commit/8397accf9b0b79b6180f83690df43b20559b6fb1))

## [1.6.12](https://github.com/hyperi-io/dfe-loader/compare/v1.6.11...v1.6.12) (2026-02-19)


### Bug Fixes

* update ci with system libc6-dev:arm64 for cross-compilation ([027f396](https://github.com/hyperi-io/dfe-loader/commit/027f39616c1338524f83c78c30f5458563933417))

## [1.6.11](https://github.com/hyperi-io/dfe-loader/compare/v1.6.10...v1.6.11) (2026-02-19)


### Bug Fixes

* update ci with two-level cross sysroot dependency resolution ([511a793](https://github.com/hyperi-io/dfe-loader/commit/511a793c11598bb33cd4cae242337dbbabd08d1f))

## [1.6.10](https://github.com/hyperi-io/dfe-loader/compare/v1.6.9...v1.6.10) (2026-02-19)


### Bug Fixes

* update ci with usrmerge sysroot handling ([cbf66e4](https://github.com/hyperi-io/dfe-loader/commit/cbf66e40bd5b77f2d7124b300664faf5f98fdf76))

## [1.6.9](https://github.com/hyperi-io/dfe-loader/compare/v1.6.8...v1.6.9) (2026-02-19)


### Bug Fixes

* update ci with cross-compilation ld script patching ([c8e4a14](https://github.com/hyperi-io/dfe-loader/commit/c8e4a1474a49105bc185b0b2bc39b32a73093d16))

## [1.6.8](https://github.com/hyperi-io/dfe-loader/compare/v1.6.7...v1.6.8) (2026-02-19)


### Bug Fixes

* update ci with cross-compilation sysroot include fix ([47132a3](https://github.com/hyperi-io/dfe-loader/commit/47132a369d8dbec46be04927595facabcdcade9f))

## [1.6.7](https://github.com/hyperi-io/dfe-loader/compare/v1.6.6...v1.6.7) (2026-02-19)


### Bug Fixes

* update ci submodule with feature set parsing fix ([3600ddd](https://github.com/hyperi-io/dfe-loader/commit/3600ddde8234b83730479553fe60299bf5b2e459))

## [1.6.6](https://github.com/hyperi-io/dfe-loader/compare/v1.6.5...v1.6.6) (2026-02-19)


### Bug Fixes

* update ci submodule and add field mapping integration tests ([5cd5c67](https://github.com/hyperi-io/dfe-loader/commit/5cd5c6720a0e7dbfe12bee14d7aecb995d9dcdd3))

## [1.6.5](https://github.com/hyperi-io/dfe-loader/compare/v1.6.4...v1.6.5) (2026-02-18)


### Bug Fixes

* add cross-compilation binary builds for amd64 and arm64 ([07d1754](https://github.com/hyperi-io/dfe-loader/commit/07d1754a4db973ddacf1f0ff125235528ac5d5d2))

## [1.6.4](https://github.com/hyperi-io/dfe-loader/compare/v1.6.3...v1.6.4) (2026-02-17)


### Bug Fixes

* resolve CI publish failures ([63bcbe2](https://github.com/hyperi-io/dfe-loader/commit/63bcbe237b682bc7b19b29a9863fb3af4eff65dd))

## [1.6.3](https://github.com/hyperi-io/dfe-loader/compare/v1.6.2...v1.6.3) (2026-02-17)


### Bug Fixes

* correct doctest assertion order for BTreeMap output ([d633a56](https://github.com/hyperi-io/dfe-loader/commit/d633a56a3d9e9c931f4d4e6aef5cb05810beb418))

## [1.6.2](https://github.com/hyperi-io/dfe-loader/compare/v1.6.1...v1.6.2) (2026-02-17)


### Bug Fixes

* remove env dependency from kafka config unit tests ([c360970](https://github.com/hyperi-io/dfe-loader/commit/c360970b9fee2d2115f92ae190ef71055d71b407))

## [1.6.1](https://github.com/hyperi-io/dfe-loader/compare/v1.6.0...v1.6.1) (2026-02-17)


### Bug Fixes

* resolve clippy warnings and allocator feature conflict ([251b72a](https://github.com/hyperi-io/dfe-loader/commit/251b72a8addfdbf7129bfab68ed70c86db04f64a))

# [1.6.0](https://github.com/hyperi-io/dfe-loader/compare/v1.5.1...v1.6.0) (2026-02-17)


### Bug Fixes

* rebrand hypersec to hyperi across project ([df02a5b](https://github.com/hyperi-io/dfe-loader/commit/df02a5b041fd35ee6abdf83ea659c885849c9a3b))


### Features

* add field mapping with ECS/CIM/Beats presets ([c2873e5](https://github.com/hyperi-io/dfe-loader/commit/c2873e5fd936602d2f20610d5ef68b6127aa1bd2))

## [1.5.1](https://github.com/hyperi-io/dfe-loader/compare/v1.5.0...v1.5.1) (2026-02-09)


### Bug Fixes

* **ci:** update ci submodule with cargo registry name fix ([b8f7eec](https://github.com/hyperi-io/dfe-loader/commit/b8f7eecc9263c76c3c25c94db1c1a0df93d39212))

# [1.5.0](https://github.com/hyperi-io/dfe-loader/compare/v1.4.0...v1.5.0) (2026-02-09)


### Features

* **bench:** add fair E2E bakeoff benchmark (Mison vs Decoder vs Current) ([db6d44f](https://github.com/hyperi-io/dfe-loader/commit/db6d44f1a4c186dbd1bc8b6091cbcfafb7379cbd))
* zero-copy _json sidecar + configurable _raw/_json field control ([6e0d037](https://github.com/hyperi-io/dfe-loader/commit/6e0d0371b062c56ff29cb114a082f180663b2ac3))

# [1.4.0](https://github.com/hyperi-io/dfe-loader/compare/v1.3.1...v1.4.0) (2026-02-05)


### Bug Fixes

* add Rust CI workflows via attach.sh ([340aeaa](https://github.com/hyperi-io/dfe-loader/commit/340aeaaeb15a28a84a47774bf1f3f3c3c295723e))
* apply cargo fmt and update ci submodule to v1.48.2 ([a951d49](https://github.com/hyperi-io/dfe-loader/commit/a951d49352a2f70a88ac2efc1528857393c34b9e))
* CI configuration and clippy lint fixes ([12d9e7e](https://github.com/hyperi-io/dfe-loader/commit/12d9e7eb256d7afd905e362bc0f967a39e437e70))
* hardcode ubuntu-latest runner to bypass queued state ([c69b42f](https://github.com/hyperi-io/dfe-loader/commit/c69b42fff1d393c46ecc93523fb1ce2fa1162158))
* limit parallel jobs to prevent CPU starvation ([dc6f4de](https://github.com/hyperi-io/dfe-loader/commit/dc6f4de7b7d33c9448858cd103c167f27afd3999))
* update ci submodule to v1.48.0 ([d3d3a9c](https://github.com/hyperi-io/dfe-loader/commit/d3d3a9c2e84ae9527c85ca19ceacdcd3e45831ec))
* update test files to use clickhouse_arrow module path ([b593600](https://github.com/hyperi-io/dfe-loader/commit/b593600f1fcb169c978b98533138750435d4d12d))
* use ubuntu-latest runners (BuildJet unavailable) ([529578a](https://github.com/hyperi-io/dfe-loader/commit/529578a33c6f6077a5b85895f0d21faa3fcda065))


### Features

* add auto-initialization for Kafka topics and ClickHouse schema ([d52ce40](https://github.com/hyperi-io/dfe-loader/commit/d52ce40725a34e7fc651601f56221cc7980694a2))
* add table-level tags with [@tag](https://github.com/tag): key=value syntax ([e9c890d](https://github.com/hyperi-io/dfe-loader/commit/e9c890d78b099ebe1d6f1bd60ecef54a645df04f))

## [1.3.1](https://github.com/hyperi-io/dfe-loader-clickhouse/compare/v1.3.0...v1.3.1) (2026-01-13)


### Bug Fixes

* update remaining old project name references ([b4be1a0](https://github.com/hyperi-io/dfe-loader-clickhouse/commit/b4be1a0cc145b7aab7383fb5f5dd1a188069c6f5))

# [1.3.0](https://github.com/hyperi-io/dfe-loader/compare/v1.2.1...v1.3.0) (2026-01-13)


### Features

* switch hs-rustlib to Artifactory registry dependency ([d6bf74d](https://github.com/hyperi-io/dfe-loader/commit/d6bf74d78bd3d806ea3edf694d9221a232540fa4))

## [1.2.1](https://github.com/hyperi-io/dfe-loader/compare/v1.2.0...v1.2.1) (2026-01-12)


### Bug Fixes

* preserve _org_id in sanitizer and improve RLS tests ([73b2788](https://github.com/hyperi-io/dfe-loader/commit/73b2788f7b15177a3c3941a4561a7009088c283a))

# [1.2.0](https://github.com/hyperi-io/dfe-loader/compare/v1.1.0...v1.2.0) (2026-01-12)


### Features

* add _org_id field and shared schema routing for RLS ([4c232c0](https://github.com/hyperi-io/dfe-loader/commit/4c232c0ccc94ef6fbfbf9b684065b18bd1fd6e5b))

# [1.1.0](https://github.com/hyperi-io/dfe-loader/compare/v1.0.0...v1.1.0) (2026-01-07)


### Features

* implement schema projection for field filtering ([e2ecb00](https://github.com/hyperi-io/dfe-loader/commit/e2ecb0005e793655cddce7d51a521817f5a0b567))

# 1.0.0 (2026-01-07)


### Bug Fixes

* Arrow-only ClickHouse inserts, remove JSON fallback ([889cf81](https://github.com/hyperi-io/dfe-loader/commit/889cf81fa265fae72aaa3be8db95138c1cd91268))
* DLQ routing and Kafka offset commit on successful insert ([bbb95f7](https://github.com/hyperi-io/dfe-loader/commit/bbb95f737118306c73249cadc0e3f13ec3796bd1))
* hot path optimisations for transform pipeline ([430bb78](https://github.com/hyperi-io/dfe-loader/commit/430bb782b3ca74282c6a45f5e5b65908462c981a))
* rewrite integration tests for Arrow-native inserts ([df3b0aa](https://github.com/hyperi-io/dfe-loader/commit/df3b0aa09fb438b15a500297fa838c3d2c2a9e59))
* routing and orchestrator improvements ([8b6d890](https://github.com/hyperi-io/dfe-loader/commit/8b6d890deebc5f3badd620e1d74e6448ad33219f))


### Features

* add BFloat16, Time, Time64, AggregateFunction types to clickhouse-arrow ([aa49b03](https://github.com/hyperi-io/dfe-loader/commit/aa49b0303e0d4c84c68820770de8eac522fa1781))
* add clickhouse-arrow fork with Variant, Dynamic, Nested types ([0d255f7](https://github.com/hyperi-io/dfe-loader/commit/0d255f74651d00f82433606831d8165a150e5880))
* add klickhouse fork with Variant, Dynamic, JSON, Nested types ([06816f0](https://github.com/hyperi-io/dfe-loader/commit/06816f0139155fe72addd46ee10cd4261579b864))
* add Phase 8 metrics, health endpoints, and integration tests ([5842258](https://github.com/hyperi-io/dfe-loader/commit/5842258357b92516744f2f525f7383befd4a82fb))
* Arrow-based chunked buffer architecture ([3b0717d](https://github.com/hyperi-io/dfe-loader/commit/3b0717dc1af30d0c68a1624c7520991cfbf50994))
* enhance schema cache with periodic refresh and error-based invalidation ([8e1518d](https://github.com/hyperi-io/dfe-loader/commit/8e1518d26ce5765f249de342eec6d38032cb3ce2))
* implement batch salvage with binary-split retry ([7088bde](https://github.com/hyperi-io/dfe-loader/commit/7088bdefbe83eb1f8f258c164a229d5fbd12dfb3))
* implement circuit breaker for per-table failure detection ([d4cb204](https://github.com/hyperi-io/dfe-loader/commit/d4cb204a7f5073e2f12d743b7a0db9b291e2e5b4))
* implement Variant/Dynamic/Nested serializers for clickhouse-arrow ([97089b9](https://github.com/hyperi-io/dfe-loader/commit/97089b944b576fc86679ff4156f43bafd2cf6109))
* initial MVP of dfe-loader ([6712527](https://github.com/hyperi-io/dfe-loader/commit/6712527f7262892b7cf7d5997273038a425ee5d0)), closes [Hi#performance](https://github.com/Hi/issues/performance)
* integrate transport abstraction and add MemoryTransport tests ([2e4beb1](https://github.com/hyperi-io/dfe-loader/commit/2e4beb18345610725afa9954b46b1c055540d954))
* per-table Arrow buffers with clickhouse-arrow native inserts ([4ff38f5](https://github.com/hyperi-io/dfe-loader/commit/4ff38f5f0e02746e16694d3e66c4aa1a755550bf))
