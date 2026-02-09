## [1.5.1](https://github.com/hypersec-io/dfe-loader/compare/v1.5.0...v1.5.1) (2026-02-09)


### Bug Fixes

* **ci:** update ci submodule with cargo registry name fix ([b8f7eec](https://github.com/hypersec-io/dfe-loader/commit/b8f7eecc9263c76c3c25c94db1c1a0df93d39212))

# [1.5.0](https://github.com/hypersec-io/dfe-loader/compare/v1.4.0...v1.5.0) (2026-02-09)


### Features

* **bench:** add fair E2E bakeoff benchmark (Mison vs Decoder vs Current) ([db6d44f](https://github.com/hypersec-io/dfe-loader/commit/db6d44f1a4c186dbd1bc8b6091cbcfafb7379cbd))
* zero-copy _json sidecar + configurable _raw/_json field control ([6e0d037](https://github.com/hypersec-io/dfe-loader/commit/6e0d0371b062c56ff29cb114a082f180663b2ac3))

# [1.4.0](https://github.com/hypersec-io/dfe-loader/compare/v1.3.1...v1.4.0) (2026-02-05)


### Bug Fixes

* add Rust CI workflows via attach.sh ([340aeaa](https://github.com/hypersec-io/dfe-loader/commit/340aeaaeb15a28a84a47774bf1f3f3c3c295723e))
* apply cargo fmt and update ci submodule to v1.48.2 ([a951d49](https://github.com/hypersec-io/dfe-loader/commit/a951d49352a2f70a88ac2efc1528857393c34b9e))
* CI configuration and clippy lint fixes ([12d9e7e](https://github.com/hypersec-io/dfe-loader/commit/12d9e7eb256d7afd905e362bc0f967a39e437e70))
* hardcode ubuntu-latest runner to bypass queued state ([c69b42f](https://github.com/hypersec-io/dfe-loader/commit/c69b42fff1d393c46ecc93523fb1ce2fa1162158))
* limit parallel jobs to prevent CPU starvation ([dc6f4de](https://github.com/hypersec-io/dfe-loader/commit/dc6f4de7b7d33c9448858cd103c167f27afd3999))
* update ci submodule to v1.48.0 ([d3d3a9c](https://github.com/hypersec-io/dfe-loader/commit/d3d3a9c2e84ae9527c85ca19ceacdcd3e45831ec))
* update test files to use clickhouse_arrow module path ([b593600](https://github.com/hypersec-io/dfe-loader/commit/b593600f1fcb169c978b98533138750435d4d12d))
* use ubuntu-latest runners (BuildJet unavailable) ([529578a](https://github.com/hypersec-io/dfe-loader/commit/529578a33c6f6077a5b85895f0d21faa3fcda065))


### Features

* add auto-initialization for Kafka topics and ClickHouse schema ([d52ce40](https://github.com/hypersec-io/dfe-loader/commit/d52ce40725a34e7fc651601f56221cc7980694a2))
* add table-level tags with [@tag](https://github.com/tag): key=value syntax ([e9c890d](https://github.com/hypersec-io/dfe-loader/commit/e9c890d78b099ebe1d6f1bd60ecef54a645df04f))

## [1.3.1](https://github.com/hypersec-io/dfe-loader-clickhouse/compare/v1.3.0...v1.3.1) (2026-01-13)


### Bug Fixes

* update remaining old project name references ([b4be1a0](https://github.com/hypersec-io/dfe-loader-clickhouse/commit/b4be1a0cc145b7aab7383fb5f5dd1a188069c6f5))

# [1.3.0](https://github.com/hypersec-io/dfe-loader/compare/v1.2.1...v1.3.0) (2026-01-13)


### Features

* switch hs-rustlib to Artifactory registry dependency ([d6bf74d](https://github.com/hypersec-io/dfe-loader/commit/d6bf74d78bd3d806ea3edf694d9221a232540fa4))

## [1.2.1](https://github.com/hypersec-io/dfe-loader/compare/v1.2.0...v1.2.1) (2026-01-12)


### Bug Fixes

* preserve _org_id in sanitizer and improve RLS tests ([73b2788](https://github.com/hypersec-io/dfe-loader/commit/73b2788f7b15177a3c3941a4561a7009088c283a))

# [1.2.0](https://github.com/hypersec-io/dfe-loader/compare/v1.1.0...v1.2.0) (2026-01-12)


### Features

* add _org_id field and shared schema routing for RLS ([4c232c0](https://github.com/hypersec-io/dfe-loader/commit/4c232c0ccc94ef6fbfbf9b684065b18bd1fd6e5b))

# [1.1.0](https://github.com/hypersec-io/dfe-loader/compare/v1.0.0...v1.1.0) (2026-01-07)


### Features

* implement schema projection for field filtering ([e2ecb00](https://github.com/hypersec-io/dfe-loader/commit/e2ecb0005e793655cddce7d51a521817f5a0b567))

# 1.0.0 (2026-01-07)


### Bug Fixes

* Arrow-only ClickHouse inserts, remove JSON fallback ([889cf81](https://github.com/hypersec-io/dfe-loader/commit/889cf81fa265fae72aaa3be8db95138c1cd91268))
* DLQ routing and Kafka offset commit on successful insert ([bbb95f7](https://github.com/hypersec-io/dfe-loader/commit/bbb95f737118306c73249cadc0e3f13ec3796bd1))
* hot path optimisations for transform pipeline ([430bb78](https://github.com/hypersec-io/dfe-loader/commit/430bb782b3ca74282c6a45f5e5b65908462c981a))
* rewrite integration tests for Arrow-native inserts ([df3b0aa](https://github.com/hypersec-io/dfe-loader/commit/df3b0aa09fb438b15a500297fa838c3d2c2a9e59))
* routing and orchestrator improvements ([8b6d890](https://github.com/hypersec-io/dfe-loader/commit/8b6d890deebc5f3badd620e1d74e6448ad33219f))


### Features

* add BFloat16, Time, Time64, AggregateFunction types to clickhouse-arrow ([aa49b03](https://github.com/hypersec-io/dfe-loader/commit/aa49b0303e0d4c84c68820770de8eac522fa1781))
* add clickhouse-arrow fork with Variant, Dynamic, Nested types ([0d255f7](https://github.com/hypersec-io/dfe-loader/commit/0d255f74651d00f82433606831d8165a150e5880))
* add klickhouse fork with Variant, Dynamic, JSON, Nested types ([06816f0](https://github.com/hypersec-io/dfe-loader/commit/06816f0139155fe72addd46ee10cd4261579b864))
* add Phase 8 metrics, health endpoints, and integration tests ([5842258](https://github.com/hypersec-io/dfe-loader/commit/5842258357b92516744f2f525f7383befd4a82fb))
* Arrow-based chunked buffer architecture ([3b0717d](https://github.com/hypersec-io/dfe-loader/commit/3b0717dc1af30d0c68a1624c7520991cfbf50994))
* enhance schema cache with periodic refresh and error-based invalidation ([8e1518d](https://github.com/hypersec-io/dfe-loader/commit/8e1518d26ce5765f249de342eec6d38032cb3ce2))
* implement batch salvage with binary-split retry ([7088bde](https://github.com/hypersec-io/dfe-loader/commit/7088bdefbe83eb1f8f258c164a229d5fbd12dfb3))
* implement circuit breaker for per-table failure detection ([d4cb204](https://github.com/hypersec-io/dfe-loader/commit/d4cb204a7f5073e2f12d743b7a0db9b291e2e5b4))
* implement Variant/Dynamic/Nested serializers for clickhouse-arrow ([97089b9](https://github.com/hypersec-io/dfe-loader/commit/97089b944b576fc86679ff4156f43bafd2cf6109))
* initial MVP of dfe-loader ([6712527](https://github.com/hypersec-io/dfe-loader/commit/6712527f7262892b7cf7d5997273038a425ee5d0)), closes [Hi#performance](https://github.com/Hi/issues/performance)
* integrate transport abstraction and add MemoryTransport tests ([2e4beb1](https://github.com/hypersec-io/dfe-loader/commit/2e4beb18345610725afa9954b46b1c055540d954))
* per-table Arrow buffers with clickhouse-arrow native inserts ([4ff38f5](https://github.com/hypersec-io/dfe-loader/commit/4ff38f5f0e02746e16694d3e66c4aa1a755550bf))
