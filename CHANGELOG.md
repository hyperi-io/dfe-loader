# 1.0.0 (2026-01-07)


### Bug Fixes

* Arrow-only ClickHouse inserts, remove JSON fallback ([889cf81](https://github.com/hypersec-io/dfe-loader-clickhouse/commit/889cf81fa265fae72aaa3be8db95138c1cd91268))
* DLQ routing and Kafka offset commit on successful insert ([bbb95f7](https://github.com/hypersec-io/dfe-loader-clickhouse/commit/bbb95f737118306c73249cadc0e3f13ec3796bd1))
* hot path optimisations for transform pipeline ([430bb78](https://github.com/hypersec-io/dfe-loader-clickhouse/commit/430bb782b3ca74282c6a45f5e5b65908462c981a))
* rewrite integration tests for Arrow-native inserts ([df3b0aa](https://github.com/hypersec-io/dfe-loader-clickhouse/commit/df3b0aa09fb438b15a500297fa838c3d2c2a9e59))
* routing and orchestrator improvements ([8b6d890](https://github.com/hypersec-io/dfe-loader-clickhouse/commit/8b6d890deebc5f3badd620e1d74e6448ad33219f))


### Features

* add BFloat16, Time, Time64, AggregateFunction types to clickhouse-arrow ([aa49b03](https://github.com/hypersec-io/dfe-loader-clickhouse/commit/aa49b0303e0d4c84c68820770de8eac522fa1781))
* add clickhouse-arrow fork with Variant, Dynamic, Nested types ([0d255f7](https://github.com/hypersec-io/dfe-loader-clickhouse/commit/0d255f74651d00f82433606831d8165a150e5880))
* add klickhouse fork with Variant, Dynamic, JSON, Nested types ([06816f0](https://github.com/hypersec-io/dfe-loader-clickhouse/commit/06816f0139155fe72addd46ee10cd4261579b864))
* add Phase 8 metrics, health endpoints, and integration tests ([5842258](https://github.com/hypersec-io/dfe-loader-clickhouse/commit/5842258357b92516744f2f525f7383befd4a82fb))
* Arrow-based chunked buffer architecture ([3b0717d](https://github.com/hypersec-io/dfe-loader-clickhouse/commit/3b0717dc1af30d0c68a1624c7520991cfbf50994))
* enhance schema cache with periodic refresh and error-based invalidation ([8e1518d](https://github.com/hypersec-io/dfe-loader-clickhouse/commit/8e1518d26ce5765f249de342eec6d38032cb3ce2))
* implement batch salvage with binary-split retry ([7088bde](https://github.com/hypersec-io/dfe-loader-clickhouse/commit/7088bdefbe83eb1f8f258c164a229d5fbd12dfb3))
* implement circuit breaker for per-table failure detection ([d4cb204](https://github.com/hypersec-io/dfe-loader-clickhouse/commit/d4cb204a7f5073e2f12d743b7a0db9b291e2e5b4))
* implement Variant/Dynamic/Nested serializers for clickhouse-arrow ([97089b9](https://github.com/hypersec-io/dfe-loader-clickhouse/commit/97089b944b576fc86679ff4156f43bafd2cf6109))
* initial MVP of dfe-loader-clickhouse ([6712527](https://github.com/hypersec-io/dfe-loader-clickhouse/commit/6712527f7262892b7cf7d5997273038a425ee5d0)), closes [Hi#performance](https://github.com/Hi/issues/performance)
* integrate transport abstraction and add MemoryTransport tests ([2e4beb1](https://github.com/hypersec-io/dfe-loader-clickhouse/commit/2e4beb18345610725afa9954b46b1c055540d954))
* per-table Arrow buffers with clickhouse-arrow native inserts ([4ff38f5](https://github.com/hypersec-io/dfe-loader-clickhouse/commit/4ff38f5f0e02746e16694d3e66c4aa1a755550bf))
