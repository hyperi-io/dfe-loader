# Testing

The general rules are hyperi-ai's testing standard (`standards/universal/testing.md` in hyperi-ai). This page covers what dfe-loader does on top of them.

## Levels

| Level | Runs | Command |
|---|---|---|
| core | locally, every push and PR | `cargo nextest run --all-features` |
| full | nightly (`schedule` in `.github/workflows/ci.yml`) and before every release (`test.full.required_for_release: true`) | `cargo nextest run --all-features --run-ignored all --ignore-default-filter` |
| stack | dfe-infra's post-deploy acceptance, not this repo | - |

Core is nextest's default selection. The `default-filter` in `.config/nextest.toml` holds every test that needs a real ClickHouse or Kafka out of it, bar the two in `tests/integration/shared_services.rs`. The `#[ignore]` tests in `tests/e2e/` run only under full, against the shared ClickHouse and Kafka (`require_e2e_services`). A missing service fails them, it never skips them. hyperi-ci runs core on push and PR, and full on the schedule, on a release, or on a dispatch with `test-tier: full`.

Core holds:

- the unit tests in `src/`, the smoke test and the integration tests that need no server
- the captured-fixture tests: the error classifier table in `src/clickhouse/error.rs` reads the server bodies in `tests/fixtures/clickhouse/errors/`
- `integration::shared_services::clickhouse_main_cases`: schema fetch, a RowBinary insert, the count back and a missing table, over HTTP, native, HTTPS and native TLS
- `integration::shared_services::kafka_main_cases`: records produced before the consumer starts arrive in order, and a commit holds for the group

Full adds every other real-server test: the inserter, readiness, data type, DLQ, offset, rebalance, outage, gRPC and hot-reload suites, the TLS trust-store matrix, the container hygiene checks and the live check of the error fixtures. The decision rule for a new test: if the server changed how it behaves, is this the test that should catch it? Yes means a real server, in full unless it is one of the main cases. No means a captured fixture, in core.

## Services, one instance each per run

| Service | Image | Container ports |
|---|---|---|
| ClickHouse | `clickhouse/clickhouse-server:26.3.42.3@sha256:21d572843e59539c7d100286b6f5a6053c341fe4b33c2d7b74b1ff7cb24c5399` | 8123 HTTP, 9000 native, 8443 HTTPS, 9440 native TLS |
| Kafka | `apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837` | 9092 PLAINTEXT |

The ClickHouse pin lives in `tests/common/containers.rs` and follows dfe-infra `versions.yaml`. The Kafka pin is scalo's `transport::kafka::test_support::KAFKA_IMAGE`, the line the DFE stack deploys.

Each ClickHouse container gets a random password for `default`, never logged, which tests read with `clickhouse_password()`. `default` may create users and grant to them. An embedded single-node Keeper backs the `dfe_replicated` database (`Replicated` engine) that the `ReplicatedMergeTree()` tests in `inserter.rs` and `rls.rs` write to. Those tests fail, not skip, when it is missing (`require_replicated!`). Its certificate names `localhost` and the Docker host, signed by a CA made for the run, which tests read back with `shared_clickhouse_ca()`. Kafka is one KRaft broker advertising the host port Docker published.

`tests/common/shared_service.rs` gives every test process in a run the same container per service:

- The first process to ask creates it, named `<prefix>-integration-<service>-r<run>`, where `<run>` is the start of `NEXTEST_RUN_ID` and `<prefix>` is `HYPERI_TEST_CONTAINER_PREFIX`, else `dfe-loader-test`. Every other process finds it by that name and waits for it to be ready.
- The creator starts a watcher in its own process group that waits on the nextest process and removes the container once it exits. That covers a pass, a failure, Ctrl-C and a killed run. nextest's setup scripts are experimental and have no teardown, so the watcher is the teardown.
- Each creation is logged once, to stderr and to `shared-services-<run>.log` under the target's `tmp/`, with the container count for its service. The watcher appends each removal. `shared_services` prints the log at the end of the run.
- Containers carry `io.hyperi.test.suite=dfe-loader-integration`, `io.hyperi.test.run`, and the owner: `owner-host` (hostname and machine id), `owner-pidns`, `owner-pid`, `owner-start` (the process start time) and `lease-until`.
- Each create sweeps this suite's containers. One owned on this host and PID namespace goes once its owner process is gone or its start time differs. One owned anywhere else goes only once its 4-hour lease has lapsed, so two machines on one daemon never take each other's live services.

Docker is whatever the environment names: `DOCKER_HOST`, else the current docker context. Ports bind to 127.0.0.1 on the daemon host, never a wildcard:

- A local daemon, or `tcp://` on loopback, is reached directly.
- An `ssh://` daemon gets one `ssh -L` tunnel per container, on the same port number at both ends so Kafka's advertised address resolves here. The watcher closes it with the container.
- A remote `tcp://` daemon is refused unless `HYPERI_TEST_DOCKER_BIND` names a private address on that host.

In CI the runner's daemon is used and a missing daemon fails the run.

Tests on the shared instances keep to their own names: unique tables (`unique_table_name`), a probe user per process, and topics and consumer groups named for the test. A test that needs a whole server or broker to itself uses `TestInfrastructure::exclusive`, which starts a container only it holds, removed when the test drops it or its process exits: topic auto-discovery, the hygiene check of per-test names, and the broker started after the loader (`start_kafka_on`).

## Pets for local core runs

A local core run can use always-present pet servers instead of the containers. The rung order is the pet named by `HYPERI_TEST_*`, then the shared container. A pet that is unset or fails a quick probe falls back to the container, and the test says which it used. `TEST_MODE=docker` points the ClickHouse tests and `tests/e2e/` at a dfe-docker stack on localhost instead. Pets are never used in CI (`CI` set) or under full (`HYPERCI_TEST_TIER=full`, which `hyperi-ci --tier full` sets). Run full by hand with that variable set.

Each `HYPERI_TEST_*` setting is read from the process environment, then the project `.env`, then the file named by `HYPERI_TEST_ENV_FILE`:

- ClickHouse: `HYPERI_TEST_CLICKHOUSE_HOST`, `_HTTP_PORT`, `_HTTPS_PORT`, `_NATIVE_PORT`, `_NATIVE_TLS_PORT`, `_USER`, `_PASSWORD`, `_CA_FILE`, `_CLUSTER`, `_DB_PREFIX`
- Kafka: `HYPERI_TEST_KAFKA_BOOTSTRAP`, `_SECURITY_PROTOCOL`, `_SASL_MECHANISM`, `_USER`, `_PASSWORD`, `_CA_FILE`, `_TOPIC_PREFIX`

On HyperI dev machines `hyperi-ai secrets devex-env` (hyperi-ai 4.1.30 or later) writes them to `~/.config/hyperi-ai/devex-test.env`, mode 0600, and prints only the path and the key count. Point `HYPERI_TEST_ENV_FILE` at that file. No host, port or credential belongs in this repo.

A run on a pet creates what it needs and drops it:

- ClickHouse: a run database `<DB_PREFIX>t<epoch>_<run>`. With `_CLUSTER` set it is created `ON CLUSTER` with the `Replicated` engine and `ReplicatedMergeTree` tables, so every replica behind the pet's hostname sees them, and dropped `ON CLUSTER`.
- Kafka: a run topic `<TOPIC_PREFIX>t<epoch>-<run>` and its group, created and deleted by the test, since the pet does not auto-create topics.
- Run databases and topics older than 6 hours are swept at the start of a run.
- The TLS cases need the service's `_CA_FILE`. Without it the ClickHouse TLS cases are left out on the pet, and a Kafka pet that needs TLS falls back to the container.

## Re-capturing the fixtures

```bash
scripts/capture-clickhouse-fixtures.py          # write the live bodies over the fixtures
scripts/capture-clickhouse-fixtures.py --check  # compare codes and names only
```

It runs `integration::clickhouse_error_bodies` with `DFE_LOADER_CAPTURE_FIXTURES=1`: the shared ClickHouse raises each error, and the body lands in `tests/fixtures/clickhouse/errors/`. Without the variable the same test, in full, fails when a live code or error name no longer matches its fixture. `unknown_status_of_insert_319.txt` is written by hand: it needs a lost Keeper session. Run the script where the fixtures should land, since cargo-remote writes on the build host. Review the diff and run the classifier table (`cargo nextest run --lib error`) before committing.
