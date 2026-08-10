# GTC — Go Change Data Capture for PostgreSQL

[![CI](https://github.com/emoss08/gtc/actions/workflows/ci.yml/badge.svg)](https://github.com/emoss08/gtc/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/badge/go-1.25-00ADD8?logo=go)](go.mod)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

GTC streams row-level changes straight from the PostgreSQL WAL to Redis
Streams, RedisJSON, and Meilisearch — as **a single Go binary with no Kafka,
no ZooKeeper, and no Connect cluster**.

It exists for the very common case where Debezium is the right idea but the
wrong amount of infrastructure: you want your caches and search indexes to
follow your database in near-real-time, and you don't want to operate a
streaming platform to get there.

```
PostgreSQL ──logical replication──▶ GTC ──▶ Redis Streams   (event bus)
                                       ├──▶ RedisJSON       (document mirror / cache)
                                       └──▶ Meilisearch     (search index)
```

## Features

- **Single binary, plug and play** — point it at a database with
  `wal_level=logical`, set a Redis URL, and every table is mirrored. The
  replication slot and publication are created automatically.
- **Lock-free initial backfill** — on first start, existing table data is
  synced to the sinks *concurrently with live streaming* using watermark
  chunking (the DBLog algorithm): no table locks, no paused replication, and
  progress survives restarts. New tables (or re-syncs) can be backfilled at
  any time via the HTTP API.
- **At-least-once delivery** — the WAL position is confirmed to PostgreSQL
  only at transaction commit boundaries, after every sink has processed every
  event. A failed sink means teardown and replay, never silent loss.
- **Sinks built in** — Redis Streams (fan-out event bus with `XADD`),
  RedisJSON (one JSON document per row, partial merges on update), and
  Meilisearch (partial document updates, waits for task completion so
  indexing failures surface as errors).
- **Correct TOAST handling** — unchanged TOAST columns are omitted and
  reported, not replaced with placeholder garbage; sinks merge instead of
  clobbering.
- **Per-table routing** — an optional YAML file selects which tables go to
  which sink and how keys are built, using Go templates
  (`{{.Prefix}}:orders:{{.Field "id"}}`).
- **Resilience** — per-sink retries with exponential backoff and a circuit
  breaker that fails fast while a sink is down (the WAL position simply
  doesn't advance until it recovers).
- **Observability** — Prometheus metrics (events, per-sink latency, errors,
  retries, breaker state, WAL lag in bytes), health and readiness endpoints,
  structured JSON logs.

## Quick start

### Docker Compose

```bash
docker compose -f docker-compose-local.yml up --build
```

This starts PostgreSQL (with logical replication enabled), Redis Stack, and
GTC. Then watch a change flow through:

```bash
# Make a change
docker compose -f docker-compose-local.yml exec postgres \
  psql -U postgres -c "CREATE TABLE users (id serial PRIMARY KEY, name text);
                       INSERT INTO users (name) VALUES ('ada');"

# See it on the Redis stream
docker compose -f docker-compose-local.yml exec redis \
  redis-cli XRANGE cdc:public:users - +

# And as a JSON document
docker compose -f docker-compose-local.yml exec redis \
  redis-cli JSON.GET cdc:public:users:1
```

### Binary

```bash
go build -o gateway ./cmd/gateway

DATABASE_URL="postgres://postgres:postgres@localhost:5432/mydb?replication=database" \
REDIS_URL="redis://localhost:6379" \
./gateway
```

That's the whole setup: with no further configuration GTC mirrors every table
in the publication to Redis. Add `REDIS_JSON_URL` and/or `MEILISEARCH_URL` to
enable the other sinks.

### PostgreSQL requirements

- PostgreSQL 14 or newer (pgoutput protocol v2).
- `wal_level = logical` (and a free slot in `max_replication_slots`).
- The connection string **must** include `?replication=database`.
- The connecting role needs the `REPLICATION` attribute (and permission to
  create the publication/slot, unless you pre-create them and set
  `CDC_AUTO_CREATE_*` to `false`).

> **Decommissioning warning:** a replication slot retains WAL until it is
> dropped. If you stop running GTC permanently, drop its slot
> (`SELECT pg_drop_replication_slot('cdc_demo_slot');`) or your disk will
> slowly fill.

## Event format

Redis Stream entries carry two fields — `event_id` (dedupe key) and
`payload`:

```json
{
  "operation": "UPDATE",
  "old_data":  {"id": 7, "name": "bob"},
  "new_data":  {"id": 7, "name": "alice"},
  "unchanged_toast_columns": ["bio"],
  "metadata": {
    "LSN": "0/1A2B3C4",
    "TransactionID": 777,
    "Timestamp": "2026-08-10T12:00:00Z"
  }
}
```

Because delivery is at-least-once, consumers should treat `event_id` as an
idempotency key. Columns listed in `unchanged_toast_columns` were not
included in the WAL record (their TOASTed value didn't change) — treat them
as "unchanged", never as "deleted".

## Configuration

Everything is configured through environment variables; per-table routing
lives in an optional YAML file.

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARN`, `ERROR` |
| `DATABASE_URL` | localhost | PostgreSQL URL, must include `?replication=database` |
| `HTTP_PORT` | `8080` | Health/readiness/metrics server port |
| `CDC_SLOT_NAME` | `cdc_demo_slot` | Replication slot name |
| `CDC_PUBLICATION_NAME` | `cdc_demo_publication` | Publication name |
| `CDC_AUTO_CREATE_SLOT` | `true` | Create the slot if missing |
| `CDC_AUTO_CREATE_PUBLICATION` | `true` | Create the publication (`FOR ALL TABLES`) if missing |
| `CDC_STANDBY_TIMEOUT` | `10s` | Standby keepalive interval |
| `CDC_PARALLEL_SINKS` | `false` | Process sinks concurrently per event |
| `CDC_PROCESS_TIMEOUT` | `10s` | Per-sink, per-event timeout (keep sink-count × timeout under `wal_sender_timeout`) |
| `CDC_EXCLUDED_TABLES` | – | Comma-separated tables to skip (`public.migrations,audit_logs`) |
| `CDC_SLOT_RETRY_INTERVAL` | `5s` | Wait between retries when the slot is busy |
| `CDC_SLOT_RETRY_TIMEOUT` | `60s` | Give up starting replication after this long |
| `CDC_MAX_RETRIES` | `3` | Per-sink retries before an event fails |
| `CDC_RETRY_BACKOFF_INITIAL` | `100ms` | Initial retry backoff |
| `CDC_RETRY_BACKOFF_MAX` | `10s` | Maximum retry backoff |
| `CDC_CIRCUIT_BREAKER_THRESHOLD` | `5` | Consecutive failures before the circuit opens |
| `CDC_CIRCUIT_BREAKER_TIMEOUT` | `30s` | How long an open circuit stays open |
| `CDC_BACKFILL_MODE` | `auto` | `auto` backfills all tables when the slot is first created; `manual` via API only; `off` disables |
| `CDC_BACKFILL_CHUNK_SIZE` | `1000` | Rows per backfill chunk |
| `CDC_BACKFILL_CHUNK_DELAY` | `0` | Pause between chunks (throttle source load) |
| `CDC_BACKFILL_STATE_TABLE` | `gtc_backfill_state` | Progress table in the source DB (`none` disables resume) |
| `SINK_CONFIG_FILE` | – | Path to the per-table sink YAML (below) |
| `REDIS_URL` | – | Enables the Redis Stream sink |
| `REDIS_STREAM_PREFIX` | `cdc` | Stream key prefix |
| `REDIS_MAX_STREAM_LEN` | `10000` | Approximate max stream length (`XADD MAXLEN ~`) |
| `REDIS_JSON_URL` | – | Enables the RedisJSON sink (requires Redis Stack / RedisJSON module) |
| `REDIS_JSON_PREFIX` | `cdc` | RedisJSON key prefix |
| `MEILISEARCH_URL` | – | Enables the Meilisearch sink |
| `MEILISEARCH_API_KEY` | – | Meilisearch API key |

### Per-table sink routing

Without `SINK_CONFIG_FILE`, both Redis sinks mirror **all** tables with
default key patterns. To route selectively, provide a YAML file (see
[`config/sinks.example.yaml`](config/sinks.example.yaml)):

```yaml
redis_stream:
  sync_all: true                                   # everything, plus overrides below
  default_key_pattern: "{{.Prefix}}:{{.Schema}}:{{.Table}}"
  tables:
    public.orders: "{{.Prefix}}:orders:{{.Field \"status\"}}"

redis_json:
  sync_all: false                                  # only the listed tables
  tables:
    public.users: "{{.Prefix}}:users:{{.Field \"id\"}}"

meilisearch:
  tables:                                          # table -> index name
    public.products: products
```

Key patterns are Go templates with access to `{{.Prefix}}`, `{{.Schema}}`,
`{{.Table}}`, `{{.Operation}}`, and `{{.Field "column"}}`.

> RedisJSON key patterns should only reference **replica identity** columns
> (normally the primary key). Other columns are absent from DELETE events,
> so a key built from them would miss the document it is meant to delete.
> The Meilisearch sink expects documents to have an `id` column.

## Backfill and replay

GTC syncs *existing* data, not just new changes. When the replication slot is
first created (a brand-new deployment), every table in the publication is
backfilled automatically. The algorithm is the watermark approach from
Netflix's DBLog paper:

1. Rows are read in primary-key-ordered chunks over a **regular** connection —
   no locks, no long-running snapshot transaction.
2. Each chunk is bracketed by low/high watermarks written into the WAL via
   `pg_logical_emit_message()`, so the watermarks travel *inside* the
   replication stream.
3. Any live change observed between the watermarks supersedes the matching
   chunk row; the remaining rows are emitted as `READ` events exactly at the
   high watermark's stream position. Per-key ordering holds, and live
   streaming never pauses.

Progress is persisted per table (in `gtc_backfill_state` in the source
database), so an interrupted backfill resumes where it left off. Tables
without a primary key are skipped with a warning.

The HTTP API doubles as a **replay** mechanism — re-sync a table into the
sinks at any time (e.g. after wiping a search index):

```bash
curl -X POST localhost:8080/backfill -d '{"table":"public.products"}'
curl -X POST localhost:8080/backfill -d '{"all":true}'
curl localhost:8080/backfill            # per-table progress
```

## Delivery semantics

GTC is **at-least-once**:

1. Events are decoded from the WAL and handed to every sink synchronously,
   in commit order.
2. The LSN is confirmed to PostgreSQL only when the *transaction* completes —
   after every sink processed every event in it.
3. If any sink fails (after retries and the circuit breaker), the replication
   connection is torn down and PostgreSQL redelivers everything after the
   last confirmed LSN on reconnect.

Consequences worth knowing:

- A down sink **stalls the pipeline** rather than losing data; WAL retention
  on the slot grows until the sink recovers. Monitor `cdc_wal_lag_bytes`.
- Sinks (and stream consumers) must tolerate duplicates. The built-in sinks
  are idempotent; consumers should dedupe by `event_id`.
- Large in-progress transactions are not streamed before commit by design —
  a streamed transaction can still abort, and GTC refuses to publish data
  that was never committed.

## Observability

| Endpoint | Purpose |
|----------|---------|
| `GET /health` | Liveness — process is up |
| `GET /readiness` | Readiness — live replication stream **and** all sinks healthy |
| `GET /metrics` | Prometheus metrics |
| `GET /backfill` | Backfill progress per table |
| `POST /backfill` | Trigger a backfill/replay (`{"table":"schema.table"}` or `{"all":true}`) |

Key metrics (all prefixed `cdc_`): `events_received_total`,
`events_processed_total{sink,status}`, `event_processing_duration_seconds`,
`sink_errors_total{sink,error_type}`, `retry_attempts_total`,
`circuit_breaker_state` (0 closed / 1 half-open / 2 open), `wal_lag_bytes`,
`last_processed_lsn`, `inflight_events`, `events_excluded_total`,
`active_sinks`.

## Architecture

Hexagonal architecture wired with Uber FX:

```
cmd/gateway/           # FX bootstrap (main.go) and module wiring (modules.go)
internal/
  core/
    domain/            # CDCEvent, Sink interface, domain errors
    ports/             # WALReader, CDCService, SinkRegistry interfaces
    services/          # CDC orchestration and sink registry
  adapters/
    primary/wal/       # Replication connection + pgoutput decoder
    primary/backfill/  # Watermark-based chunked backfill coordinator
    secondary/
      redis/           # Redis Stream sink + shared base/key templates
      redisjson/       # RedisJSON document sink
      meilisearch/     # Meilisearch indexing sink
  infrastructure/
    config/            # Env + sink YAML loading
    resilience/        # Retry + circuit breaker wrapper for sinks
    server/            # HTTP health/readiness/metrics
    metrics/           # Prometheus collectors
```

The core never imports an adapter; sinks implement a five-method
`domain.Sink` interface and are registered in one place
(`newSinks` in `cmd/gateway/modules.go`). Adding a sink for another store is
a small, self-contained change — see
[CONTRIBUTING.md](CONTRIBUTING.md#adding-a-new-sink).

## When *not* to use GTC

- You need exactly-once end-to-end, multi-consumer replay with long
  retention, or schema-registry integration → use Debezium + Kafka.
- You need sinks GTC doesn't have and don't want to write one → check the
  Kafka Connect ecosystem first.
- Your consumers can't tolerate duplicate events and can't dedupe.

## Contributing

Contributions are welcome — bug reports, docs, new sinks. Please read
[CONTRIBUTING.md](CONTRIBUTING.md) and the
[Code of Conduct](CODE_OF_CONDUCT.md) first. Security issues should follow
[SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE)
