# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GTC is a PostgreSQL Change Data Capture (CDC) platform written in Go. It captures changes directly from PostgreSQL WAL and routes them to configurable sinks (Redis streams, RedisJSON, Meilisearch, NATS/JetStream, webhooks, ClickHouse). Built with hexagonal architecture and Uber FX for dependency injection.

## Build, Test, and Run Commands

```bash
go build -o gateway ./cmd/gateway
go test ./...
go run ./cmd/gateway
go run ./cmd/gateway doctor   # preflight checks (PG settings, privileges, sinks); exit 1 on failure

# End-to-end test (real PostgreSQL wal_level=logical + Redis; also run in CI):
GTC_TEST_DATABASE_URL=postgres://... GTC_TEST_REDIS_URL=redis://... \
  go test -tags integration -timeout 5m ./test/integration

# Dashboard (ui/): the built dist/ is committed and embedded via go:embed,
# so plain `go build` works without Node. After changing ui/src, rebuild:
cd ui && npm ci && npm run build
# Dev server with API proxied to a running gateway on :8080:
cd ui && npm run dev
```

The codebase is pure Go (CGO_ENABLED=0) and must keep building for
linux/darwin/windows on amd64/arm64 — CI cross-compiles all six targets and
runs the test suite on Linux, Windows, and macOS. Releases are cut by
pushing a `v*` tag (goreleaser builds the binaries; a multi-arch Docker
image goes to ghcr.io).

## Delivery Semantics

GTC provides **at-least-once** delivery. The WAL position is only confirmed to
PostgreSQL at transaction commit boundaries, after every sink has successfully
processed every event in the transaction. If any sink fails, the replication
connection is torn down and the unconfirmed events are redelivered on
reconnect — so **sinks must tolerate duplicate events** (the built-in sinks are
idempotent; stream consumers should dedupe by `event_id`).

Consequences to keep in mind:

- A sink that is down does not lose events; it stalls the pipeline (replication
  slot WAL retention grows) until the sink recovers. A *poison event* (same
  event failing the same sink CDC_DLQ_THRESHOLD times) is parked in the Redis
  dead-letter queue instead, and triaged via GET /dlq, POST /dlq/retry,
  POST /dlq/discard. Circuit-open failures never park (a down sink is not a
  poison event), and a full/unreachable DLQ stalls rather than drops. Sink
  layering is transform(dlq(resilience(sink))): parked entries hold the
  already-transformed event, and retries bypass transforms so masking is
  never re-applied.
- `CDC_PROCESS_TIMEOUT` (default 10s) must stay small enough that
  sink-count × timeout is below PostgreSQL's `wal_sender_timeout` (default
  60s), because sinks run synchronously with the WAL stream and standby
  keepalives are not sent mid-event.
- Unchanged TOAST columns are omitted from `NewData` (never sent as
  placeholder strings) and listed in `UnchangedToastColumns`. Sinks handle
  this with partial updates (Meilisearch `UpdateDocuments`, RedisJSON
  `JSON.MERGE`).
- Schema (DDL) changes are detected by diffing pgoutput relation
  descriptions, which PostgreSQL re-sends before a changed table's next row
  change. They are **best-effort**, not at-least-once: a reconnect
  re-describes tables with nothing to diff against, so a missed change cannot
  be redelivered. Observers must therefore never fail the WAL stream —
  `ports.SchemaObserver` returns no error by design.
- The ClickHouse sink mirrors each table as
  ReplacingMergeTree(_version, _deleted) keyed by the source primary key,
  versioned by LSN. It introspects the source catalog for column types (CDC
  events carry values, not types), carries unchanged TOAST columns forward by
  reading the stored row, and implements ports.SchemaObserver so an upstream
  ADD COLUMN reaches the mirror. Tables without a primary key are skipped
  with an error rather than mirrored wrongly.
- Backfill emits `READ` events for existing rows, interleaved with the live
  stream via WAL watermarks (`pg_logical_emit_message`, prefix
  `gtc-backfill`). Chunk rows superseded by live events between the low and
  high watermarks are dropped, preserving per-key ordering. Sinks must treat
  `OperationRead` like an insert of the row's current state.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| LOG_LEVEL | INFO | Log level: DEBUG, INFO, WARN, ERROR |
| DATABASE_URL | postgres://...?replication=database | PostgreSQL connection (must include ?replication=database) |
| HTTP_PORT | 8080 | Port for health/readiness/metrics HTTP server |
| CDC_SLOT_NAME | cdc_demo_slot | Replication slot name |
| CDC_PUBLICATION_NAME | cdc_demo_publication | Publication name |
| CDC_AUTO_CREATE_SLOT | true | Create the replication slot if missing |
| CDC_AUTO_CREATE_PUBLICATION | true | Create the publication (FOR ALL TABLES) if missing |
| CDC_STANDBY_TIMEOUT | 10s | Standby message interval |
| CDC_PARALLEL_SINKS | false | Process sinks in parallel |
| CDC_PROCESS_TIMEOUT | 10s | Per-sink, per-event processing timeout |
| CDC_EXCLUDED_TABLES | - | Comma-separated tables to exclude (e.g., public.migrations,audit_logs) |
| CDC_SLOT_RETRY_INTERVAL | 5s | Wait between retries when the slot is busy |
| CDC_SLOT_RETRY_TIMEOUT | 60s | Give up starting replication after this long |
| CDC_MAX_RETRIES | 3 | Per-sink retry attempts before failing the event |
| CDC_RETRY_BACKOFF_INITIAL | 100ms | Initial retry backoff |
| CDC_RETRY_BACKOFF_MAX | 10s | Maximum retry backoff |
| CDC_CIRCUIT_BREAKER_THRESHOLD | 5 | Consecutive failures before the circuit opens |
| CDC_CIRCUIT_BREAKER_TIMEOUT | 30s | How long an open circuit stays open |
| CDC_BACKFILL_MODE | auto | auto = backfill all tables when the slot is first created; manual = API only; off |
| CDC_BACKFILL_CHUNK_SIZE | 1000 | Rows per backfill chunk |
| CDC_BACKFILL_CHUNK_DELAY | 0 | Pause between chunks |
| CDC_BACKFILL_STATE_TABLE | gtc_backfill_state | Progress table in the source DB ("none" disables resume) |
| CDC_DLQ_ENABLED | true | Park poison events in Redis instead of stalling forever (needs REDIS_URL) |
| CDC_DLQ_THRESHOLD | 3 | Failure cycles for the same event before it is parked |
| CDC_DLQ_MAX_ENTRIES | 10000 | DLQ cap; when full, parking fails and the pipeline stalls |
| CDC_MASK_HMAC_KEY | - | Secret key for the hmac256 mask strategy (startup fails if a transform uses hmac256 without it) |
| CDC_SCHEMA_EVENTS | true | Publish detected DDL changes to the <prefix>:schema Redis stream (detection/metrics/API always on) |
| SINK_CONFIG_FILE | - | Path to per-table sink YAML (see config/sinks.example.yaml). Without it, key-templated sinks mirror all tables |
| REDIS_URL | - | Redis connection (enables stream sink if set) |
| REDIS_STREAM_PREFIX | cdc | Stream key prefix |
| REDIS_MAX_STREAM_LEN | 10000 | Max stream length |
| REDIS_JSON_URL | - | Redis connection (enables RedisJSON sink if set) |
| REDIS_JSON_PREFIX | cdc | RedisJSON key prefix |
| MEILISEARCH_URL | - | Meilisearch URL (enables sink if set) |
| MEILISEARCH_API_KEY | - | Meilisearch API key |
| NATS_URL | - | NATS connection (enables NATS sink if set) |
| NATS_SUBJECT_PREFIX | cdc | Subject prefix: <prefix>.<schema>.<table> |
| NATS_JETSTREAM | true | Publish via JetStream (acked + deduped by LSN); false = fire-and-forget |
| NATS_STREAM | GTC | JetStream stream capturing <prefix>.> |
| NATS_AUTO_CREATE_STREAM | true | Create that stream when missing |
| NATS_CREDENTIALS / NATS_TOKEN | - | NATS auth |
| NATS_TIMEOUT | 5s | Connect/publish/flush timeout |
| WEBHOOK_URL | - | HTTP endpoint (enables webhook sink if set) |
| WEBHOOK_SIGNING_SECRET | - | HMAC-SHA256 key for the X-GTC-Signature header |
| WEBHOOK_AUTH_HEADER | - | Sent verbatim as Authorization |
| WEBHOOK_TIMEOUT | 5s | Per-delivery HTTP timeout |
| WEBHOOK_MAX_IDLE_CONNS | 10 | Connection pool size |
| CLICKHOUSE_URL | - | ClickHouse DSN (enables the analytics mirror if set) |
| CLICKHOUSE_DATABASE | gtc | Database holding mirrored tables |
| CLICKHOUSE_TABLE_PREFIX | - | Prefix for generated table names |
| CLICKHOUSE_AUTO_CREATE_TABLES | true | Create/extend target tables from the source schema |
| CLICKHOUSE_ASYNC_INSERT | true | Server-side batching of the stream's small inserts |
| CLICKHOUSE_WAIT_FOR_INSERT | true | Wait for the flush; false weakens at-least-once |
| CLICKHOUSE_TIMEOUT | 10s | Connect/insert/query timeout |

Key-templated sinks (redis_stream, redis_json, webhook, nats) mirror every
table when SINK_CONFIG_FILE is unset; with a config file present, each sink
needs `sync_all: true` or an explicit `tables` map or it delivers nothing
(`gtc doctor` warns about that case). The Meilisearch sink only indexes
tables mapped in `SINK_CONFIG_FILE` under `meilisearch.tables`. RedisJSON key patterns should only reference replica
identity columns (normally the primary key); other columns are absent from
DELETE events, which would make the delete target the wrong key.

Sink YAML also carries declarative transforms (per sink `transform:` block
and per-table object entries): CEL `filter` expressions (variables op,
schema, table, new, old), `drop_columns`, and `mask` strategies (redact,
null, sha256, hmac256 — keyed by CDC_MASK_HMAC_KEY — and last4).
Transforms compile at startup (fail fast), are applied
per sink outside the resilience wrapper, and never mutate the shared event —
see `internal/infrastructure/transform` and config/sinks.example.yaml.

An `outbox:` section in the sink YAML enables the transactional outbox sink:
INSERTs into the configured table publish to Redis stream
`<stream_prefix>:<topic>` (fields id/type/key/payload), with optional
delete-after-publish. The outbox table is auto-excluded from the mirroring
sinks and from backfill. Requires REDIS_URL.

## Architecture

Hexagonal architecture with Uber FX dependency injection:

```
cmd/gateway/           # FX bootstrap (main.go) and module wiring (modules.go)
internal/
  core/
    domain/            # CDCEvent, Sink interface, domain errors
    ports/             # WALReader, CDCService, SinkRegistry interfaces
    services/          # CDCService and SinkRegistry implementations
  adapters/
    primary/wal/       # PostgreSQL WAL reader and pgoutput decoder
    primary/backfill/  # Watermark-based chunked backfill (DBLog algorithm)
    secondary/
      redis/           # Redis stream sink + shared base/key templates
      redisjson/       # RedisJSON document sink
      meilisearch/     # Meilisearch indexing sink
      outbox/          # Transactional outbox publisher (INSERTs -> events:<topic> streams)
  infrastructure/
    config/            # Configuration loading from env vars and sink YAML
    transform/         # Declarative per-sink transforms (CEL filters, masking)
    resilience/        # Circuit breaker and retry wrapper for sinks
    dlq/               # Dead-letter queue (Redis store, parking decorator, triage)
    doctor/            # `gtc doctor` preflight checks
    schema/            # DDL change history, logging, and Redis notification stream
    server/            # HTTP server: health/readiness/metrics, APIs, dashboard
    metrics/           # Prometheus metrics
ui/                    # React 19 + Tailwind dashboard; dist/ committed + embedded
```

## Adding New Sinks

1. Create package under `internal/adapters/secondary/<sink_name>/`
2. Implement `domain.Sink` interface (Name, Initialize, Process, Shutdown, HealthCheck)
3. Add config loading in the package (enable the sink when its URL env var is set)
4. Register it in `newSinks` in `cmd/gateway/modules.go`, wrapped with `resilience.NewResilientSink`
5. Sinks must be idempotent (at-least-once delivery) and must respect
   `UnchangedToastColumns` if they replace whole documents

## Key Dependencies

- `go.uber.org/fx` - Dependency injection
- `github.com/jackc/pglogrepl` - PostgreSQL logical replication
- `github.com/jackc/pgx/v5` - PostgreSQL driver
- `github.com/redis/go-redis/v9` - Redis client
- `github.com/meilisearch/meilisearch-go` - Meilisearch client
- `github.com/sony/gobreaker/v2` - Circuit breaker

## Code Style

- Follow Uber Go Style Guide
- Follow DRY and SOLID principles
