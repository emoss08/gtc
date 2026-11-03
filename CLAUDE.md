# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GTC is a PostgreSQL Change Data Capture (CDC) platform written in Go. It captures changes directly from PostgreSQL WAL and routes them to configurable sinks (Redis streams, RedisJSON, Meilisearch, NATS/JetStream, webhooks). Built with hexagonal architecture and Uber FX for dependency injection.

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
processed every event up to that commit. If any sink fails, the replication
connection is torn down and the unconfirmed events are redelivered on
reconnect — so **sinks must tolerate duplicate events** (the built-in sinks are
idempotent; stream consumers should dedupe by `event_id`).

Events reach sinks in **batches**: the reader closes a batch at
`CDC_BATCH_MAX_EVENTS`, after `CDC_BATCH_LINGER` since its first event, or
before a backfill watermark. A batch may span transactions and may hold part
of a large one; positions are confirmed only after the batch holding every
event before them was handled. `Sink.Process` receives `[]CDCEvent` in stream
order and must apply them in order (one row can appear several times in a
batch). Sinks pay one round trip per batch, not per row: Meilisearch collapses
consecutive same-kind events per index into one task and awaits all tasks with
one poll; the Redis sinks pipeline; JetStream publishes asynchronously in
windows. A sink whose unit is one event (webhook) implements
`domain.BatchLimiter`, and the pipeline splits batches to that size before the
DLQ and retry layers see them.

Errors caused by the data (unrenderable key, a document the destination
rejects) are wrapped with `domain.Permanent`: they are not retried and do not
trip the circuit breaker.

Consequences to keep in mind:

- A sink that is down does not lose events; it stalls the pipeline (replication
  slot WAL retention grows) until the sink recovers. A *poison event* is
  parked in the Redis dead-letter queue instead, and triaged via GET /dlq,
  POST /dlq/retry, POST /dlq/discard. The DLQ layer isolates it by bisecting
  the failing batch (halves in order; succeeding halves are delivered): at
  once for a permanent error, after CDC_DLQ_THRESHOLD failure cycles of the
  batch (counted against its first event, stable across replays) for a
  transient one. Circuit-open failures and timeouts never park (a down or
  slow sink is not a poison event), and a full/unreachable DLQ stalls rather
  than drops. Sink layering is transform(dlq(resilience(sink))): parked
  entries hold the already-transformed event, and retries bypass transforms
  so masking is never re-applied.
- `CDC_PROCESS_TIMEOUT` (default 30s) bounds one delivery attempt of one batch
  to one sink. The reader keeps sending standby keepalives from a side
  goroutine while sinks work, so it is independent of `wal_sender_timeout`;
  only `CDC_STANDBY_TIMEOUT` must stay well inside it (`gtc doctor` checks).
  A Meilisearch attempt that times out leaves its tasks running: the sink
  remembers them and a retry of the same batch resumes waiting instead of
  enqueueing it again.
- Several nodes may share one slot: PostgreSQL lets one stream at a time, the
  others retry every `CDC_SLOT_RETRY_INTERVAL` and take over when it is free
  (hot standby). The backfill worker runs only while its node streams,
  chunk ids carry a per-process instance id, and a node re-reads a table's
  persisted state before running it, so a takeover resumes another node's
  progress. Tables are persisted as pending before they are queued, so a
  restart mid-backfill resumes every queued table.
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
- `POST /backfill` replays from the start: `{"table":...}` one table, `{"all":true}` every published table, completed or not (`ReplayAll`). The startup auto-backfill uses `EnqueueAll`, which only fills tables not yet done. `gtc_backfill_state` persists cursor, done and rows_copied; on start the coordinator loads it so completed backfills show in status after a restart.
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
| CDC_PARALLEL_SINKS | true | Deliver each batch to all sinks concurrently |
| CDC_PROCESS_TIMEOUT | 30s | Timeout of one delivery attempt of one batch to one sink |
| CDC_BATCH_MAX_EVENTS | 5000 | Most events handed to the sinks at once |
| CDC_BATCH_LINGER | 10ms | Longest an event waits for its batch to fill when the stream is quiet |
| CDC_EXCLUDED_TABLES | - | Comma-separated tables to exclude (e.g., public.migrations,audit_logs) |
| CDC_SLOT_RETRY_INTERVAL | 5s | Wait between retries when the slot is busy |
| CDC_SLOT_RETRY_TIMEOUT | 60s | Give up starting replication after this long |
| CDC_MAX_RETRIES | 3 | Per-sink retry attempts before failing the batch |
| CDC_RETRY_BACKOFF_INITIAL | 100ms | Initial retry backoff |
| CDC_RETRY_BACKOFF_MAX | 10s | Maximum retry backoff |
| CDC_CIRCUIT_BREAKER_THRESHOLD | 5 | Consecutive failures before the circuit opens |
| CDC_CIRCUIT_BREAKER_TIMEOUT | 30s | How long an open circuit stays open |
| CDC_BACKFILL_MODE | auto | auto = backfill all tables when the slot is first created; manual = API only; off |
| CDC_BACKFILL_CHUNK_SIZE | 5000 | Rows per backfill chunk (a shorter chunk ends the table) |
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

Key-templated sinks (redis_stream, redis_json, webhook, nats) mirror every
table when SINK_CONFIG_FILE is unset; with a config file present, each sink
needs `sync_all: true` or an explicit `tables` map or it delivers nothing
(`gtc doctor` warns about that case). Meilisearch table entries may set `primary_key` (default `id`), `searchable_attributes` and `filterable_attributes`; the sink applies them at startup (creating the index with its key, updating attributes only when they differ). The Meilisearch sink only indexes
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
