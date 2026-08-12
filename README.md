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
                                       ├──▶ Meilisearch     (search index)
                                       ├──▶ NATS JetStream  (durable pub/sub)
                                       ├──▶ ClickHouse      (analytics mirror)
                                       └──▶ Webhooks        (any HTTP endpoint)
```

## Features

- **Single binary, plug and play** — point it at a database with
  `wal_level=logical`, set a Redis URL, and every table is mirrored. The
  replication slot and publication are created automatically, and
  `gtc doctor` verifies the whole environment (settings, privileges, sinks)
  before the first run.
- **Lock-free initial backfill** — on first start, existing table data is
  synced to the sinks *concurrently with live streaming* using watermark
  chunking (the DBLog algorithm): no table locks, no paused replication, and
  progress survives restarts. New tables (or re-syncs) can be backfilled at
  any time via the HTTP API.
- **At-least-once delivery** — the WAL position is confirmed to PostgreSQL
  only at transaction commit boundaries, after every sink has processed every
  event. A failed sink means teardown and replay, never silent loss.
- **Sinks built in** — Redis Streams (fan-out event bus with `XADD`),
  RedisJSON (one JSON document per row, partial merges on update),
  Meilisearch (partial document updates, waits for task completion so
  indexing failures surface as errors), NATS JetStream (acknowledged,
  server-side deduplicated publishes), ClickHouse (an analytics mirror whose
  tables are created — and extended on upstream `ALTER` — from the source
  schema), and webhooks (a signed POST per event to any HTTP endpoint). Every sink is independently filtered, transformed,
  retried and circuit-broken.
- **Correct TOAST handling** — unchanged TOAST columns are omitted and
  reported, not replaced with placeholder garbage; sinks merge instead of
  clobbering.
- **Per-table routing** — an optional YAML file selects which tables go to
  which sink and how keys are built, using Go templates
  (`{{.Prefix}}:orders:{{.Field "id"}}`).
- **Declarative transforms** — PII masking (`sha256`, keyed `hmac256`,
  `last4`, `redact`, `null`), column dropping, and CEL row filters,
  configured per sink and per table in YAML. The stream can carry full rows while the search index gets
  masked ones — no plugin code, no SMT classes.
- **Schema-change awareness** — GTC notices when a published table gains,
  loses or retypes a column, changes replica identity, or is renamed, and
  reports it on the dashboard, in `/api/schema`, in Prometheus, and on a
  Redis notification stream — flagging changes that can break consumers. No
  DDL triggers, no schema-history topic, no superuser.
- **First-class transactional outbox** — write domain events to an outbox
  table in the same transaction as your business writes, and GTC publishes
  them to per-topic Redis streams with optional delete-after-publish. Five
  lines of YAML instead of Debezium's SMT routing configuration.
- **Resilience** — per-sink retries with exponential backoff and a circuit
  breaker that fails fast while a sink is down (the WAL position simply
  doesn't advance until it recovers).
- **Dead-letter queue with a triage API** — a poison event that repeatedly
  fails one sink is parked in Redis instead of stalling the pipeline
  forever, then inspected, retried, or discarded over HTTP. Sink *outages*
  still stall (a down sink is not a poison event) — nothing is ever
  silently dropped.
- **Embedded live dashboard** — a React dashboard served from the binary at
  `/` (PocketBase-style, no separate deployment): live throughput, WAL lag,
  sink health and breaker states, per-table activity, schema-change history,
  backfill progress, and DLQ triage with one-click retry/discard. Light and
  dark themes.
- **Observability** — Prometheus metrics (events, per-sink latency, errors,
  retries, breaker state, WAL lag in bytes), health and readiness endpoints,
  structured JSON logs.

![GTC dashboard — pipeline overview](docs/dashboard.png)

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

GTC is a single, pure-Go binary and runs natively on **Linux, macOS, and
Windows** (amd64 and arm64). Grab a prebuilt binary from the
[releases page](https://github.com/emoss08/gtc/releases), pull the multi-arch
Docker image (`ghcr.io/emoss08/gtc`), or build from source:

```bash
go build -o gateway ./cmd/gateway

DATABASE_URL="postgres://postgres:postgres@localhost:5432/mydb?replication=database" \
REDIS_URL="redis://localhost:6379" \
./gateway
```

On Windows (PowerShell):

```powershell
go build -o gateway.exe ./cmd/gateway

$env:DATABASE_URL = "postgres://postgres:postgres@localhost:5432/mydb?replication=database"
$env:REDIS_URL = "redis://localhost:6379"
.\gateway.exe
```

That's the whole setup: with no further configuration GTC mirrors every table
in the publication to Redis. Add `REDIS_JSON_URL` and/or `MEILISEARCH_URL` to
enable the other sinks. `gateway --version` prints the build version.

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

### Preflight: `gtc doctor`

Before the first run (or when something is off), let GTC diagnose the
environment instead of reading startup logs:

```bash
DATABASE_URL="postgres://...?replication=database" REDIS_URL="redis://..." \
./gateway doctor
```

It verifies everything in one pass — `wal_level`, replication privileges
(including a real `IDENTIFY_SYSTEM` handshake), free replication slots and
WAL senders, publication/slot existence vs. auto-create permissions, tables
that would reject UPDATE/DELETE for lack of a replica identity, sink
reachability (Redis `PING`, RedisJSON module presence, Meilisearch health),
transform compilation, and whether your sink timeouts fit inside
`wal_sender_timeout` — then exits 0 if GTC is ready to stream, 1 if
something needs fixing, with a concrete hint per finding:

```
PostgreSQL
  ✓ connection             PostgreSQL 16.13
  ✗ wal_level              "replica" — logical replication needs wal_level=logical
                           (ALTER SYSTEM SET wal_level = 'logical'; then restart PostgreSQL)
  ✓ privileges             user "postgres" is superuser
  ...
```

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
| `CDC_DLQ_ENABLED` | `true` | Park poison events in Redis instead of stalling forever (needs `REDIS_URL`) |
| `CDC_DLQ_THRESHOLD` | `3` | Failure cycles for the same event before it is parked |
| `CDC_DLQ_MAX_ENTRIES` | `10000` | DLQ cap; when full, parking fails and the pipeline stalls |
| `CDC_MASK_HMAC_KEY` | – | Secret key for the `hmac256` mask strategy (required when a transform uses it) |
| `CDC_SCHEMA_EVENTS` | `true` | Publish detected DDL changes to the `<prefix>:schema` Redis stream (detection, metrics and `/api/schema` are always on) |
| `SINK_CONFIG_FILE` | – | Path to the per-table sink YAML (below) |
| `REDIS_URL` | – | Enables the Redis Stream sink |
| `REDIS_STREAM_PREFIX` | `cdc` | Stream key prefix |
| `REDIS_MAX_STREAM_LEN` | `10000` | Approximate max stream length (`XADD MAXLEN ~`) |
| `REDIS_JSON_URL` | – | Enables the RedisJSON sink (requires Redis Stack / RedisJSON module) |
| `REDIS_JSON_PREFIX` | `cdc` | RedisJSON key prefix |
| `MEILISEARCH_URL` | – | Enables the Meilisearch sink |
| `MEILISEARCH_API_KEY` | – | Meilisearch API key |
| `NATS_URL` | – | Enables the NATS sink (`nats://host:4222`; comma-separated for a cluster) |
| `NATS_SUBJECT_PREFIX` | `cdc` | Subject prefix: `<prefix>.<schema>.<table>` |
| `NATS_JETSTREAM` | `true` | Publish through JetStream (acknowledged + deduplicated). `false` is fire-and-forget |
| `NATS_STREAM` | `GTC` | JetStream stream capturing `<prefix>.>` |
| `NATS_AUTO_CREATE_STREAM` | `true` | Create that stream when missing |
| `NATS_CREDENTIALS` | – | Path to a NATS `.creds` file |
| `NATS_TOKEN` | – | NATS auth token |
| `NATS_TIMEOUT` | `5s` | Connect, publish and flush timeout |
| `WEBHOOK_URL` | – | Enables the webhook sink; receives one POST per event |
| `WEBHOOK_SIGNING_SECRET` | – | HMAC-SHA256 key for `X-GTC-Signature` (strongly recommended) |
| `WEBHOOK_AUTH_HEADER` | – | Sent verbatim as the `Authorization` header |
| `WEBHOOK_TIMEOUT` | `5s` | Per-delivery HTTP timeout |
| `WEBHOOK_MAX_IDLE_CONNS` | `10` | Connection pool size for the receiver |
| `CLICKHOUSE_URL` | – | Enables the ClickHouse sink (`clickhouse://user:pass@host:9000/db`) |
| `CLICKHOUSE_DATABASE` | `gtc` | Database holding the mirrored tables; created when missing |
| `CLICKHOUSE_TABLE_PREFIX` | – | Prefix for generated table names |
| `CLICKHOUSE_AUTO_CREATE_TABLES` | `true` | Create and extend target tables from the source schema |
| `CLICKHOUSE_ASYNC_INSERT` | `true` | Let ClickHouse batch the stream's small inserts |
| `CLICKHOUSE_WAIT_FOR_INSERT` | `true` | Wait for the batch to flush before reporting success — **turning this off weakens at-least-once** |
| `CLICKHOUSE_TIMEOUT` | `10s` | Connect, insert and query timeout |

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

### Transforms: masking, filtering, dropping columns

Every table entry can also be an object carrying transforms, and each sink
section accepts a sink-wide `transform:` block (applied before the table's
own). This is per sink by design — your event stream can keep full fidelity
while your search index never sees an email address:

```yaml
redis_json:
  transform:
    drop_columns: [password_hash]        # applies to every table in this sink
  tables:
    public.users:
      key: "{{.Prefix}}:users:{{.Field \"id\"}}"
      mask:
        email: sha256                    # deterministic hash (joinable)
        phone: last4                     # ****1234
      filter: 'new.deleted_at == null'   # CEL: deliver only when true
```

- **`filter`** is a [CEL](https://github.com/google/cel-go) expression with
  variables `op`, `schema`, `table`, `new`, and `old`. Accessing a column
  that's absent from the event is an error (visible, not silent data loss) —
  guard optional columns with `"col" in new && new.col == ...`.
- **`mask`** strategies: `redact` (`[REDACTED]`), `null`, `sha256`
  (deterministic, so masked values still join/dedupe), `hmac256`
  (deterministic like `sha256`, but keyed with `CDC_MASK_HMAC_KEY` so the
  original value can't be recovered by hashing guessed inputs — use this for
  low-entropy PII like emails and phone numbers), `last4`
  (`************4242`).
- **`drop_columns`** removes columns entirely.

Transforms are compiled at startup — a typo in a CEL expression, an unknown
mask strategy, or `hmac256` without `CDC_MASK_HMAC_KEY` fails boot with a
clear error instead of surfacing mid-stream. Note that rotating the HMAC key
changes every digest, so previously-emitted masked values stop matching new
ones.
Masking runs before key generation, so don't build sink keys from masked
columns. Filtered events count in `cdc_events_filtered_total{sink,schema,table}`.

> RedisJSON key patterns should only reference **replica identity** columns
> (normally the primary key). Other columns are absent from DELETE events,
> so a key built from them would miss the document it is meant to delete.
> The Meilisearch sink expects documents to have an `id` column.

### Transactional outbox

The [outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html)
is the reliable way to publish domain events: write the event to an outbox
table **in the same transaction** as your business change, and let CDC turn
it into a message — no dual-write race, no lost events. GTC makes it a config
block:

```yaml
outbox:
  table: public.outbox
  delete_after_publish: true
```

```sql
CREATE TABLE outbox (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  topic        text NOT NULL,          -- destination stream: events:<topic>
  event_type   text,                   -- e.g. OrderPlaced
  aggregate_id text,                   -- e.g. the order id
  payload      jsonb NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now()
);

-- In your application, inside the business transaction:
INSERT INTO outbox (topic, event_type, aggregate_id, payload)
VALUES ('orders', 'OrderPlaced', '42', '{"total": 99}');
```

Each INSERT is published to the Redis stream `events:<topic>` with fields
`id` (dedupe key), `type`, `key`, and `payload`. Column names, the stream
prefix, and a fallback `default_topic` are configurable (see
[`config/sinks.example.yaml`](config/sinks.example.yaml)).

Behavior worth knowing:

- The outbox table is **automatically excluded** from the data-mirroring
  sinks and from backfill — its rows are messages, not data, and a backfill
  must not republish history.
- With `delete_after_publish: true`, rows are deleted once published (a
  failed delete is logged and left for cleanup — the message is already
  out, so it never blocks the pipeline).
- Delivery is at-least-once like everything else; consumers dedupe by the
  `id` field.

## Webhook and NATS sinks

Both are enabled the same way as every other sink — set a URL — and both
respect per-table filtering and transforms from `SINK_CONFIG_FILE`.

### Webhook

```bash
WEBHOOK_URL="https://example.com/hooks/gtc" \
WEBHOOK_SIGNING_SECRET="$(openssl rand -hex 32)" ./gateway
```

Each event is one `POST` with the same JSON body the Redis stream carries,
plus headers:

| Header | Purpose |
|--------|---------|
| `X-GTC-Event-Id` | The event's LSN — **dedupe on this**, delivery is at-least-once |
| `X-GTC-Table` | `schema.table`, for cheap routing without parsing the body |
| `X-GTC-Operation` | `INSERT`/`UPDATE`/`DELETE`/`READ`/`TRUNCATE` |
| `X-GTC-Timestamp` | Unix seconds, signed alongside the body |
| `X-GTC-Signature` | `sha256=<hex>` — HMAC-SHA256 over `"<timestamp>.<body>"` |

Verify a delivery (Go):

```go
mac := hmac.New(sha256.New, []byte(secret))
mac.Write([]byte(r.Header.Get("X-GTC-Timestamp") + "."))
mac.Write(body)
ok := hmac.Equal([]byte(r.Header.Get("X-GTC-Signature")),
    []byte("sha256="+hex.EncodeToString(mac.Sum(nil))))
```

Any 2xx is success. Anything else — or a timeout — fails the event, so it is
retried and eventually dead-lettered rather than silently lost, and the WAL
position does not advance past it. Because the receiver is in the replication
path, keep it fast: `WEBHOOK_TIMEOUT` defaults to 5s and sink-count × timeout
must stay under `wal_sender_timeout` (`gtc doctor` checks this).

### NATS

```bash
NATS_URL="nats://localhost:4222" ./gateway
```

Events publish to `<NATS_SUBJECT_PREFIX>.<schema>.<table>`, and the stream
capturing `<prefix>.>` is created automatically. JetStream is the default
because it acknowledges each publish — a delivery is only counted once the
server has persisted it — and because publishing with the event's LSN as the
message ID lets **JetStream collapse the duplicates** that at-least-once
redelivery produces. Setting `NATS_JETSTREAM=false` falls back to core NATS,
which is fire-and-forget: events vanish when nobody is subscribed, so
`gtc doctor` warns about it.

Subjects come from the same template engine as Redis keys, so a table can be
routed by column value:

```yaml
nats:
  sync_all: true
  tables:
    public.orders: "{{.Prefix}}.orders.{{.Field \"status\"}}"
```

Wildcards and whitespace that PostgreSQL allows in identifiers but NATS
rejects in subjects (`*`, `>`, spaces) become underscores.

## ClickHouse mirror

```bash
CLICKHOUSE_URL="clickhouse://default@localhost:9000/default" ./gateway
```

Each source table becomes a `ReplacingMergeTree` in the `gtc` database,
created from the source catalog on first sight — no DDL to write:

```sql
CREATE TABLE gtc.orders (
    `id` Int32,
    `total` Nullable(Decimal(10, 2)),
    `notes` Nullable(String),
    `_version` UInt64,              -- the event's LSN
    `_deleted` UInt8 DEFAULT 0,     -- tombstone marker
    `_synced_at` DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(`_version`, `_deleted`)
ORDER BY (`id`)
```

Query current state with `FINAL`, which collapses row versions and hides
tombstones:

```sql
SELECT * FROM gtc.orders FINAL WHERE _deleted = 0;
```

Why it is built this way:

- **Versioned by LSN.** Replays are idempotent (a redelivered event has the
  same version), and a live change always beats a backfilled one for the same
  row, so backfill and streaming can interleave safely.
- **Deletes become tombstones** rather than vanishing, so a delete that
  arrives out of order cannot resurrect a row.
- **Unchanged TOAST columns are carried forward.** `ReplacingMergeTree`
  replaces whole rows, so an update that left a large column untouched would
  otherwise blank it; GTC reads the stored value back and rewrites it.
- **Batched inserts, without giving up the guarantee.** A change stream is
  exactly the many-small-inserts pattern ClickHouse dislikes, so GTC uses
  server-side async inserts *and waits for the flush*. Setting
  `CLICKHOUSE_WAIT_FOR_INSERT=false` trades that guarantee for throughput —
  the WAL position advances past rows still buffered in the server, and
  `gtc doctor` warns about it.
- **DDL follows automatically.** A column added upstream is added to the
  mirror (see [Schema changes](#schema-changes-ddl)). Dropped and retyped
  columns are logged but left alone: the mirror holds history the source no
  longer has, and discarding it silently would be worse than a warning.

Type mapping is exact where ClickHouse has an equivalent (`int4`→`Int32`,
`numeric(10,2)`→`Decimal(10, 2)`, `timestamptz`→`DateTime64(6, 'UTC')`,
`uuid`→`UUID`) and falls back to `String` where it does not — unconstrained
`numeric`, arrays, JSON, enums and user-defined types keep their exact
PostgreSQL text form for a query to cast.

**Tables need a primary key.** Without one there is no `ORDER BY`, so row
versions cannot be collapsed and deletes cannot address a row; such tables
are skipped with an error naming them, and the rest keep mirroring.

## Schema changes (DDL)

PostgreSQL's logical decoding does not stream DDL statements — there is no
`ALTER TABLE` event to subscribe to. What it *does* do is re-describe a
published table whenever its shape has changed, immediately before that
table's next row change. GTC diffs those descriptions and reports the
**effect** of the DDL:

| Detected | Kind | Breaking? |
|----------|------|-----------|
| New column | `column_added` | no — additive |
| Column removed | `column_dropped` | yes |
| Column type altered | `column_type_changed` | yes |
| Primary key / identity index replaced | `key_changed` | yes |
| `REPLICA IDENTITY` altered | `replica_identity_changed` | yes |
| Table renamed or moved schema | `table_renamed` | yes |

Every change is counted in `cdc_schema_changes_total{schema,table,kind}`,
logged (breaking ones at WARN, so existing log alerting picks them up), kept
in a rolling in-memory history behind `GET /api/schema`, and shown on the
dashboard's **Schema** page. With `REDIS_URL` set, it is also published to
the `<REDIS_STREAM_PREFIX>:schema` stream so consumers can react
programmatically:

```bash
redis-cli XRANGE cdc:schema - +
# fields: table, lsn, breaking, payload (full JSON diff)
```

Set `CDC_SCHEMA_EVENTS=false` to keep detection, metrics and the API while
skipping the Redis stream.

None of this needs DDL triggers, a schema-history topic, or superuser — it
falls out of the replication stream you are already reading. Two honest
limitations follow from that:

- **Detection is tied to the next row change.** A table altered at 02:00 and
  first written at 09:00 reports its change at 09:00. The LSN on the change
  is where GTC *detected* it, not where the DDL committed.
- **Several statements can collapse into one change.** If a column is added
  and another dropped between two row changes, they arrive as a single
  combined change — the net effect, not a statement log.

Unlike row events, schema notifications are **best-effort**: a change cannot
be re-derived on redelivery (a reconnect re-describes the table with nothing
to diff against), so a failed Redis publish is counted in
`cdc_schema_publish_errors_total` and logged rather than stalling the
pipeline. Row delivery keeps its at-least-once guarantee regardless.

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

### Dead-letter queue

A *poison event* — one that a sink permanently rejects (bad document schema,
oversized value) — would otherwise stall the pipeline forever. With Redis
configured, GTC parks it instead: after the same event fails the same sink
through `CDC_DLQ_THRESHOLD` (default 3) full retry-and-replay cycles, the
complete event is stored in Redis and the pipeline moves on.

The distinction that matters: **outages never divert to the DLQ.** A failure
caused by an open circuit breaker means the sink is down, so it stalls the
pipeline (as it should) instead of shoveling the whole stream into the queue.
And when the DLQ is full (`CDC_DLQ_MAX_ENTRIES`, default 10000) or Redis is
unreachable, parking fails and the pipeline stalls — the DLQ never drops an
event to save itself.

Triage over HTTP:

```bash
curl localhost:8080/dlq                                   # inspect (?limit=)
curl -X POST localhost:8080/dlq/retry   -d '{"id":"meilisearch:0/1A2B3C4"}'
curl -X POST localhost:8080/dlq/retry   -d '{"all":true}'
curl -X POST localhost:8080/dlq/discard -d '{"id":"meilisearch:0/1A2B3C4"}'
```

Retries re-deliver the stored (already-transformed) event straight to the
sink it failed. **Ordering caveat:** if newer changes for the same row have
flowed since the event was parked, retrying it re-applies the older state —
for a long-parked entry, prefer discarding it and re-syncing the table via
`POST /backfill`. Monitor `cdc_dlq_entries` and alert when it grows.

Set `CDC_DLQ_ENABLED=false` to opt out and always stall on failure.

## Observability

### Dashboard

Open `http://localhost:8080/` for the embedded dashboard: throughput with a
live sparkline, WAL lag, in-flight events, sink health with circuit-breaker
states, per-table operation counts, backfill progress (with a "sync all"
trigger), and dead-letter triage with retry/discard buttons. It's a React
SPA compiled into the binary — nothing extra to deploy, and it polls the
same public API you can script against.

Like `/metrics`, the dashboard is unauthenticated and intended for private
networks.

### Endpoints

| Endpoint | Purpose |
|----------|---------|
| `GET /` | Embedded dashboard |
| `GET /api/stats` | Dashboard's JSON snapshot (uptime, lag, sinks, tables, backfill, DLQ) |
| `GET /api/schema` | Recently detected schema (DDL) changes, newest first |
| `GET /health` | Liveness — process is up |
| `GET /readiness` | Readiness — live replication stream **and** all sinks healthy |
| `GET /metrics` | Prometheus metrics |
| `GET /backfill` | Backfill progress per table |
| `POST /backfill` | Trigger a backfill/replay (`{"table":"schema.table"}` or `{"all":true}`) |
| `GET /dlq` | List parked dead-letter entries (`?limit=`) |
| `POST /dlq/retry` | Retry an entry (`{"id":"..."}`) or all (`{"all":true}`) |
| `POST /dlq/discard` | Discard an entry (`{"id":"..."}`) |

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
      outbox/          # Transactional outbox publisher
  infrastructure/
    config/            # Env + sink YAML loading
    resilience/        # Retry + circuit breaker wrapper for sinks
    dlq/               # Dead-letter queue: parking decorator + triage API backend
ui/                    # React dashboard (built dist/ is committed and embedded)
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
