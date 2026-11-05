# GTC benchmark harness

End-to-end benchmarks for GTC: real PostgreSQL in, real sinks out, timed
from the gateway's own Prometheus counters.

## What it measures

| Mode | How |
|------|-----|
| **stream** | Creates the replication slot with an idle gateway, stops it, commits N rows (1,000 per transaction from four connections), then starts the gateway and times it from its first delivered event until **every sink has acknowledged all N events**. Writing the backlog first isolates GTC's throughput from PostgreSQL's insert speed. |
| **backfill** | Writes N rows *before* the slot exists, then starts the gateway with `CDC_BACKFILL_MODE=auto` and times the lock-free watermark backfill until every row has been delivered as a `READ` event. |
| **latency** | With the gateway streaming, four connections insert single-row transactions at a fixed rate (default 1,000 rows/s); each row carries its send time, and a consumer reads it back with `XREAD` from the Redis stream. The figure covers `INSERT` + `COMMIT` + decode + batching + `XADD` + `XREAD`. |

Rows are seven columns (bigserial key, integer, two text columns, numeric,
a small jsonb object, timestamptz), roughly 200 bytes each. Every gateway
setting is left at its default (`CDC_BATCH_MAX_EVENTS=5000`,
`CDC_BATCH_LINGER=10ms`, DLQ on, retries and circuit breaker on) except
`LOG_LEVEL=ERROR`.

## Running it

You need PostgreSQL 14+ with `wal_level=logical` and a role that may create
replication slots and publications. Redis is optional (it enables the Redis
Streams, backfill, and latency scenarios). NATS JetStream runs in-process,
and the webhook receiver is built in, so those need nothing extra.

```bash
go build -o gateway ./cmd/gateway
go run ./test/bench -gateway ./gateway \
  -db postgres://postgres:postgres@127.0.0.1:5432/postgres \
  -redis redis://127.0.0.1:6379
```

Add `-redis-json redis://...` (Redis Stack) or `-meili http://... -meili-key ...`
to include those sinks. `-only redis-stream,latency` runs a subset, and
`-out results.md` saves the table.

The harness creates and drops its own slot (`gtc_bench_slot`), publication
(`gtc_bench_pub`), and tables (`bench_events`, `bench_latency`). In Redis it
only touches keys under `gtcbench:*`. Point it at a scratch database, not
production: the publication is `FOR ALL TABLES`.

| Flag | Default | |
|------|---------|--|
| `-events` | `1000000` | Rows per stream/backfill scenario |
| `-webhook-events` | `100000` | Rows for the webhook scenario (one POST per event) |
| `-meili-events` | `200000` | Rows for the Meilisearch scenario |
| `-txn-size` | `1000` | Rows per source transaction |
| `-latency-rate` | `1000` | Rows/s during the latency test |
| `-latency-duration` | `20s` | Length of the latency test |
| `-nats` | `true` | Run the in-process NATS JetStream scenarios |
| `-backfill` / `-latency` | `true` | Include those scenarios |
| `-port` | `18080` | HTTP port of the gateway under test |
| `-env` | – | Extra `KEY=VALUE` for the gateway, repeatable (e.g. `-env CDC_BATCH_LINGER=50ms`) |

## Reading the results

- Everything — PostgreSQL, Redis, NATS, the webhook receiver, the gateway,
  and the harness — shares one machine in the published numbers, so they
  are a floor, not a ceiling: on separate hosts the sinks stop competing
  with GTC for CPU.
- "Avg sink write per batch" is the mean time one sink took to apply one
  batch (from `cdc_sink_batch_duration_seconds`).
- The webhook sink posts one event at a time by design (each event is its
  own retry and dead-letter unit), so its throughput is bounded by the
  receiver's round-trip time.
- The latency p50 includes up to `CDC_BATCH_LINGER` (10 ms) of batching
  when the stream is quiet; lower it for latency, raise it for throughput.
