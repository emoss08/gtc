# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GTC is a PostgreSQL Change Data Capture (CDC) platform written in Go. It captures changes directly from PostgreSQL WAL and routes them to configurable sinks (Redis streams, Meilisearch). Built with hexagonal architecture and Uber FX for dependency injection.

## Build and Run Commands

```bash
go build -o gateway ./cmd/gateway
go run ./cmd/gateway
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| LOG_LEVEL | INFO | Log level: DEBUG, INFO, WARN, ERROR |
| DATABASE_URL | postgres://...?replication=database | PostgreSQL connection (must include ?replication=database) |
| CDC_SLOT_NAME | cdc_demo_slot | Replication slot name |
| CDC_PUBLICATION_NAME | cdc_demo_publication | Publication name |
| CDC_STANDBY_TIMEOUT | 10s | Standby message interval |
| CDC_PARALLEL_SINKS | false | Process sinks in parallel |
| CDC_PROCESS_TIMEOUT | 30s | Per-event processing timeout |
| CDC_EXCLUDED_TABLES | - | Comma-separated tables to exclude (e.g., public.migrations,audit_logs) |
| REDIS_URL | - | Redis connection (enables sink if set) |
| REDIS_STREAM_PREFIX | cdc | Stream key prefix |
| REDIS_MAX_STREAM_LEN | 10000 | Max stream length |
| MEILISEARCH_URL | - | Meilisearch URL (enables sink if set) |
| MEILISEARCH_API_KEY | - | Meilisearch API key |
| MEILISEARCH_TABLE_MAPPING | {} | JSON table-to-index mapping e.g. {"public.users":"users_idx"} |

## Architecture

Hexagonal architecture with Uber FX dependency injection:

```
cmd/gateway/           # FX bootstrap and module definitions
internal/
  core/
    domain/            # CDCEvent, Sink interface, domain errors
    ports/             # WALReader, CDCService, SinkRegistry interfaces
    services/          # CDCService and SinkRegistry implementations
  adapters/
    primary/wal/       # PostgreSQL WAL reader and decoder
    secondary/
      redis/           # Redis stream sink
      meilisearch/     # Meilisearch indexing sink
  infrastructure/
    config/            # Configuration loading from env vars
    resilience/        # Circuit breaker and retry logic
```

## Adding New Sinks

1. Create package under `internal/adapters/secondary/<sink_name>/`
2. Implement `domain.Sink` interface (Name, Initialize, Process, Shutdown, HealthCheck)
3. Add config loading in the package
4. Create FX module in `cmd/gateway/modules.go` with conditional loading
5. Register sink using `fx.Annotate` with `group:"sinks"` tag

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
