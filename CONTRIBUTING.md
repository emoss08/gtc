# Contributing to GTC

Thanks for your interest in improving GTC! Bug reports, documentation fixes,
new sinks, and performance work are all welcome.

## Ground rules

- Be respectful — this project follows the
  [Code of Conduct](CODE_OF_CONDUCT.md).
- **Never report security vulnerabilities in public issues** — see
  [SECURITY.md](SECURITY.md).
- Open an issue before starting large changes (new sinks, architectural
  changes) so the approach can be discussed first. Small fixes can go
  straight to a pull request.

## Development setup

You need Go 1.25+ and Docker (for the local stack).

```bash
git clone https://github.com/emoss08/gtc.git
cd gtc

# Start PostgreSQL (wal_level=logical) + Redis Stack
docker compose -f docker-compose-local.yml up -d postgres redis

# Build and run
go build ./...
DATABASE_URL="postgres://postgres:postgres@localhost:5432/postgres?replication=database" \
REDIS_URL="redis://localhost:6379" \
go run ./cmd/gateway
```

Generate some traffic and watch it flow:

```bash
docker compose -f docker-compose-local.yml exec postgres \
  psql -U postgres -c "CREATE TABLE t (id serial PRIMARY KEY, v text);
                       INSERT INTO t (v) VALUES ('hello');"
docker compose -f docker-compose-local.yml exec redis \
  redis-cli XRANGE cdc:public:t - +
```

## Working on the dashboard

The dashboard is a React 19 + TypeScript + Tailwind SPA in `ui/` (shadcn/ui,
TanStack Table/Query, react-hook-form + zod), embedded into the
binary PocketBase-style: the built `ui/dist/` is **committed**, so plain
`go build` needs no Node toolchain.

```bash
cd ui
npm ci
npm run dev     # dev server; proxies /api, /dlq, /backfill to :8080
npm run build   # rebuild dist/ — commit the result with your change
```

If you change anything under `ui/src`, rebuild and commit `ui/dist` in the
same PR.

## Before you submit

```bash
gofmt -l ./cmd ./internal   # must print nothing
go vet ./...
go test ./...
```

- Follow the [Uber Go Style Guide](https://github.com/uber-go/guide/blob/master/style.md).
- Add or update tests for behavior you change. Correctness-critical areas
  (the pgoutput decoder, LSN acknowledgment, TOAST handling, key templates)
  must not lose coverage.
- Keep documentation in sync: `README.md` and `CLAUDE.md` both list the
  environment variables — update both if you add or change one.

## Commit messages

Use a short type prefix in the subject, present tense:

```
fix: do not acknowledge keepalive LSNs inside a transaction
change: add ClickHouse sink
chore: bump deps
docs: document replica identity requirements
```

(You'll see emoji prefixes like `🟡(change):` in the history — those are
fine too; the type is what matters.)

## Pull requests

1. Fork and create a feature branch from `main`.
2. Keep PRs focused — one logical change per PR.
3. Describe **what** changed and **why**; call out any behavior change,
   especially anything touching delivery semantics.
4. Make sure `go test ./...` passes and `gofmt` is clean.

## Adding a new sink

Sinks are small and self-contained; the whole surface is the five-method
`domain.Sink` interface:

1. Create a package under `internal/adapters/secondary/<sink_name>/`.
2. Implement `domain.Sink` — `Name`, `Initialize`, `Process`, `Shutdown`,
   `HealthCheck`. `Process` receives one `domain.CDCEvent` at a time.
3. Add config loading in the package: the sink is enabled when its URL env
   var is set (see `redis/config.go` for the pattern).
4. Register it in `newSinks` in `cmd/gateway/modules.go`, wrapped with
   `resilience.NewResilientSink`.
5. Document its env vars in `README.md` and `CLAUDE.md`.

Sink implementation rules (these are the contract that keeps GTC correct):

- **Idempotency is mandatory.** Delivery is at-least-once; your sink will
  receive duplicate events after reconnects and must converge to the same
  state.
- **Respect `UnchangedToastColumns`.** Columns listed there are *absent*
  from `NewData` because their TOASTed value didn't change. If your sink
  replaces whole documents, you must merge instead (see the RedisJSON
  `JSON.MERGE` and Meilisearch `UpdateDocuments` handling for examples) —
  otherwise you will erase real data.
- **Honor the context.** `Process` runs synchronously with the WAL stream
  under `CDC_PROCESS_TIMEOUT`; use context-aware client calls so a hung
  backend can't stall replication past `wal_sender_timeout`.
- **Return errors honestly.** A returned error is what prevents the WAL
  position from being confirmed — swallowing one converts "retry later"
  into permanent data loss. If your backend acknowledges writes
  asynchronously, wait for the result (see the Meilisearch sink's task
  waiting).

## Reporting bugs

Use the bug report issue template. For pipeline issues, the most useful
things to include are:

- GTC logs around the failure (`LOG_LEVEL=DEBUG` if possible)
- PostgreSQL version and the output of
  `SELECT * FROM pg_replication_slots;`
- Your sink configuration (redact credentials)
