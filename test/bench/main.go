// Command bench measures GTC end to end: PostgreSQL WAL in, sink writes out.
//
// It drives a real gateway binary against a real PostgreSQL (and whichever
// sinks you point it at) and reports three things:
//
//   - stream: how fast GTC drains a backlog of committed changes. The WAL is
//     written while the gateway is stopped, then the gateway is started and
//     timed from its first delivered event until every sink has acknowledged
//     every event. This isolates GTC's throughput from PostgreSQL's write
//     speed.
//   - backfill: how fast an existing table is copied to the sinks as READ
//     events (watermark chunking, no locks).
//   - latency: commit-to-consumer latency at a steady write rate, read back
//     from the Redis stream.
//
// Every number comes from the gateway's own Prometheus counters, so the
// harness measures exactly what the pipeline confirms, not what it was sent.
//
// Usage (see README.md in this directory):
//
//	go build -o gateway ./cmd/gateway
//	go run ./test/bench -gateway ./gateway -redis redis://127.0.0.1:6379
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/redis/go-redis/v9"
)

const (
	slotName    = "gtc_bench_slot"
	pubName     = "gtc_bench_pub"
	keyPrefix   = "gtcbench"
	eventsTable = "bench_events"
	latTable    = "bench_latency"
	meiliIndex  = "gtcbench_events"
)

type options struct {
	gateway       string
	dbURL         string
	redisURL      string
	redisJSONURL  string
	meiliURL      string
	meiliKey      string
	nats          bool
	events        int
	webhookEvents int
	meiliEvents   int
	txnSize       int
	backfill      bool
	latency       bool
	latencyRate   int
	latencyDur    time.Duration
	only          string
	httpPort      int
	out           string
	gatewayEnv    envList
}

// envList collects repeated -env KEY=VALUE flags.
type envList []string

func (e *envList) String() string { return strings.Join(*e, ",") }

func (e *envList) Set(v string) error {
	if !strings.Contains(v, "=") {
		return fmt.Errorf("want KEY=VALUE, got %q", v)
	}
	*e = append(*e, v)
	return nil
}

func main() {
	log.SetFlags(0)
	var o options
	flag.StringVar(&o.gateway, "gateway", "./gateway", "path to a built gateway binary")
	flag.StringVar(&o.dbURL, "db", "postgres://postgres:postgres@127.0.0.1:5432/postgres", "PostgreSQL URL (wal_level=logical; no replication parameter)")
	flag.StringVar(&o.redisURL, "redis", "", "Redis URL: enables the Redis Streams scenarios and the latency test")
	flag.StringVar(&o.redisJSONURL, "redis-json", "", "Redis Stack URL: enables the RedisJSON scenario")
	flag.StringVar(&o.meiliURL, "meili", "", "Meilisearch URL: enables the Meilisearch scenario")
	flag.StringVar(&o.meiliKey, "meili-key", "", "Meilisearch API key")
	flag.BoolVar(&o.nats, "nats", true, "run the NATS JetStream scenarios against an in-process server")
	flag.IntVar(&o.events, "events", 1_000_000, "rows written per stream scenario")
	flag.IntVar(&o.webhookEvents, "webhook-events", 100_000, "rows for the webhook scenario (one POST per event)")
	flag.IntVar(&o.meiliEvents, "meili-events", 200_000, "rows for the Meilisearch scenario")
	flag.IntVar(&o.txnSize, "txn-size", 1000, "rows per source transaction")
	flag.BoolVar(&o.backfill, "backfill", true, "run the backfill scenarios")
	flag.BoolVar(&o.latency, "latency", true, "run the latency test (needs -redis)")
	flag.IntVar(&o.latencyRate, "latency-rate", 1000, "rows per second during the latency test")
	flag.DurationVar(&o.latencyDur, "latency-duration", 20*time.Second, "length of the latency test")
	flag.StringVar(&o.only, "only", "", "comma-separated scenario names to run (default: all available)")
	flag.IntVar(&o.httpPort, "port", 18080, "HTTP port for the gateway under test")
	flag.StringVar(&o.out, "out", "", "also write the results table (markdown) to this file")
	flag.Var(&o.gatewayEnv, "env", "extra KEY=VALUE for the gateway under test, e.g. -env CDC_BATCH_LINGER=50ms (repeatable)")
	flag.Parse()

	if err := run(o); err != nil {
		log.Fatalf("bench: %v", err)
	}
}

// scenario is one gateway configuration under one workload.
type scenario struct {
	name   string
	mode   string // stream | backfill
	events int
	sinks  []string // sink names as reported in cdc_events_processed_total
	env    func(*env) []string
	setup  func(context.Context) (cleanup func(), err error)
}

// env carries per-run endpoints that only exist once a scenario starts.
type env struct {
	natsURL    string
	webhookURL string
}

type result struct {
	scenario   string
	mode       string
	events     int
	elapsed    time.Duration
	batchMean  float64
	sinkMeanMs map[string]float64
}

func run(o options) error {
	ctx := context.Background()

	if _, err := os.Stat(o.gateway); err != nil {
		return fmt.Errorf("gateway binary %q: %w (build it with: go build -o gateway ./cmd/gateway)", o.gateway, err)
	}
	pg, err := pgx.Connect(ctx, o.dbURL)
	if err != nil {
		return fmt.Errorf("connect PostgreSQL: %w", err)
	}
	defer pg.Close(ctx)

	var walLevel, pgVersion string
	if err := pg.QueryRow(ctx, "SHOW wal_level").Scan(&walLevel); err != nil {
		return err
	}
	if walLevel != "logical" {
		return fmt.Errorf("wal_level is %q; set wal_level=logical and restart PostgreSQL", walLevel)
	}
	_ = pg.QueryRow(ctx, "SHOW server_version").Scan(&pgVersion)

	var rdb *redis.Client
	if o.redisURL != "" {
		opt, err := redis.ParseURL(o.redisURL)
		if err != nil {
			return fmt.Errorf("parse -redis: %w", err)
		}
		rdb = redis.NewClient(opt)
		defer rdb.Close()
		if err := rdb.Ping(ctx).Err(); err != nil {
			return fmt.Errorf("ping Redis: %w", err)
		}
	}

	wh := newWebhookReceiver()
	defer wh.close()

	b := &bench{o: o, pg: pg, rdb: rdb}
	scenarios := b.scenarios(wh)
	if len(scenarios) == 0 {
		return errors.New("no scenarios selected")
	}

	log.Printf("GTC benchmark — PostgreSQL %s, %d CPUs, %s/%s", pgVersion, runtime.NumCPU(), runtime.GOOS, runtime.GOARCH)

	var results []result
	for _, sc := range scenarios {
		log.Printf("\n▶ %s (%s, %d events)", sc.name, sc.mode, sc.events)
		r, err := b.runScenario(ctx, sc, wh)
		if err != nil {
			return fmt.Errorf("%s: %w", sc.name, err)
		}
		log.Printf("  %d events in %s → %s events/s", r.events, r.elapsed.Round(time.Millisecond), commas(rate(r)))
		results = append(results, r)
	}

	var lat *latencyResult
	if o.latency && rdb != nil && selected(o.only, "latency") {
		log.Printf("\n▶ latency (Redis Streams, %d rows/s for %s)", o.latencyRate, o.latencyDur)
		lat, err = b.runLatency(ctx)
		if err != nil {
			return fmt.Errorf("latency: %w", err)
		}
	}

	report := renderReport(results, lat)
	fmt.Println()
	fmt.Print(report)
	if o.out != "" {
		if err := os.WriteFile(o.out, []byte(report), 0o644); err != nil {
			return err
		}
	}
	return b.dropSlot(ctx)
}

type bench struct {
	o   options
	pg  *pgx.Conn
	rdb *redis.Client
}

func (b *bench) scenarios(wh *webhookReceiver) []scenario {
	o := b.o
	redisEnv := func(*env) []string {
		return []string{"REDIS_URL=" + o.redisURL}
	}
	natsEnv := func(e *env) []string {
		return []string{"NATS_URL=" + e.natsURL}
	}
	webhookEnv := func(e *env) []string {
		return []string{"WEBHOOK_URL=" + e.webhookURL, "WEBHOOK_SIGNING_SECRET=bench-secret"}
	}
	join := func(fns ...func(*env) []string) func(*env) []string {
		return func(e *env) []string {
			var out []string
			for _, fn := range fns {
				out = append(out, fn(e)...)
			}
			return out
		}
	}

	var all []scenario
	add := func(sc scenario) {
		if selected(o.only, sc.name) {
			all = append(all, sc)
		}
	}

	add(scenario{name: "no-sinks", mode: "stream", events: o.events,
		env: func(*env) []string { return nil }})
	if o.redisURL != "" {
		add(scenario{name: "redis-stream", mode: "stream", events: o.events,
			sinks: []string{"redis-stream"}, env: redisEnv})
	}
	if o.nats {
		add(scenario{name: "nats-jetstream", mode: "stream", events: o.events,
			sinks: []string{"nats"}, env: natsEnv})
	}
	if o.redisURL != "" && o.nats {
		add(scenario{name: "redis-stream+nats", mode: "stream", events: o.events,
			sinks: []string{"redis-stream", "nats"}, env: join(redisEnv, natsEnv)})
	}
	if o.redisJSONURL != "" {
		add(scenario{name: "redis-json", mode: "stream", events: o.events,
			sinks: []string{"redis-json"},
			env:   func(*env) []string { return []string{"REDIS_JSON_URL=" + o.redisJSONURL} }})
	}
	if o.meiliURL != "" {
		add(scenario{name: "meilisearch", mode: "stream", events: o.meiliEvents,
			sinks: []string{"meilisearch"},
			env: func(*env) []string {
				return []string{"MEILISEARCH_URL=" + o.meiliURL, "MEILISEARCH_API_KEY=" + o.meiliKey}
			},
			setup: func(ctx context.Context) (func(), error) { return func() {}, b.deleteMeiliIndex(ctx) }})
	}
	add(scenario{name: "webhook", mode: "stream", events: o.webhookEvents,
		sinks: []string{"webhook"}, env: webhookEnv})

	if o.backfill {
		// Backfill only reads tables some sink consumes, so there is no
		// sink-less variant.
		if o.redisURL != "" {
			add(scenario{name: "backfill-redis-stream", mode: "backfill", events: o.events,
				sinks: []string{"redis-stream"}, env: redisEnv})
		}
		if o.nats {
			add(scenario{name: "backfill-nats-jetstream", mode: "backfill", events: o.events,
				sinks: []string{"nats"}, env: natsEnv})
		}
	}
	return all
}

func selected(only, name string) bool {
	if only == "" {
		return true
	}
	for _, s := range strings.Split(only, ",") {
		if strings.TrimSpace(s) == name {
			return true
		}
	}
	return false
}

func (b *bench) runScenario(ctx context.Context, sc scenario, wh *webhookReceiver) (result, error) {
	e := &env{webhookURL: wh.url}
	if needsNATS(sc) {
		srv, url, err := startNATS()
		if err != nil {
			return result{}, err
		}
		defer srv.Shutdown()
		e.natsURL = url
	}
	if sc.setup != nil {
		cleanup, err := sc.setup(ctx)
		if err != nil {
			return result{}, err
		}
		defer cleanup()
	}
	if err := b.cleanRedis(ctx); err != nil {
		return result{}, err
	}

	if err := b.dropSlot(ctx); err != nil {
		return result{}, err
	}
	if err := b.recreateTable(ctx, eventsTable); err != nil {
		return result{}, err
	}
	// Leave only this scenario's table in the publication.
	if _, err := b.pg.Exec(ctx, "DROP TABLE IF EXISTS "+latTable); err != nil {
		return result{}, err
	}

	cfgFile, err := writeSinkConfig()
	if err != nil {
		return result{}, err
	}
	defer os.Remove(cfgFile)
	gwEnv := append(sc.env(e), "SINK_CONFIG_FILE="+cfgFile)

	switch sc.mode {
	case "stream":
		// Create the slot with an idle gateway, stop it, then commit the
		// workload: the next start finds the whole backlog waiting.
		gw, err := b.startGateway(ctx, append(gwEnv, "CDC_BACKFILL_MODE=off"))
		if err != nil {
			return result{}, err
		}
		if err := gw.stop(); err != nil {
			return result{}, err
		}
		if err := b.writeRows(ctx, eventsTable, sc.events); err != nil {
			return result{}, err
		}
		return b.measure(ctx, sc, append(gwEnv, "CDC_BACKFILL_MODE=off"), "")

	case "backfill":
		// Rows exist before the slot does, so auto mode copies them all.
		if err := b.writeRows(ctx, eventsTable, sc.events); err != nil {
			return result{}, err
		}
		if _, err := b.pg.Exec(ctx, "VACUUM ANALYZE "+eventsTable); err != nil {
			return result{}, err
		}
		return b.measure(ctx, sc, append(gwEnv, "CDC_BACKFILL_MODE=auto"), "READ")
	}
	return result{}, fmt.Errorf("unknown mode %q", sc.mode)
}

func needsNATS(sc scenario) bool {
	for _, s := range sc.sinks {
		if s == "nats" {
			return true
		}
	}
	return false
}

// measure starts the gateway and times it from its first delivered event
// until every sink has acknowledged sc.events events (counted from the
// gateway's metrics; a fresh process starts every counter at zero).
func (b *bench) measure(ctx context.Context, sc scenario, gwEnv []string, op string) (result, error) {
	gw, err := b.startGatewayNoWait(gwEnv)
	if err != nil {
		return result{}, err
	}
	defer gw.stop()

	metricsURL := fmt.Sprintf("http://127.0.0.1:%d/metrics", b.o.httpPort)
	client := &http.Client{Timeout: 5 * time.Second}
	deadline := time.Now().Add(30 * time.Minute)
	var first time.Time
	var last time.Time
	lastProgress := time.Now()
	var lastSeen float64

	for {
		if time.Now().After(deadline) {
			return result{}, errors.New("timed out waiting for delivery")
		}
		if gw.exited() {
			return result{}, fmt.Errorf("gateway exited early:\n%s", gw.tail())
		}
		m, err := scrape(client, metricsURL)
		now := time.Now()
		if err != nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		received := m.sum("cdc_events_received_total", map[string]string{"table": eventsTable, "operation": op})
		if received > 0 && first.IsZero() {
			first = now
		}
		done := received >= float64(sc.events)
		for _, s := range sc.sinks {
			if m.sum("cdc_events_processed_total", map[string]string{"sink": s, "status": "success"}) < float64(sc.events) {
				done = false
			}
		}
		if done {
			last = now
			res := result{
				scenario:   sc.name,
				mode:       sc.mode,
				events:     sc.events,
				elapsed:    last.Sub(first),
				batchMean:  m.mean("cdc_batch_events", nil),
				sinkMeanMs: map[string]float64{},
			}
			for _, s := range sc.sinks {
				res.sinkMeanMs[s] = 1000 * m.mean("cdc_sink_batch_duration_seconds", map[string]string{"sink": s})
			}
			return res, nil
		}
		if received != lastSeen {
			lastSeen, lastProgress = received, now
		} else if !first.IsZero() && now.Sub(lastProgress) > 2*time.Minute {
			return result{}, fmt.Errorf("no progress for 2m at %.0f/%d events:\n%s", received, sc.events, gw.tail())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// --- latency ---

type latencyResult struct {
	rate                     int
	samples                  int
	p50, p90, p99, p999, max time.Duration
}

// runLatency writes single-row transactions at a fixed rate while the
// gateway streams, and reads them back from the Redis stream. Each row
// carries the client's send time, so the latency covers INSERT + COMMIT +
// decode + batch + XADD + XREAD.
func (b *bench) runLatency(ctx context.Context) (*latencyResult, error) {
	if err := b.cleanRedis(ctx); err != nil {
		return nil, err
	}
	if err := b.dropSlot(ctx); err != nil {
		return nil, err
	}
	if _, err := b.pg.Exec(ctx, "DROP TABLE IF EXISTS "+latTable+", "+eventsTable); err != nil {
		return nil, err
	}
	if _, err := b.pg.Exec(ctx, "CREATE TABLE "+latTable+" (id bigserial PRIMARY KEY, sent_us bigint NOT NULL, note text)"); err != nil {
		return nil, err
	}
	cfgFile, err := writeSinkConfig()
	if err != nil {
		return nil, err
	}
	defer os.Remove(cfgFile)

	gw, err := b.startGateway(ctx, []string{
		"REDIS_URL=" + b.o.redisURL, "SINK_CONFIG_FILE=" + cfgFile, "CDC_BACKFILL_MODE=off",
		"REDIS_MAX_STREAM_LEN=1000000",
	})
	if err != nil {
		return nil, err
	}
	defer gw.stop()

	stream := keyPrefix + ":public:" + latTable
	total := int(float64(b.o.latencyRate) * b.o.latencyDur.Seconds())

	var mu sync.Mutex
	lat := make([]time.Duration, 0, total)
	readCtx, cancelRead := context.WithCancel(ctx)
	defer cancelRead()
	readDone := make(chan error, 1)
	go func() {
		readDone <- func() error {
			last := "$"
			for {
				res, err := b.rdb.XRead(readCtx, &redis.XReadArgs{Streams: []string{stream, last}, Block: 500 * time.Millisecond, Count: 1000}).Result()
				recv := time.Now()
				if errors.Is(err, redis.Nil) {
					continue
				}
				if err != nil {
					if readCtx.Err() != nil {
						return nil
					}
					return err
				}
				for _, s := range res {
					for _, msg := range s.Messages {
						last = msg.ID
						sent, err := sentMicros(msg.Values["payload"])
						if err != nil {
							return err
						}
						mu.Lock()
						lat = append(lat, recv.Sub(time.UnixMicro(sent)))
						n := len(lat)
						mu.Unlock()
						if n >= total {
							return nil
						}
					}
				}
			}
		}()
	}()
	// XREAD with "$" only sees entries added after it starts blocking.
	time.Sleep(300 * time.Millisecond)

	const writers = 4
	perWriter := b.o.latencyRate / writers
	var wg sync.WaitGroup
	var writeErr atomic.Value
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := pgx.Connect(ctx, b.o.dbURL)
			if err != nil {
				writeErr.Store(err)
				return
			}
			defer conn.Close(ctx)
			interval := time.Second / time.Duration(perWriter)
			next := time.Now()
			for i := 0; i < total/writers; i++ {
				if d := time.Until(next); d > 0 {
					time.Sleep(d)
				}
				next = next.Add(interval)
				if _, err := conn.Exec(ctx, "INSERT INTO "+latTable+" (sent_us, note) VALUES ($1, 'latency probe')", time.Now().UnixMicro()); err != nil {
					writeErr.Store(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if v := writeErr.Load(); v != nil {
		return nil, v.(error)
	}

	select {
	case err := <-readDone:
		if err != nil {
			return nil, err
		}
	case <-time.After(30 * time.Second):
		cancelRead()
		<-readDone
	}

	mu.Lock()
	defer mu.Unlock()
	if len(lat) == 0 {
		return nil, errors.New("no events read back from the Redis stream")
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(p float64) time.Duration { return lat[int(p*float64(len(lat)-1))] }
	return &latencyResult{
		rate: b.o.latencyRate, samples: len(lat),
		p50: pct(0.50), p90: pct(0.90), p99: pct(0.99), p999: pct(0.999), max: lat[len(lat)-1],
	}, nil
}

func sentMicros(payload any) (int64, error) {
	s, ok := payload.(string)
	if !ok {
		return 0, fmt.Errorf("unexpected payload type %T", payload)
	}
	var ev struct {
		NewData map[string]any `json:"new_data"`
	}
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	if err := dec.Decode(&ev); err != nil {
		return 0, err
	}
	n, ok := ev.NewData["sent_us"].(json.Number)
	if !ok {
		return 0, fmt.Errorf("payload has no numeric new_data.sent_us: %s", s)
	}
	return n.Int64()
}

// --- PostgreSQL ---

func (b *bench) recreateTable(ctx context.Context, table string) error {
	_, err := b.pg.Exec(ctx, fmt.Sprintf(`
		DROP TABLE IF EXISTS %[1]s;
		CREATE TABLE %[1]s (
			id          bigserial PRIMARY KEY,
			customer_id integer      NOT NULL,
			email       text         NOT NULL,
			status      text         NOT NULL,
			amount      numeric(10,2) NOT NULL,
			attributes  jsonb        NOT NULL,
			created_at  timestamptz  NOT NULL DEFAULT now()
		)`, table))
	return err
}

// writeRows inserts n realistic rows (seven columns, ~200 bytes of row data)
// in transactions of txnSize rows from four connections.
func (b *bench) writeRows(ctx context.Context, table string, n int) error {
	start := time.Now()
	const writers = 4
	txns := (n + b.o.txnSize - 1) / b.o.txnSize
	var next atomic.Int64
	errs := make(chan error, writers)
	for w := 0; w < writers; w++ {
		go func() {
			errs <- func() error {
				conn, err := pgx.Connect(ctx, b.o.dbURL)
				if err != nil {
					return err
				}
				defer conn.Close(ctx)
				for {
					i := int(next.Add(1)) - 1
					if i >= txns {
						return nil
					}
					rows := b.o.txnSize
					if rem := n - i*b.o.txnSize; rem < rows {
						rows = rem
					}
					if _, err := conn.Exec(ctx, fmt.Sprintf(`
						INSERT INTO %s (customer_id, email, status, amount, attributes)
						SELECT (random() * 100000)::int,
						       'customer' || g || '@example.com',
						       (ARRAY['pending','paid','shipped','refunded'])[1 + (g %% 4)],
						       round((random() * 1000)::numeric, 2),
						       jsonb_build_object('source', 'bench', 'priority', g %% 5, 'tags', jsonb_build_array('a', 'b'))
						FROM generate_series(1, $1) AS g`, table), rows); err != nil {
						return err
					}
				}
			}()
		}()
	}
	for w := 0; w < writers; w++ {
		if err := <-errs; err != nil {
			return fmt.Errorf("write rows: %w", err)
		}
	}
	log.Printf("  wrote %s rows in %s", commas(float64(n)), time.Since(start).Round(time.Millisecond))
	return nil
}

func (b *bench) dropSlot(ctx context.Context) error {
	// A gateway that just exited may still hold the slot for a moment.
	for i := 0; i < 50; i++ {
		var active bool
		err := b.pg.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name = $1", slotName).Scan(&active)
		if errors.Is(err, pgx.ErrNoRows) {
			break
		}
		if err != nil {
			return err
		}
		if !active {
			if _, err := b.pg.Exec(ctx, "SELECT pg_drop_replication_slot($1)", slotName); err != nil {
				return err
			}
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	_, err := b.pg.Exec(ctx, "DROP PUBLICATION IF EXISTS "+pubName)
	if err != nil {
		return err
	}
	_, err = b.pg.Exec(ctx, "DROP TABLE IF EXISTS gtc_backfill_state")
	return err
}

// --- gateway process ---

type gateway struct {
	cmd    *exec.Cmd
	done   chan struct{}
	output *tailBuffer
}

func (b *bench) gatewayEnv(extra []string) []string {
	u := b.o.dbURL
	if strings.Contains(u, "?") {
		u += "&replication=database"
	} else {
		u += "?replication=database"
	}
	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"DATABASE_URL=" + u,
		"HTTP_PORT=" + strconv.Itoa(b.o.httpPort),
		"LOG_LEVEL=ERROR",
		"CDC_SLOT_NAME=" + slotName,
		"CDC_PUBLICATION_NAME=" + pubName,
		"REDIS_STREAM_PREFIX=" + keyPrefix,
		"REDIS_JSON_PREFIX=" + keyPrefix,
		"NATS_SUBJECT_PREFIX=" + keyPrefix,
		"NATS_STREAM=GTCBENCH",
	}
	env = append(env, extra...)
	return append(env, b.o.gatewayEnv...)
}

func (b *bench) startGatewayNoWait(extra []string) (*gateway, error) {
	cmd := exec.Command(b.o.gateway)
	cmd.Env = b.gatewayEnv(extra)
	out := &tailBuffer{max: 8 << 10}
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	g := &gateway{cmd: cmd, done: make(chan struct{}), output: out}
	go func() { _ = cmd.Wait(); close(g.done) }()
	return g, nil
}

// startGateway starts the gateway and waits until it is streaming.
func (b *bench) startGateway(ctx context.Context, extra []string) (*gateway, error) {
	g, err := b.startGatewayNoWait(extra)
	if err != nil {
		return nil, err
	}
	url := fmt.Sprintf("http://127.0.0.1:%d/readiness", b.o.httpPort)
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if g.exited() {
			return nil, fmt.Errorf("gateway exited during startup:\n%s", g.tail())
		}
		resp, err := http.Get(url)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return g, nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	_ = g.stop()
	return nil, fmt.Errorf("gateway not ready after 60s:\n%s", g.tail())
}

func (g *gateway) exited() bool {
	select {
	case <-g.done:
		return true
	default:
		return false
	}
}

func (g *gateway) stop() error {
	if g.exited() {
		return nil
	}
	if err := g.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		_ = g.cmd.Process.Kill()
	}
	select {
	case <-g.done:
		return nil
	case <-time.After(30 * time.Second):
		_ = g.cmd.Process.Kill()
		<-g.done
		return errors.New("gateway did not stop within 30s")
	}
}

func (g *gateway) tail() string { return g.output.String() }

type tailBuffer struct {
	mu  sync.Mutex
	buf []byte
	max int
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = append(t.buf, p...)
	if len(t.buf) > t.max {
		t.buf = t.buf[len(t.buf)-t.max:]
	}
	return len(p), nil
}

func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return string(t.buf)
}

func writeSinkConfig() (string, error) {
	f, err := os.CreateTemp("", "gtc-bench-sinks-*.yaml")
	if err != nil {
		return "", err
	}
	defer f.Close()
	_, err = fmt.Fprintf(f, `redis_stream:
  sync_all: true
redis_json:
  sync_all: true
nats:
  sync_all: true
webhook:
  sync_all: true
meilisearch:
  tables:
    public.%s:
      index: %s
      primary_key: id
`, eventsTable, meiliIndex)
	return f.Name(), err
}

// --- sinks under test ---

func startNATS() (*natsserver.Server, string, error) {
	dir, err := os.MkdirTemp("", "gtc-bench-nats-*")
	if err != nil {
		return nil, "", err
	}
	srv, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: filepath.Clean(dir),
		NoLog: true, NoSigs: true,
	})
	if err != nil {
		return nil, "", err
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		return nil, "", errors.New("NATS server did not start")
	}
	return srv, srv.ClientURL(), nil
}

// webhookReceiver is a minimal endpoint that acknowledges every POST.
type webhookReceiver struct {
	srv *http.Server
	url string
}

func newWebhookReceiver() *webhookReceiver {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("webhook receiver: %v", err)
	}
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusNoContent)
		}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() { _ = srv.Serve(ln) }()
	return &webhookReceiver{srv: srv, url: "http://" + ln.Addr().String() + "/hook"}
}

func (w *webhookReceiver) close() { _ = w.srv.Close() }

// cleanRedis removes only the keys this harness creates.
func (b *bench) cleanRedis(ctx context.Context) error {
	for _, raw := range []string{b.o.redisURL, b.o.redisJSONURL} {
		if raw == "" {
			continue
		}
		opt, err := redis.ParseURL(raw)
		if err != nil {
			return err
		}
		c := redis.NewClient(opt)
		iter := c.Scan(ctx, 0, keyPrefix+":*", 1000).Iterator()
		var keys []string
		for iter.Next(ctx) {
			keys = append(keys, iter.Val())
			if len(keys) == 1000 {
				if err := c.Unlink(ctx, keys...).Err(); err != nil {
					c.Close()
					return err
				}
				keys = keys[:0]
			}
		}
		if err := iter.Err(); err != nil {
			c.Close()
			return err
		}
		if len(keys) > 0 {
			if err := c.Unlink(ctx, keys...).Err(); err != nil {
				c.Close()
				return err
			}
		}
		c.Close()
	}
	return nil
}

func (b *bench) deleteMeiliIndex(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		strings.TrimRight(b.o.meiliURL, "/")+"/indexes/"+meiliIndex, nil)
	if err != nil {
		return err
	}
	if b.o.meiliKey != "" {
		req.Header.Set("Authorization", "Bearer "+b.o.meiliKey)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("delete Meilisearch index: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("delete Meilisearch index: %s", resp.Status)
	}
	// Deletion is asynchronous; give the task a moment before the sink
	// recreates the index.
	time.Sleep(2 * time.Second)
	return nil
}

// --- Prometheus text parsing ---

type sample struct {
	name   string
	labels map[string]string
	value  float64
}

type metricSet []sample

func scrape(c *http.Client, url string) (metricSet, error) {
	resp, err := c.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var out metricSet
	for _, line := range strings.Split(string(body), "\n") {
		if line == "" || line[0] == '#' || !strings.HasPrefix(line, "cdc_") {
			continue
		}
		sp := strings.LastIndexByte(line, ' ')
		if sp < 0 {
			continue
		}
		v, err := strconv.ParseFloat(line[sp+1:], 64)
		if err != nil {
			continue
		}
		head := line[:sp]
		s := sample{labels: map[string]string{}, value: v}
		if i := strings.IndexByte(head, '{'); i >= 0 {
			s.name = head[:i]
			for _, kv := range strings.Split(strings.TrimSuffix(head[i+1:], "}"), ",") {
				if k, val, ok := strings.Cut(kv, "="); ok {
					s.labels[k] = strings.Trim(val, `"`)
				}
			}
		} else {
			s.name = head
		}
		out = append(out, s)
	}
	return out, nil
}

// sum adds every sample of name whose labels match want (empty values match
// anything).
func (m metricSet) sum(name string, want map[string]string) float64 {
	var total float64
	for _, s := range m {
		if s.name != name || !matches(s.labels, want) {
			continue
		}
		total += s.value
	}
	return total
}

func (m metricSet) mean(histogram string, want map[string]string) float64 {
	count := m.sum(histogram+"_count", want)
	if count == 0 {
		return 0
	}
	return m.sum(histogram+"_sum", want) / count
}

func matches(labels, want map[string]string) bool {
	for k, v := range want {
		if v != "" && labels[k] != v {
			return false
		}
	}
	return true
}

// --- report ---

func rate(r result) float64 {
	if r.elapsed <= 0 {
		return 0
	}
	return float64(r.events) / r.elapsed.Seconds()
}

func renderReport(results []result, lat *latencyResult) string {
	var sb strings.Builder
	sb.WriteString("| Scenario | Mode | Events | Time | Events/s | Avg batch | Avg sink write per batch |\n")
	sb.WriteString("|---|---|--:|--:|--:|--:|---|\n")
	for _, r := range results {
		var writes []string
		names := make([]string, 0, len(r.sinkMeanMs))
		for s := range r.sinkMeanMs {
			names = append(names, s)
		}
		sort.Strings(names)
		for _, s := range names {
			writes = append(writes, fmt.Sprintf("%s %.1f ms", s, r.sinkMeanMs[s]))
		}
		w := "–"
		if len(writes) > 0 {
			w = strings.Join(writes, ", ")
		}
		fmt.Fprintf(&sb, "| %s | %s | %s | %.2f s | **%s** | %s | %s |\n",
			r.scenario, r.mode, commas(float64(r.events)), r.elapsed.Seconds(), commas(rate(r)), commas(r.batchMean), w)
	}
	if lat != nil {
		fmt.Fprintf(&sb, "\nLatency, commit → Redis stream consumer, at %s rows/s (%s samples): p50 %s · p90 %s · p99 %s · p99.9 %s · max %s\n",
			commas(float64(lat.rate)), commas(float64(lat.samples)),
			ms(lat.p50), ms(lat.p90), ms(lat.p99), ms(lat.p999), ms(lat.max))
	}
	return sb.String()
}

func ms(d time.Duration) string {
	return fmt.Sprintf("%.1f ms", float64(d)/float64(time.Millisecond))
}

func commas(f float64) string {
	s := strconv.FormatInt(int64(f+0.5), 10)
	n := len(s)
	if n <= 3 {
		return s
	}
	var sb strings.Builder
	pre := n % 3
	if pre > 0 {
		sb.WriteString(s[:pre])
	}
	for i := pre; i < n; i += 3 {
		if sb.Len() > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(s[i : i+3])
	}
	return sb.String()
}
