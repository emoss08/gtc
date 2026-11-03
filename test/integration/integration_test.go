//go:build integration

// Package integration runs the compiled gateway against a real PostgreSQL
// (wal_level=logical) and Redis, and asserts that rows written to the source
// database arrive on the Redis stream sink: backfilled READ events for
// pre-existing rows, then INSERT/UPDATE/DELETE from live replication.
//
// It is excluded from `go test ./...` by the build tag. Run it with:
//
//	GTC_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//	GTC_TEST_REDIS_URL=redis://localhost:6379 \
//	go test -tags integration -v -timeout 5m ./test/integration
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/redis/go-redis/v9"
)

const (
	slotName    = "gtc_it_slot"
	publication = "gtc_it_publication"
	prefix      = "gtcit"
	table       = "gtc_it_users"
	stateTable  = "gtc_it_backfill_state"
	natsStream  = "GTC_IT"
	stream      = prefix + ":public:" + table
	bulkTable   = "gtc_it_bulk"
	bulkStream  = prefix + ":public:" + bulkTable
	// bulkRows is large enough that backfill spans several chunks and the
	// live bulk insert spans several batches inside one transaction.
	bulkRows = 20000
)

// webhookReceiver is a real HTTP endpoint the gateway subprocess delivers to.
type webhookReceiver struct {
	mu     sync.Mutex
	events []payload
	server *httptest.Server
}

func startWebhookReceiver(t *testing.T) *webhookReceiver {
	t.Helper()
	rec := &webhookReceiver{}
	rec.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p payload
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if r.Header.Get("X-GTC-Event-Id") == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		rec.mu.Lock()
		rec.events = append(rec.events, p)
		rec.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(rec.server.Close)
	return rec
}

func (w *webhookReceiver) snapshot() []payload {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]payload(nil), w.events...)
}

// await blocks until done(received) or the deadline passes.
func (w *webhookReceiver) await(t *testing.T, done func([]payload) bool) []payload {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		got := w.snapshot()
		if done(got) {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for webhook deliveries; got %d: %+v", len(got), got)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

type payload struct {
	Operation string         `json:"operation"`
	Table     string         `json:"table"`
	NewData   map[string]any `json:"new_data"`
	OldData   map[string]any `json:"old_data"`
}

func TestEndToEnd(t *testing.T) {
	dbURL := os.Getenv("GTC_TEST_DATABASE_URL")
	redisURL := os.Getenv("GTC_TEST_REDIS_URL")
	if dbURL == "" || redisURL == "" {
		t.Skip("set GTC_TEST_DATABASE_URL and GTC_TEST_REDIS_URL to run integration tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	db, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer db.Close(context.Background())

	ropts, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatalf("parse redis url: %v", err)
	}
	rdb := redis.NewClient(ropts)
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("connect redis: %v", err)
	}

	cleanup := func() {
		_, _ = db.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		_, _ = db.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", bulkTable))
		// The backfill resume state must not leak between runs, or the
		// second run would consider the table already backfilled.
		_, _ = db.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", stateTable))
		_, _ = db.Exec(context.Background(), fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", publication))
		_, _ = db.Exec(context.Background(),
			"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
			slotName)
		_ = rdb.Del(context.Background(), stream, bulkStream, prefix+":schema").Err()
	}
	cleanup()          // leftovers from an aborted previous run
	t.Cleanup(cleanup) // called after the gateway has been stopped

	// Rows that exist before the gateway ever starts: the slot does not
	// exist yet, so auto backfill must emit READ events for them.
	mustExec(t, ctx, db, fmt.Sprintf(
		"CREATE TABLE %s (id int PRIMARY KEY, name text, email text)", table))
	for i := 1; i <= 3; i++ {
		mustExec(t, ctx, db, fmt.Sprintf(
			"INSERT INTO %s (id, name, email) VALUES ($1, $2, $3)", table),
			i, fmt.Sprintf("seed-%d", i), fmt.Sprintf("seed-%d@example.com", i))
	}

	// A large table that also exists before the slot: its backfill spans
	// several chunks.
	mustExec(t, ctx, db, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, payload text)", bulkTable))
	mustExec(t, ctx, db, fmt.Sprintf(
		"INSERT INTO %s SELECT g, md5(g::text) FROM generate_series(1, %d) g", bulkTable, bulkRows))

	bin := buildGateway(t, ctx)

	// Preflight: `gtc doctor` must pass against a correctly provisioned
	// environment, and fail when a sink is unreachable.
	runDoctor(t, ctx, bin, dbURL, redisURL, true)
	runDoctor(t, ctx, bin, dbURL, "redis://127.0.0.1:1/", false)

	hook := startWebhookReceiver(t)
	natsURL := os.Getenv("GTC_TEST_NATS_URL")
	gw := startGateway(t, ctx, bin, dbURL, redisURL, hook.server.URL, natsURL)
	gw.awaitReady(t)
	backfillStart := time.Now()

	// Backfill: three READ events, one per pre-existing row.
	events := awaitEvents(t, ctx, rdb, func(seen []payload) bool {
		return len(distinctIDs(seen, "READ")) >= 3
	})
	reads := distinctIDs(events, "READ")
	for i := 1; i <= 3; i++ {
		if _, ok := reads[fmt.Sprint(i)]; !ok {
			t.Fatalf("backfill missing READ for id=%d; got %v", i, reads)
		}
	}

	// Every pre-existing bulk row is backfilled exactly as READ.
	bulkReads := awaitBulk(t, ctx, rdb, "READ", bulkRows)
	t.Logf("backfilled %d rows in %s", bulkReads, time.Since(backfillStart).Round(time.Millisecond))

	// Live volume: one transaction far larger than a batch (delivered in
	// several batches before its commit is confirmed) plus many small
	// transactions that the reader coalesces into shared batches.
	liveStart := time.Now()
	mustExec(t, ctx, db, fmt.Sprintf(
		"INSERT INTO %s SELECT g, md5(g::text) FROM generate_series(%d, %d) g",
		bulkTable, bulkRows+1, 2*bulkRows))
	for i := 1; i <= 500; i++ {
		mustExec(t, ctx, db, fmt.Sprintf("INSERT INTO %s VALUES ($1, 'small')", bulkTable), 2*bulkRows+i)
	}
	bulkInserts := awaitBulk(t, ctx, rdb, "INSERT", bulkRows+500)
	t.Logf("streamed %d live rows in %s", bulkInserts, time.Since(liveStart).Round(time.Millisecond))

	// Live stream: INSERT, UPDATE, DELETE on one row, in order.
	mustExec(t, ctx, db, fmt.Sprintf(
		"INSERT INTO %s (id, name, email) VALUES (4, 'ada', 'ada@example.com')", table))
	mustExec(t, ctx, db, fmt.Sprintf(
		"UPDATE %s SET email = 'ada@lovelace.dev' WHERE id = 4", table))
	mustExec(t, ctx, db, fmt.Sprintf("DELETE FROM %s WHERE id = 4", table))

	events = awaitEvents(t, ctx, rdb, func(seen []payload) bool {
		return rowEventCount(seen, "4") >= 3
	})

	var ops []string
	for _, e := range events {
		if eventRowID(e) != "4" {
			continue
		}
		// At-least-once delivery: collapse duplicates of the same operation.
		if len(ops) == 0 || ops[len(ops)-1] != e.Operation {
			ops = append(ops, e.Operation)
		}
		switch e.Operation {
		case "INSERT":
			if e.NewData["email"] != "ada@example.com" {
				t.Errorf("INSERT new_data wrong: %v", e.NewData)
			}
		case "UPDATE":
			if e.NewData["email"] != "ada@lovelace.dev" {
				t.Errorf("UPDATE new_data wrong: %v", e.NewData)
			}
		case "DELETE":
			if e.NewData != nil {
				t.Errorf("DELETE must not carry new_data: %v", e.NewData)
			}
		}
	}
	if want := []string{"INSERT", "UPDATE", "DELETE"}; !slices.Equal(ops, want) {
		t.Fatalf("row 4 operations = %v, want %v (per-key order must hold)", ops, want)
	}

	// The webhook sink delivered the same live changes over HTTP.
	hookEvents := hook.await(t, func(got []payload) bool {
		return rowEventCount(got, "4") >= 3
	})
	var hookOps []string
	for _, e := range hookEvents {
		if eventRowID(e) == "4" && (len(hookOps) == 0 || hookOps[len(hookOps)-1] != e.Operation) {
			hookOps = append(hookOps, e.Operation)
		}
	}
	if want := []string{"INSERT", "UPDATE", "DELETE"}; !slices.Equal(hookOps, want) {
		t.Errorf("webhook row 4 operations = %v, want %v", hookOps, want)
	}

	// The NATS sink publishes the same events when a server is available.
	if natsURL != "" {
		assertNATSDelivery(t, ctx, natsURL)
	} else {
		t.Log("GTC_TEST_NATS_URL unset; skipping NATS sink assertions")
	}

	// DDL: PostgreSQL re-describes the table before the next row change, and
	// GTC turns that into a schema-change notification.
	mustExec(t, ctx, db, fmt.Sprintf("ALTER TABLE %s ADD COLUMN nickname text", table))
	mustExec(t, ctx, db, fmt.Sprintf("ALTER TABLE %s DROP COLUMN email", table))
	mustExec(t, ctx, db, fmt.Sprintf(
		"INSERT INTO %s (id, name, nickname) VALUES (5, 'grace', 'amazing grace')", table))

	ddl := awaitSchemaChange(t, ctx, gw.baseURL, fmt.Sprintf("public.%s", table))
	if !ddl.Breaking {
		t.Errorf("dropping a column must be reported as breaking: %+v", ddl)
	}
	if !slices.Contains(ddl.Kinds, "column_added") || !slices.Contains(ddl.Kinds, "column_dropped") {
		t.Errorf("kinds = %v, want both column_added and column_dropped", ddl.Kinds)
	}
	if len(ddl.AddedColumns) != 1 || ddl.AddedColumns[0].Name != "nickname" {
		t.Errorf("added columns = %+v", ddl.AddedColumns)
	}
	if ddl.AddedColumns[0].Type != "text" {
		t.Errorf("added column type = %q, want text", ddl.AddedColumns[0].Type)
	}
	if len(ddl.DroppedColumns) != 1 || ddl.DroppedColumns[0].Name != "email" {
		t.Errorf("dropped columns = %+v", ddl.DroppedColumns)
	}

	// The same change reaches the schema notification stream. The recorder
	// behind /api/schema and the Redis publisher are independent observers,
	// so the API can be ahead of the stream by a few hundred microseconds.
	last := awaitSchemaStreamEntry(t, ctx, rdb)
	if got := last["table"]; got != "public."+table {
		t.Errorf("schema entry table = %v", got)
	}
	if got := last["breaking"]; got != "true" {
		t.Errorf("schema entry breaking = %v, want true", got)
	}

	// The dashboard's stats endpoint sees the same pipeline.
	var stats struct {
		Streaming   bool    `json:"streaming"`
		EventsTotal float64 `json:"events_total"`
	}
	getJSON(t, ctx, gw.baseURL+"/api/stats", &stats)
	if !stats.Streaming || stats.EventsTotal < 3 {
		t.Errorf("stats disagree with the stream: %+v", stats)
	}

	// A second node on the same slot is a hot standby: it must not stream
	// (or backfill) while the first holds the slot, and must take over when
	// the first goes away.
	standby := startGateway(t, ctx, bin, dbURL, redisURL, hook.server.URL, natsURL,
		"CDC_SLOT_RETRY_INTERVAL=500ms")
	time.Sleep(3 * time.Second)
	if standby.ready() {
		t.Fatal("a second node must not stream from a slot another node holds")
	}

	gw.stop(t)
	standby.awaitReady(t)

	mustExec(t, ctx, db, fmt.Sprintf(
		"INSERT INTO %s (id, name) VALUES (7, 'after failover')", table))
	awaitEvents(t, ctx, rdb, func(seen []payload) bool {
		return rowEventCount(seen, "7") >= 1
	})

	standby.stop(t)
}

// awaitBulk waits until the bulk stream holds want distinct row ids for op,
// and fails if any id is missing at that point. It returns the distinct count.
func awaitBulk(t *testing.T, ctx context.Context, rdb *redis.Client, op string, want int) int {
	t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	for {
		ids := make(map[string]struct{}, want)
		start := "-"
		for {
			msgs, err := rdb.XRangeN(ctx, bulkStream, start, "+", 10000).Result()
			if err != nil && err != redis.Nil {
				t.Fatalf("xrange %s: %v", bulkStream, err)
			}
			for _, msg := range msgs {
				var p payload
				if err := json.Unmarshal([]byte(msg.Values["payload"].(string)), &p); err != nil {
					t.Fatalf("bad payload JSON: %v", err)
				}
				if p.Operation == op {
					ids[eventRowID(p)] = struct{}{}
				}
			}
			if len(msgs) < 10000 {
				break
			}
			start = "(" + msgs[len(msgs)-1].ID
		}
		if len(ids) >= want {
			return len(ids)
		}
		if time.Now().After(deadline) {
			t.Fatalf("bulk stream has %d distinct %s rows, want %d", len(ids), op, want)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// gateway is the compiled binary under test, running as a subprocess.
type gateway struct {
	cmd     *exec.Cmd
	baseURL string
	logs    *os.File
}

func buildGateway(t *testing.T, ctx context.Context) string {
	t.Helper()

	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}

	bin := filepath.Join(t.TempDir(), "gateway")
	build := exec.CommandContext(ctx, "go", "build", "-o", bin, "./cmd/gateway")
	build.Dir = repoRoot
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build gateway: %v\n%s", err, out)
	}
	return bin
}

// runDoctor executes `gateway doctor` and asserts on its exit code.
func runDoctor(t *testing.T, ctx context.Context, bin, dbURL, redisURL string, wantOK bool) {
	t.Helper()

	cmd := exec.CommandContext(ctx, bin, "doctor")
	cmd.Env = append(os.Environ(),
		"DATABASE_URL="+withReplication(t, dbURL),
		"REDIS_URL="+redisURL,
		"CDC_SLOT_NAME="+slotName,
		"CDC_PUBLICATION_NAME="+publication,
	)
	out, err := cmd.CombinedOutput()
	ok := err == nil
	if ok != wantOK {
		t.Fatalf("doctor exit ok=%v, want %v; output:\n%s", ok, wantOK, out)
	}
	if wantOK && !strings.Contains(string(out), "Ready to stream.") {
		t.Fatalf("doctor passed without the ready line:\n%s", out)
	}
}

func startGateway(
	t *testing.T,
	ctx context.Context,
	bin, dbURL, redisURL, webhookURL, natsURL string,
	extraEnv ...string,
) *gateway {
	t.Helper()

	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}

	port := freePort(t)
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)

	logs, err := os.Create(filepath.Join(t.TempDir(), "gateway.log"))
	if err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(bin)
	cmd.Dir = repoRoot
	cmd.Stdout = logs
	cmd.Stderr = logs
	cmd.Env = append(os.Environ(),
		"DATABASE_URL="+withReplication(t, dbURL),
		"REDIS_URL="+redisURL,
		fmt.Sprintf("HTTP_PORT=%d", port),
		"CDC_SLOT_NAME="+slotName,
		"CDC_PUBLICATION_NAME="+publication,
		"REDIS_STREAM_PREFIX="+prefix,
		"CDC_BACKFILL_MODE=auto",
		"CDC_BACKFILL_STATE_TABLE="+stateTable,
		"WEBHOOK_URL="+webhookURL,
		"WEBHOOK_SIGNING_SECRET=integration-secret",
		"NATS_URL="+natsURL,
		"NATS_SUBJECT_PREFIX="+prefix,
		"NATS_STREAM="+natsStream,
		"LOG_LEVEL=DEBUG",
		// The bulk run must not be trimmed out of the stream.
		"REDIS_MAX_STREAM_LEN=200000",
	)
	cmd.Env = append(cmd.Env, extraEnv...)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start gateway: %v", err)
	}

	gw := &gateway{cmd: cmd, baseURL: baseURL, logs: logs}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
		gw.dumpLogsOnFailure(t)
	})

	return gw
}

// ready reports whether the gateway is streaming from the slot.
func (g *gateway) ready() bool {
	resp, err := http.Get(g.baseURL + "/readiness")
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// awaitReady waits until replication is streaming.
func (g *gateway) awaitReady(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for !g.ready() {
		if time.Now().After(deadline) {
			t.Fatal("gateway never became ready")
		}
		if g.cmd.ProcessState != nil {
			t.Fatal("gateway exited before becoming ready")
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// stop asks for a graceful shutdown and requires a clean exit, so a hang or
// panic in teardown (slot release, sink shutdown) fails the test.
func (g *gateway) stop(t *testing.T) {
	t.Helper()
	if err := g.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal gateway: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- g.cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("gateway did not exit cleanly: %v", err)
		}
	case <-time.After(20 * time.Second):
		_ = g.cmd.Process.Kill()
		t.Error("gateway did not shut down within 20s of SIGTERM")
	}
}

func (g *gateway) dumpLogsOnFailure(t *testing.T) {
	if !t.Failed() {
		return
	}
	data, err := os.ReadFile(g.logs.Name())
	if err != nil {
		t.Logf("read gateway log: %v", err)
		return
	}
	t.Logf("gateway output:\n%s", data)
}

// awaitEvents polls the stream until done(events) is true, returning every
// decoded payload seen so far.
func awaitEvents(
	t *testing.T,
	ctx context.Context,
	rdb *redis.Client,
	done func([]payload) bool,
) []payload {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		msgs, err := rdb.XRange(ctx, stream, "-", "+").Result()
		if err != nil && err != redis.Nil {
			t.Fatalf("xrange %s: %v", stream, err)
		}
		events := make([]payload, 0, len(msgs))
		for _, msg := range msgs {
			raw, ok := msg.Values["payload"].(string)
			if !ok {
				t.Fatalf("stream entry without payload field: %v", msg.Values)
			}
			var p payload
			if err := json.Unmarshal([]byte(raw), &p); err != nil {
				t.Fatalf("bad payload JSON: %v\n%s", err, raw)
			}
			events = append(events, p)
		}
		if done(events) {
			return events
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for events; stream has %d entries: %+v",
				len(events), events)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

type schemaChange struct {
	Schema         string   `json:"schema"`
	Table          string   `json:"table"`
	Kinds          []string `json:"kinds"`
	Breaking       bool     `json:"breaking"`
	Summary        string   `json:"summary"`
	AddedColumns   []column `json:"added_columns"`
	DroppedColumns []column `json:"dropped_columns"`
}

type column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// assertNATSDelivery checks the live changes reached the JetStream stream.
func assertNATSDelivery(t *testing.T, ctx context.Context, natsURL string) {
	t.Helper()

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("connect nats: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	subject := fmt.Sprintf("%s.public.%s", prefix, table)
	deadline := time.Now().Add(60 * time.Second)
	for {
		stream, err := js.Stream(ctx, natsStream)
		if err == nil {
			msg, err := stream.GetLastMsgForSubject(ctx, subject)
			if err == nil {
				var p payload
				if err := json.Unmarshal(msg.Data, &p); err != nil {
					t.Fatalf("nats payload is not JSON: %v", err)
				}
				if p.Table != table {
					t.Errorf("nats payload table = %q, want %q", p.Table, table)
				}
				if p.Operation == "" {
					t.Errorf("nats payload has no operation: %+v", p)
				}
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no NATS message on %s within the deadline", subject)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// awaitSchemaStreamEntry polls the schema notification stream until it has an
// entry, returning the newest one.
func awaitSchemaStreamEntry(t *testing.T, ctx context.Context, rdb *redis.Client) map[string]any {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		msgs, err := rdb.XRange(ctx, prefix+":schema", "-", "+").Result()
		if err != nil && err != redis.Nil {
			t.Fatalf("xrange schema stream: %v", err)
		}
		if len(msgs) > 0 {
			return msgs[len(msgs)-1].Values
		}
		if time.Now().After(deadline) {
			t.Fatal("no entries on the schema notification stream")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// awaitSchemaChange polls /api/schema until a change for the table shows up.
func awaitSchemaChange(t *testing.T, ctx context.Context, baseURL, table string) schemaChange {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		var resp struct {
			Changes []schemaChange `json:"changes"`
		}
		getJSON(t, ctx, baseURL+"/api/schema", &resp)
		for _, change := range resp.Changes {
			if change.Schema+"."+change.Table == table {
				return change
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for a schema change on %s; saw %+v", table, resp.Changes)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func eventRowID(p payload) string {
	if v, ok := p.NewData["id"]; ok {
		return fmt.Sprint(v)
	}
	if v, ok := p.OldData["id"]; ok {
		return fmt.Sprint(v)
	}
	return ""
}

func distinctIDs(events []payload, op string) map[string]struct{} {
	ids := make(map[string]struct{})
	for _, e := range events {
		if e.Operation == op {
			ids[eventRowID(e)] = struct{}{}
		}
	}
	return ids
}

func rowEventCount(events []payload, id string) int {
	n := 0
	for _, e := range events {
		if eventRowID(e) == id {
			n++
		}
	}
	return n
}

func mustExec(t *testing.T, ctx context.Context, db *pgx.Conn, sql string, args ...any) {
	t.Helper()
	if _, err := db.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func withReplication(t *testing.T, dbURL string) string {
	t.Helper()
	u, err := url.Parse(dbURL)
	if err != nil {
		t.Fatalf("parse database url: %v", err)
	}
	q := u.Query()
	q.Set("replication", "database")
	u.RawQuery = q.Encode()
	return u.String()
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func getJSON(t *testing.T, ctx context.Context, url string, out any) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
}
