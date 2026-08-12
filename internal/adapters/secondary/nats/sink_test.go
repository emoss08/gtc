package nats

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/emoss08/gtc/internal/core/domain"
)

// runServer starts an in-process NATS server with JetStream, so these tests
// exercise the real protocol rather than a mock.
func runServer(t *testing.T) *natsserver.Server {
	t.Helper()

	srv, err := natsserver.NewServer(&natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1, // any free port
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	})
	if err != nil {
		t.Fatalf("create NATS server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("NATS server did not become ready")
	}
	t.Cleanup(srv.Shutdown)
	return srv
}

func testConfig(url string) Config {
	return Config{
		Enabled:          true,
		URL:              url,
		SubjectPrefix:    "cdc",
		JetStream:        true,
		Stream:           "GTC_TEST",
		AutoCreateStream: true,
		Timeout:          5 * time.Second,
	}
}

func newSink(t *testing.T, cfg Config, resolver SubjectResolver) *Sink {
	t.Helper()
	sink := NewSink(SinkParams{
		Config:   cfg,
		Resolver: resolver,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err := sink.Initialize(context.Background()); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	t.Cleanup(func() { _ = sink.Shutdown(context.Background()) })
	return sink
}

func testEvent() domain.CDCEvent {
	return domain.CDCEvent{
		ID:        "0/1A2B3C",
		Operation: domain.OperationInsert,
		Schema:    "public",
		Table:     "users",
		NewData:   map[string]any{"id": 1, "email": "ada@example.com"},
		Metadata: domain.EventMetadata{
			LSN:           "0/1A2B3C",
			TransactionID: 99,
			Timestamp:     time.Unix(1700000000, 0).UTC(),
		},
	}
}

func TestPublishesToJetStream(t *testing.T) {
	srv := runServer(t)
	sink := newSink(t, testConfig(srv.ClientURL()), nil)

	if err := sink.Process(context.Background(), testEvent()); err != nil {
		t.Fatalf("process: %v", err)
	}

	msg := fetchOne(t, srv.ClientURL(), "GTC_TEST", "cdc.public.users")
	var decoded map[string]any
	if err := json.Unmarshal(msg, &decoded); err != nil {
		t.Fatalf("payload is not JSON: %v\n%s", err, msg)
	}
	if decoded["operation"] != "INSERT" || decoded["table"] != "users" {
		t.Errorf("payload = %v", decoded)
	}
	if decoded["event_id"] != "0/1A2B3C" {
		t.Errorf("event_id = %v", decoded["event_id"])
	}
}

// At-least-once delivery replays events after a failure. Publishing with the
// LSN as the message ID lets JetStream collapse those replays.
func TestRedeliveryIsDeduplicatedByLSN(t *testing.T) {
	srv := runServer(t)
	sink := newSink(t, testConfig(srv.ClientURL()), nil)

	for range 3 {
		if err := sink.Process(context.Background(), testEvent()); err != nil {
			t.Fatalf("process: %v", err)
		}
	}

	if got := streamMessages(t, srv.ClientURL(), "GTC_TEST"); got != 1 {
		t.Errorf("stream holds %d messages after 3 redeliveries of one event, want 1", got)
	}
}

func TestDistinctEventsAreAllPublished(t *testing.T) {
	srv := runServer(t)
	sink := newSink(t, testConfig(srv.ClientURL()), nil)

	for i, id := range []string{"0/1", "0/2", "0/3"} {
		event := testEvent()
		event.ID = id
		event.Metadata.LSN = id
		event.NewData = map[string]any{"id": i}
		if err := sink.Process(context.Background(), event); err != nil {
			t.Fatalf("process %s: %v", id, err)
		}
	}

	if got := streamMessages(t, srv.ClientURL(), "GTC_TEST"); got != 3 {
		t.Errorf("stream holds %d messages, want 3", got)
	}
}

// A stream is created for the configured subjects so a fresh NATS server
// works with no manual setup.
func TestStreamIsAutoCreated(t *testing.T) {
	srv := runServer(t)
	newSink(t, testConfig(srv.ClientURL()), nil)

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := js.Stream(ctx, "GTC_TEST")
	if err != nil {
		t.Fatalf("stream was not created: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(info.Config.Subjects) != 1 || info.Config.Subjects[0] != "cdc.>" {
		t.Errorf("stream subjects = %v, want [cdc.>]", info.Config.Subjects)
	}
}

func TestCoreNATSPublishWithoutJetStream(t *testing.T) {
	srv := runServer(t)
	cfg := testConfig(srv.ClientURL())
	cfg.JetStream = false

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	sub, err := nc.SubscribeSync("cdc.public.users")
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	sink := newSink(t, cfg, nil)
	if err := sink.Process(context.Background(), testEvent()); err != nil {
		t.Fatalf("process: %v", err)
	}

	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("no message received over core NATS: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(msg.Data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["event_id"] != "0/1A2B3C" {
		t.Errorf("payload = %v", decoded)
	}
}

type resolver struct {
	subject string
	skip    bool
}

func (r resolver) ShouldProcess(string, string) bool { return !r.skip }
func (r resolver) GenerateKey(domain.CDCEvent) (string, error) {
	return r.subject, nil
}

func TestResolverControlsSubject(t *testing.T) {
	srv := runServer(t)
	sink := newSink(t, testConfig(srv.ClientURL()), resolver{subject: "cdc.custom.route"})

	if err := sink.Process(context.Background(), testEvent()); err != nil {
		t.Fatalf("process: %v", err)
	}
	if msg := fetchOne(t, srv.ClientURL(), "GTC_TEST", "cdc.custom.route"); len(msg) == 0 {
		t.Error("event was not published to the resolver's subject")
	}
}

func TestFilteredTableIsSkipped(t *testing.T) {
	srv := runServer(t)
	sink := newSink(t, testConfig(srv.ClientURL()), resolver{skip: true})

	if err := sink.Process(context.Background(), testEvent()); err != nil {
		t.Fatalf("a filtered event must be skipped, not fail: %v", err)
	}
	if got := streamMessages(t, srv.ClientURL(), "GTC_TEST"); got != 0 {
		t.Errorf("stream holds %d messages for a filtered table, want 0", got)
	}
}

func TestPublishFailsWhenServerIsGone(t *testing.T) {
	srv := runServer(t)
	sink := newSink(t, testConfig(srv.ClientURL()), nil)
	srv.Shutdown()

	if err := sink.Process(context.Background(), testEvent()); err == nil {
		t.Fatal("publishing to a downed server must fail so the event is retried")
	}
}

func TestHealthCheckReflectsConnection(t *testing.T) {
	srv := runServer(t)
	sink := newSink(t, testConfig(srv.ClientURL()), nil)

	if err := sink.HealthCheck(context.Background()); err != nil {
		t.Errorf("healthy sink reported unhealthy: %v", err)
	}
	srv.Shutdown()
	// The client notices the drop asynchronously; give it a moment.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if sink.HealthCheck(context.Background()) != nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Error("health check stayed healthy after the server went away")
}

func TestSanitizeSubject(t *testing.T) {
	cases := map[string]string{
		"cdc.public.users":     "cdc.public.users",
		"cdc.public.odd table": "cdc.public.odd_table",
		"cdc.public.*":         "cdc.public._",
		"cdc.public.>":         "cdc.public._",
		"cdc.public.a\tb":      "cdc.public.a_b",
	}
	for in, want := range cases {
		if got := SanitizeSubject(in); got != want {
			t.Errorf("SanitizeSubject(%q) = %q, want %q", in, got, want)
		}
	}
}

// Wildcards in a generated subject would make the publish fail outright, so
// a table whose name contains one must still be deliverable.
func TestWildcardTableNameIsPublishable(t *testing.T) {
	srv := runServer(t)
	sink := newSink(t, testConfig(srv.ClientURL()), nil)

	event := testEvent()
	event.Table = "we*rd"
	if err := sink.Process(context.Background(), event); err != nil {
		t.Fatalf("a table name with a wildcard must still publish: %v", err)
	}
	if msg := fetchOne(t, srv.ClientURL(), "GTC_TEST", "cdc.public.we_rd"); len(msg) == 0 {
		t.Error("event was not published to the sanitized subject")
	}
}

// fetchOne returns the first message on a subject, failing if none arrives.
func fetchOne(t *testing.T, url, stream, subject string) []byte {
	t.Helper()

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	str, err := js.Stream(ctx, stream)
	if err != nil {
		t.Fatalf("stream %s: %v", stream, err)
	}
	msg, err := str.GetLastMsgForSubject(ctx, subject)
	if err != nil {
		t.Fatalf("no message on %s: %v", subject, err)
	}
	return msg.Data
}

func streamMessages(t *testing.T, url, stream string) uint64 {
	t.Helper()

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	str, err := js.Stream(ctx, stream)
	if err != nil {
		t.Fatalf("stream %s: %v", stream, err)
	}
	info, err := str.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return info.State.Msgs
}
