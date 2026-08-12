package webhook

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/emoss08/gtc/internal/core/domain"
)

func testEvent() domain.CDCEvent {
	return domain.CDCEvent{
		ID:        "0/1A2B3C",
		Operation: domain.OperationUpdate,
		Schema:    "public",
		Table:     "users",
		OldData:   map[string]any{"id": 1, "email": "old@example.com"},
		NewData:   map[string]any{"id": 1, "email": "new@example.com"},
		Metadata: domain.EventMetadata{
			LSN:           "0/1A2B3C",
			TransactionID: 4242,
			Timestamp:     time.Unix(1700000000, 0).UTC(),
		},
	}
}

// capture records what the receiver saw.
type capture struct {
	mu      sync.Mutex
	count   int
	headers http.Header
	body    []byte
}

func (c *capture) snapshot() (int, http.Header, []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count, c.headers, c.body
}

func newSink(t *testing.T, cfg Config, filter TableFilter) *Sink {
	t.Helper()
	if cfg.Timeout == 0 {
		cfg.Timeout = 2 * time.Second
	}
	return NewSink(SinkParams{
		Config: cfg,
		Filter: filter,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
}

func serve(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *capture) {
	t.Helper()
	rec := &capture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rec.mu.Lock()
		rec.count++
		rec.headers = r.Header.Clone()
		rec.body = body
		rec.mu.Unlock()
		handler(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv, rec
}

func TestDeliversEventAsJSON(t *testing.T) {
	srv, rec := serve(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	sink := newSink(t, Config{URL: srv.URL}, nil)

	if err := sink.Process(context.Background(), testEvent()); err != nil {
		t.Fatalf("process: %v", err)
	}

	count, headers, body := rec.snapshot()
	if count != 1 {
		t.Fatalf("receiver saw %d requests, want 1", count)
	}
	if got := headers.Get("Content-Type"); got != "application/json" {
		t.Errorf("content-type = %q", got)
	}
	// At-least-once delivery: the receiver needs a stable key to dedupe on.
	if got := headers.Get(HeaderEventID); got != "0/1A2B3C" {
		t.Errorf("%s = %q", HeaderEventID, got)
	}
	if got := headers.Get(HeaderTable); got != "public.users" {
		t.Errorf("%s = %q", HeaderTable, got)
	}
	if got := headers.Get(HeaderOperation); got != "UPDATE" {
		t.Errorf("%s = %q", HeaderOperation, got)
	}

	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("body is not JSON: %v\n%s", err, body)
	}
	if decoded["operation"] != "UPDATE" || decoded["table"] != "users" {
		t.Errorf("payload = %v", decoded)
	}
	newData, _ := decoded["new_data"].(map[string]any)
	if newData["email"] != "new@example.com" {
		t.Errorf("new_data = %v", decoded["new_data"])
	}
	meta, _ := decoded["metadata"].(map[string]any)
	if meta["lsn"] != "0/1A2B3C" {
		t.Errorf("metadata = %v", decoded["metadata"])
	}
}

func TestSignsBodyWhenSecretConfigured(t *testing.T) {
	srv, rec := serve(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) })
	sink := newSink(t, Config{URL: srv.URL, SigningSecret: "topsecret"}, nil)

	if err := sink.Process(context.Background(), testEvent()); err != nil {
		t.Fatalf("process: %v", err)
	}

	_, headers, body := rec.snapshot()
	timestamp := headers.Get(HeaderTimestamp)
	if timestamp == "" {
		t.Fatal("no timestamp header sent with a signed delivery")
	}
	want := Sign("topsecret", timestamp, body)
	if got := headers.Get(HeaderSignature); got != want {
		t.Errorf("signature = %q, want %q", got, want)
	}
	// The signature must actually depend on the secret and the timestamp.
	if Sign("other", timestamp, body) == want {
		t.Error("signature does not depend on the secret")
	}
	if Sign("topsecret", "0", body) == want {
		t.Error("signature does not depend on the timestamp (replayable)")
	}
}

func TestNoSignatureHeadersWithoutSecret(t *testing.T) {
	srv, rec := serve(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	sink := newSink(t, Config{URL: srv.URL}, nil)

	if err := sink.Process(context.Background(), testEvent()); err != nil {
		t.Fatalf("process: %v", err)
	}
	_, headers, _ := rec.snapshot()
	if headers.Get(HeaderSignature) != "" || headers.Get(HeaderTimestamp) != "" {
		t.Error("signature headers sent without a configured secret")
	}
}

func TestSendsAuthHeader(t *testing.T) {
	srv, rec := serve(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	sink := newSink(t, Config{URL: srv.URL, AuthHeader: "Bearer token123"}, nil)

	if err := sink.Process(context.Background(), testEvent()); err != nil {
		t.Fatalf("process: %v", err)
	}
	if _, headers, _ := rec.snapshot(); headers.Get("Authorization") != "Bearer token123" {
		t.Errorf("authorization = %q", headers.Get("Authorization"))
	}
}

// A non-2xx response must fail the event so the pipeline retries it rather
// than advancing the WAL position past an undelivered change.
func TestNonSuccessStatusIsAnError(t *testing.T) {
	srv, _ := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
		_, _ = w.Write([]byte("upstream exploded"))
	})
	sink := newSink(t, Config{URL: srv.URL}, nil)

	err := sink.Process(context.Background(), testEvent())
	if err == nil {
		t.Fatal("a 500 response must be an error")
	}
	if !strings.Contains(err.Error(), "500") || !strings.Contains(err.Error(), "upstream exploded") {
		t.Errorf("error should quote the status and body: %v", err)
	}
}

func TestUnreachableEndpointIsAnError(t *testing.T) {
	// Port 1 on loopback refuses connections immediately.
	sink := newSink(t, Config{URL: "http://127.0.0.1:1/hook"}, nil)
	if err := sink.Process(context.Background(), testEvent()); err == nil {
		t.Fatal("an unreachable endpoint must be an error")
	}
}

func TestTimeoutIsAnError(t *testing.T) {
	release := make(chan struct{})
	srv, _ := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		<-release
		w.WriteHeader(200)
	})
	t.Cleanup(func() { close(release) })

	sink := newSink(t, Config{URL: srv.URL, Timeout: 150 * time.Millisecond}, nil)
	if err := sink.Process(context.Background(), testEvent()); err == nil {
		t.Fatal("a hung receiver must time out, not block the pipeline forever")
	}
}

type onlyTable struct{ table string }

func (f onlyTable) ShouldProcess(_, table string) bool { return table == f.table }

func TestFilteredTablesAreNotDelivered(t *testing.T) {
	srv, rec := serve(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	sink := newSink(t, Config{URL: srv.URL}, onlyTable{table: "orders"})

	if err := sink.Process(context.Background(), testEvent()); err != nil {
		t.Fatalf("a filtered event must be skipped, not fail: %v", err)
	}
	if count, _, _ := rec.snapshot(); count != 0 {
		t.Errorf("receiver saw %d requests for a filtered table, want 0", count)
	}
}

// The pipeline cancels a sink's context when the per-event deadline passes;
// the sink must abort rather than keep the connection open.
func TestRespectsContextCancellation(t *testing.T) {
	release := make(chan struct{})
	srv, _ := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		<-release
		w.WriteHeader(200)
	})
	t.Cleanup(func() { close(release) })

	sink := newSink(t, Config{URL: srv.URL, Timeout: 10 * time.Second}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if err := sink.Process(ctx, testEvent()); err == nil {
		t.Fatal("a cancelled context must abort the delivery")
	}
}

func TestDeleteEventCarriesOldDataOnly(t *testing.T) {
	srv, rec := serve(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	sink := newSink(t, Config{URL: srv.URL}, nil)

	event := testEvent()
	event.Operation = domain.OperationDelete
	event.NewData = nil
	if err := sink.Process(context.Background(), event); err != nil {
		t.Fatalf("process: %v", err)
	}

	_, _, body := rec.snapshot()
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["new_data"] != nil {
		t.Errorf("DELETE must not carry new_data: %v", decoded["new_data"])
	}
	if decoded["old_data"] == nil {
		t.Error("DELETE must carry old_data")
	}
}
