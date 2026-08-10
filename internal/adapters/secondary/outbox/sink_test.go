package outbox

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/emoss08/gtc/internal/infrastructure/config"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testConfig() Config {
	return Config{
		Schema:       "public",
		Table:        "outbox",
		StreamPrefix: "events",
		Columns: config.OutboxColumns{
			ID:        "id",
			Topic:     "topic",
			Key:       "aggregate_id",
			Payload:   "payload",
			EventType: "event_type",
		},
	}
}

func outboxEvent(row map[string]any) domain.CDCEvent {
	return domain.CDCEvent{
		ID:        "0/1000",
		Operation: domain.OperationInsert,
		Schema:    "public",
		Table:     "outbox",
		NewData:   row,
	}
}

func TestBuildMessageFullRow(t *testing.T) {
	stream, fields, err := buildMessage(testConfig(), outboxEvent(map[string]any{
		"id":           "evt-1",
		"topic":        "orders",
		"event_type":   "OrderPlaced",
		"aggregate_id": "order-42",
		"payload":      map[string]any{"total": int64(99)},
	}))
	if err != nil {
		t.Fatal(err)
	}

	if stream != "events:orders" {
		t.Errorf("expected stream events:orders, got %q", stream)
	}
	if fields["id"] != "evt-1" || fields["type"] != "OrderPlaced" || fields["key"] != "order-42" {
		t.Errorf("unexpected fields: %v", fields)
	}
	if fields["payload"] != `{"total":99}` {
		t.Errorf("jsonb payload must be re-serialized: %v", fields["payload"])
	}
}

func TestBuildMessageStringPayloadPassesThrough(t *testing.T) {
	_, fields, err := buildMessage(testConfig(), outboxEvent(map[string]any{
		"id":      int64(7),
		"topic":   "audit",
		"payload": `{"raw":"json"}`,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if fields["payload"] != `{"raw":"json"}` {
		t.Errorf("string payload must pass through unchanged: %v", fields["payload"])
	}
	if fields["id"] != "7" {
		t.Errorf("numeric id must be stringified: %v", fields["id"])
	}
	if _, present := fields["type"]; present {
		t.Error("absent event_type must be omitted")
	}
	if _, present := fields["key"]; present {
		t.Error("absent aggregate key must be omitted")
	}
}

func TestBuildMessageTopicResolution(t *testing.T) {
	// No topic, no default: visible error.
	_, _, err := buildMessage(testConfig(), outboxEvent(map[string]any{
		"id": "e1", "payload": "x",
	}))
	if err == nil {
		t.Fatal("missing topic without default_topic must be an error")
	}

	// No topic, with default: falls back.
	cfg := testConfig()
	cfg.DefaultTopic = "misc"
	stream, _, err := buildMessage(cfg, outboxEvent(map[string]any{
		"id": "e1", "payload": "x",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if stream != "events:misc" {
		t.Errorf("expected default topic stream, got %q", stream)
	}
}

func TestBuildMessageMissingPayloadIsAnError(t *testing.T) {
	_, _, err := buildMessage(testConfig(), outboxEvent(map[string]any{
		"id": "e1", "topic": "orders",
	}))
	if err == nil {
		t.Fatal("missing payload must be an error")
	}
}

func TestProcessIgnoresOtherTablesAndOperations(t *testing.T) {
	sink := &Sink{cfg: testConfig(), logger: discardLogger()}

	// Different table: silently skipped (fast path, no redis call).
	ev := outboxEvent(map[string]any{"id": "e1"})
	ev.Table = "users"
	if err := sink.Process(context.Background(), ev); err != nil {
		t.Fatal(err)
	}

	// Non-INSERT operations on the outbox: skipped (deletes are
	// housekeeping, READ would republish history).
	for _, op := range []domain.Operation{
		domain.OperationUpdate, domain.OperationDelete,
		domain.OperationTruncate, domain.OperationRead,
	} {
		ev := outboxEvent(map[string]any{"id": "e1"})
		ev.Operation = op
		if err := sink.Process(context.Background(), ev); err != nil {
			t.Fatalf("op %s must be skipped, got %v", op, err)
		}
	}
}

func TestExcludeTableWrapper(t *testing.T) {
	inner := &captureSink{}
	wrapped := ExcludeTable(inner, "public", "outbox")

	if err := wrapped.Process(context.Background(), outboxEvent(map[string]any{"id": 1})); err != nil {
		t.Fatal(err)
	}
	if len(inner.events) != 0 {
		t.Fatal("outbox table events must not reach the wrapped sink")
	}

	other := outboxEvent(map[string]any{"id": 1})
	other.Table = "users"
	if err := wrapped.Process(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	if len(inner.events) != 1 {
		t.Fatal("other tables must pass through")
	}
	if wrapped.Name() != inner.Name() {
		t.Error("wrapper must delegate Name")
	}
}

type captureSink struct {
	events []domain.CDCEvent
}

func (s *captureSink) Name() string                      { return "capture" }
func (s *captureSink) Initialize(context.Context) error  { return nil }
func (s *captureSink) Shutdown(context.Context) error    { return nil }
func (s *captureSink) HealthCheck(context.Context) error { return nil }
func (s *captureSink) Process(_ context.Context, e domain.CDCEvent) error {
	s.events = append(s.events, e)
	return nil
}
