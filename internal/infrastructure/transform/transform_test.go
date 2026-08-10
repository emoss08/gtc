package transform

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/emoss08/gtc/internal/infrastructure/config"
)

func event(op domain.Operation, newData, oldData map[string]any) domain.CDCEvent {
	return domain.CDCEvent{
		ID:        "0/1000",
		Operation: op,
		Schema:    "public",
		Table:     "users",
		NewData:   newData,
		OldData:   oldData,
	}
}

func mustCompile(t *testing.T, specs ...config.TransformSpec) *Chain {
	t.Helper()
	chain, err := Compile(specs...)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return chain
}

func TestFilterKeepsAndDropsRows(t *testing.T) {
	chain := mustCompile(t, config.TransformSpec{Filter: `new.status != "draft"`})

	_, keep, err := chain.Apply(event(domain.OperationInsert,
		map[string]any{"status": "published"}, nil))
	if err != nil || !keep {
		t.Fatalf("published row must be kept: keep=%v err=%v", keep, err)
	}

	_, keep, err = chain.Apply(event(domain.OperationInsert,
		map[string]any{"status": "draft"}, nil))
	if err != nil || keep {
		t.Fatalf("draft row must be filtered: keep=%v err=%v", keep, err)
	}
}

func TestFilterSeesOperationAndTable(t *testing.T) {
	chain := mustCompile(t, config.TransformSpec{
		Filter: `op != "DELETE" && table == "users"`,
	})

	_, keep, _ := chain.Apply(event(domain.OperationDelete, nil, map[string]any{"id": 1}))
	if keep {
		t.Fatal("DELETE must be filtered by op check")
	}

	_, keep, _ = chain.Apply(event(domain.OperationInsert, map[string]any{"id": 1}, nil))
	if !keep {
		t.Fatal("INSERT on users must pass")
	}
}

func TestFilterMissingKeyIsAnError(t *testing.T) {
	chain := mustCompile(t, config.TransformSpec{Filter: `new.missing_column == "x"`})

	_, _, err := chain.Apply(event(domain.OperationInsert, map[string]any{"id": 1}, nil))
	if err == nil {
		t.Fatal("accessing a missing column must surface an error, not silently drop data")
	}
}

func TestGuardedMissingKeyFilter(t *testing.T) {
	chain := mustCompile(t, config.TransformSpec{
		Filter: `"status" in new && new.status == "live"`,
	})

	_, keep, err := chain.Apply(event(domain.OperationInsert, map[string]any{"id": 1}, nil))
	if err != nil {
		t.Fatalf("guarded filter must not error: %v", err)
	}
	if keep {
		t.Fatal("row without status must be filtered")
	}
}

func TestDropColumnsDoesNotMutateOriginal(t *testing.T) {
	chain := mustCompile(t, config.TransformSpec{DropColumns: []string{"password_hash"}})

	original := map[string]any{"id": 1, "password_hash": "secret"}
	out, keep, err := chain.Apply(event(domain.OperationInsert, original, nil))
	if err != nil || !keep {
		t.Fatalf("unexpected: keep=%v err=%v", keep, err)
	}

	if _, present := out.NewData["password_hash"]; present {
		t.Error("password_hash must be dropped from the transformed event")
	}
	if _, present := original["password_hash"]; !present {
		t.Error("the original event's map must not be mutated (it is shared across sinks)")
	}
}

func TestMaskStrategies(t *testing.T) {
	chain := mustCompile(t, config.TransformSpec{Mask: map[string]string{
		"email": "sha256",
		"card":  "last4",
		"ssn":   "redact",
		"note":  "null",
	}})

	out, _, err := chain.Apply(event(domain.OperationInsert, map[string]any{
		"email": "ada@example.com",
		"card":  "4242424242424242",
		"ssn":   "123-45-6789",
		"note":  "sensitive",
		"nil":   nil,
	}, nil))
	if err != nil {
		t.Fatal(err)
	}

	if got := out.NewData["email"].(string); len(got) != 64 || got == "ada@example.com" {
		t.Errorf("sha256 mask failed: %q", got)
	}
	if got := out.NewData["card"].(string); got != strings.Repeat("*", 12)+"4242" {
		t.Errorf("last4 mask failed: %q", got)
	}
	if out.NewData["ssn"] != "[REDACTED]" {
		t.Errorf("redact mask failed: %v", out.NewData["ssn"])
	}
	if out.NewData["note"] != nil {
		t.Errorf("null mask failed: %v", out.NewData["note"])
	}
}

func TestMaskAppliesToOldData(t *testing.T) {
	chain := mustCompile(t, config.TransformSpec{Mask: map[string]string{"email": "redact"}})

	out, _, err := chain.Apply(event(domain.OperationUpdate,
		map[string]any{"email": "new@example.com"},
		map[string]any{"email": "old@example.com"},
	))
	if err != nil {
		t.Fatal(err)
	}
	if out.OldData["email"] != "[REDACTED]" {
		t.Errorf("old data must be masked too: %v", out.OldData["email"])
	}
}

func TestCompileRejectsBadInput(t *testing.T) {
	if _, err := Compile(config.TransformSpec{Filter: "new.x >"}); err == nil {
		t.Error("syntactically invalid filter must fail compilation")
	}
	if _, err := Compile(config.TransformSpec{Filter: `new.status`}); err == nil {
		t.Error("non-boolean filter must fail compilation")
	}
	if _, err := Compile(config.TransformSpec{Mask: map[string]string{"x": "rot13"}}); err == nil {
		t.Error("unknown mask strategy must fail compilation")
	}
}

func TestNilChainIsNoOp(t *testing.T) {
	chain := mustCompile(t) // no specs -> nil chain
	in := event(domain.OperationInsert, map[string]any{"id": 1}, nil)
	out, keep, err := chain.Apply(in)
	if err != nil || !keep {
		t.Fatalf("nil chain must keep everything: keep=%v err=%v", keep, err)
	}
	if out.NewData["id"] != 1 {
		t.Error("nil chain must pass data through unchanged")
	}
}

// captureSink records the events it receives.
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

func TestSinkAppliesGlobalThenTableChain(t *testing.T) {
	inner := &captureSink{}
	sink, err := NewSink(inner,
		&config.TransformSpec{Mask: map[string]string{"email": "redact"}},
		map[string]config.TransformSpec{
			"public.users": {DropColumns: []string{"password_hash"}},
		},
		slog.Default(),
	)
	if err != nil {
		t.Fatal(err)
	}

	err = sink.Process(context.Background(), event(domain.OperationInsert, map[string]any{
		"id": 1, "email": "a@b.c", "password_hash": "x",
	}, nil))
	if err != nil {
		t.Fatal(err)
	}

	if len(inner.events) != 1 {
		t.Fatalf("expected 1 delivered event, got %d", len(inner.events))
	}
	got := inner.events[0].NewData
	if got["email"] != "[REDACTED]" {
		t.Errorf("global mask not applied: %v", got["email"])
	}
	if _, present := got["password_hash"]; present {
		t.Error("table-level drop not applied")
	}
}

func TestSinkFiltersEvent(t *testing.T) {
	inner := &captureSink{}
	sink, err := NewSink(inner, &config.TransformSpec{Filter: `op != "DELETE"`}, nil, slog.Default())
	if err != nil {
		t.Fatal(err)
	}

	if err := sink.Process(context.Background(),
		event(domain.OperationDelete, nil, map[string]any{"id": 1})); err != nil {
		t.Fatal(err)
	}
	if len(inner.events) != 0 {
		t.Fatal("filtered event must not reach the inner sink")
	}
}

func TestSinkWithoutTransformsIsUnwrapped(t *testing.T) {
	inner := &captureSink{}
	sink, err := NewSink(inner, nil, nil, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if sink != domain.Sink(inner) {
		t.Error("no transforms must return the inner sink unchanged")
	}
}
