package redis

import (
	"strings"
	"testing"

	"github.com/emoss08/gtc/internal/core/domain"
)

type staticPatternResolver struct {
	pattern string
}

func (r staticPatternResolver) GetKeyPattern(schema, table string) string {
	return r.pattern
}

func testEvent() domain.CDCEvent {
	return domain.CDCEvent{
		ID:        "0/1000",
		Operation: domain.OperationUpdate,
		Schema:    "public",
		Table:     "users",
		NewData:   map[string]any{"id": int64(7), "name": "alice"},
		OldData:   map[string]any{"id": int64(7), "name": "bob"},
	}
}

func TestGenerateKeyWithConfiguredPattern(t *testing.T) {
	kr, err := NewKeyResolver(KeyResolverParams{
		Resolver: staticPatternResolver{pattern: `{{.Prefix}}:{{.Table}}:{{.Field "id"}}`},
		Prefix:   "cdc",
	})
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}

	key, err := kr.GenerateKey(testEvent())
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	if key != "cdc:users:7" {
		t.Errorf("expected cdc:users:7, got %q", key)
	}
}

func TestGenerateKeyFallsBackToDefaultPattern(t *testing.T) {
	kr, err := NewKeyResolver(KeyResolverParams{
		Resolver:       staticPatternResolver{pattern: ""},
		Prefix:         "cdc",
		DefaultPattern: DefaultJSONKeyPattern,
	})
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}

	key, err := kr.GenerateKey(testEvent())
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	if key != "cdc:public:users:7" {
		t.Errorf("expected cdc:public:users:7, got %q", key)
	}
}

func TestFieldFallsBackToOldData(t *testing.T) {
	event := testEvent()
	event.NewData = nil // DELETE events only carry old data

	kr, err := NewKeyResolver(KeyResolverParams{
		Resolver: staticPatternResolver{pattern: `{{.Field "id"}}`},
		Prefix:   "cdc",
	})
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}

	key, err := kr.GenerateKey(event)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	if key != "7" {
		t.Errorf("expected 7, got %q", key)
	}
}

func TestInvalidPatternIsAnError(t *testing.T) {
	kr, err := NewKeyResolver(KeyResolverParams{
		Resolver: staticPatternResolver{pattern: "{{.Broken"},
		Prefix:   "cdc",
	})
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}

	if _, err := kr.GenerateKey(testEvent()); err == nil {
		t.Fatal("expected error for invalid pattern")
	} else if !strings.Contains(err.Error(), "invalid pattern") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestShouldProcessWithoutFilterAllowsAll(t *testing.T) {
	kr, err := NewKeyResolver(KeyResolverParams{
		Resolver: staticPatternResolver{},
		Prefix:   "cdc",
	})
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	if !kr.ShouldProcess("public", "anything") {
		t.Error("resolver without filter must process every table")
	}
}
