package config

import (
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func yamlUnmarshal(t *testing.T, input string, out any) error {
	t.Helper()
	return yaml.Unmarshal([]byte(input), out)
}

func TestGetBoolFallsBackOnInvalidInput(t *testing.T) {
	t.Setenv("TEST_BOOL", "yes") // not valid for strconv.ParseBool
	if got := getBool("TEST_BOOL", true); got != true {
		t.Error("invalid bool input must fall back to the default, got false")
	}

	t.Setenv("TEST_BOOL", "false")
	if got := getBool("TEST_BOOL", true); got != false {
		t.Error("valid bool input must be honored")
	}
}

func TestGetDurationFallsBackOnInvalidInput(t *testing.T) {
	t.Setenv("TEST_DUR", "30sec") // invalid unit
	if got := getDuration("TEST_DUR", 10*time.Second); got != 10*time.Second {
		t.Errorf("invalid duration must fall back to the default, got %v", got)
	}

	t.Setenv("TEST_DUR", "5s")
	if got := getDuration("TEST_DUR", 10*time.Second); got != 5*time.Second {
		t.Errorf("valid duration must be honored, got %v", got)
	}
}

func TestParseTableSet(t *testing.T) {
	t.Setenv("TEST_TABLES", " public.users, audit_logs ,, ")
	set := parseTableSet("TEST_TABLES")
	if len(set) != 2 {
		t.Fatalf("expected 2 tables, got %d: %v", len(set), set)
	}
	for _, want := range []string{"public.users", "audit_logs"} {
		if _, ok := set[want]; !ok {
			t.Errorf("missing %q in %v", want, set)
		}
	}
}

func TestRedisKeyedSinkConfig(t *testing.T) {
	cfg := RedisKeyedSinkConfig{
		SyncAll:           false,
		DefaultKeyPattern: "default-pattern",
		Tables: map[string]RedisTableConfig{
			"public.orders": {Key: "orders-pattern"},
			"events":        {Key: "events-pattern"},
		},
	}

	if !cfg.ShouldProcess("public", "orders") {
		t.Error("full-name match must be processed")
	}
	if !cfg.ShouldProcess("other", "events") {
		t.Error("bare-name match must be processed")
	}
	if cfg.ShouldProcess("public", "users") {
		t.Error("unlisted table must be skipped when sync_all is false")
	}

	cfg.SyncAll = true
	if !cfg.ShouldProcess("public", "users") {
		t.Error("sync_all must process every table")
	}

	if got := cfg.GetKeyPattern("public", "orders"); got != "orders-pattern" {
		t.Errorf("expected orders-pattern, got %q", got)
	}
	if got := cfg.GetKeyPattern("public", "users"); got != "default-pattern" {
		t.Errorf("expected default-pattern, got %q", got)
	}
}

func TestSinkTableConfigYAMLShapes(t *testing.T) {
	input := `
redis_json:
  transform:
    mask:
      email: sha256
  tables:
    public.users: "cdc:users:{{.Field \"id\"}}"
    public.orders:
      key: "cdc:orders:{{.Field \"id\"}}"
      filter: 'new.status != "draft"'
      drop_columns: [internal_notes]
      mask:
        card_number: last4
meilisearch:
  tables:
    public.products: products
    public.users:
      index: users
      drop_columns: [password_hash]
`
	var cfg SinksConfig
	if err := yamlUnmarshal(t, input, &cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Plain string stays a bare key pattern.
	if got := cfg.RedisJSON.GetKeyPattern("public", "users"); got != `cdc:users:{{.Field "id"}}` {
		t.Errorf("string-form table: unexpected key pattern %q", got)
	}

	// Object form carries key + transforms.
	orders := cfg.RedisJSON.Tables["public.orders"]
	if orders.Key != `cdc:orders:{{.Field "id"}}` {
		t.Errorf("object-form table: unexpected key %q", orders.Key)
	}
	if orders.Filter != `new.status != "draft"` {
		t.Errorf("unexpected filter %q", orders.Filter)
	}
	if len(orders.DropColumns) != 1 || orders.DropColumns[0] != "internal_notes" {
		t.Errorf("unexpected drop_columns %v", orders.DropColumns)
	}
	if orders.Mask["card_number"] != "last4" {
		t.Errorf("unexpected mask %v", orders.Mask)
	}

	// Sink-level transform block.
	if cfg.RedisJSON.Transform == nil || cfg.RedisJSON.Transform.Mask["email"] != "sha256" {
		t.Errorf("sink-level transform not parsed: %+v", cfg.RedisJSON.Transform)
	}

	// Meilisearch string and object forms.
	if idx, ok := cfg.Meilisearch.GetIndex("public", "products"); !ok || idx != "products" {
		t.Errorf("string-form index: got %q ok=%v", idx, ok)
	}
	if idx, ok := cfg.Meilisearch.GetIndex("public", "users"); !ok || idx != "users" {
		t.Errorf("object-form index: got %q ok=%v", idx, ok)
	}
	if tt := cfg.Meilisearch.TableTransforms(); len(tt["public.users"].DropColumns) != 1 {
		t.Errorf("meilisearch table transforms not collected: %+v", tt)
	}
}

func TestOutboxConfigParsingAndDefaults(t *testing.T) {
	input := `
outbox:
  table: app.domain_events
  delete_after_publish: true
  columns:
    topic: destination
`
	var cfg SinksConfig
	if err := yamlUnmarshal(t, input, &cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !cfg.Outbox.Enabled() {
		t.Fatal("outbox with a table must be enabled")
	}
	cfg.Outbox.applyDefaults()

	schema, table := cfg.Outbox.SchemaTable()
	if schema != "app" || table != "domain_events" {
		t.Errorf("unexpected schema.table: %s.%s", schema, table)
	}
	if cfg.Outbox.StreamPrefix != "events" {
		t.Errorf("default stream prefix: %q", cfg.Outbox.StreamPrefix)
	}
	if cfg.Outbox.Columns.Topic != "destination" {
		t.Errorf("overridden topic column: %q", cfg.Outbox.Columns.Topic)
	}
	if cfg.Outbox.Columns.Payload != "payload" || cfg.Outbox.Columns.ID != "id" {
		t.Errorf("column defaults not applied: %+v", cfg.Outbox.Columns)
	}
	if !cfg.Outbox.DeleteAfterPublish {
		t.Error("delete_after_publish not parsed")
	}

	// Bare table name defaults to the public schema.
	bare := OutboxConfig{Table: "outbox"}
	if s, tb := bare.SchemaTable(); s != "public" || tb != "outbox" {
		t.Errorf("bare table: %s.%s", s, tb)
	}

	// Absent section stays disabled.
	var empty SinksConfig
	if empty.Outbox.Enabled() {
		t.Error("outbox without a table must be disabled")
	}
}

func TestLoadSinksConfigDefaultsToSyncAll(t *testing.T) {
	t.Setenv("SINK_CONFIG_FILE", "")
	cfg, err := LoadSinksConfig()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !cfg.RedisStream.SyncAll || !cfg.RedisJSON.SyncAll {
		t.Error("without a config file the redis sinks must default to sync_all")
	}
}
