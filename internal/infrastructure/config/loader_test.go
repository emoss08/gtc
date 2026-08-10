package config

import (
	"testing"
	"time"
)

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
		Tables: map[string]string{
			"public.orders": "orders-pattern",
			"events":        "events-pattern",
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
