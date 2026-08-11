package server

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/emoss08/gtc/internal/infrastructure/metrics"
)

type fakeWAL struct{}

func (fakeWAL) Streaming() bool    { return true }
func (fakeWAL) CurrentLSN() string { return "0/ABC" }

func TestStatsEndpointShapesMetrics(t *testing.T) {
	// Package-level prometheus collectors are shared; use distinct labels.
	metrics.EventsReceived.WithLabelValues("statstest", "users", "INSERT").Add(3)
	metrics.EventsReceived.WithLabelValues("statstest", "users", "UPDATE").Add(2)
	metrics.EventsProcessed.WithLabelValues("statstest-sink", "success").Add(4)
	metrics.EventsProcessed.WithLabelValues("statstest-sink", "failure").Add(1)
	metrics.SinkErrors.WithLabelValues("statstest-sink", "timeout").Add(2)

	health := NewHealthStatus()
	health.SetReady(true)
	health.UpdateSinkStatus(map[string]bool{"statstest-sink": true})

	srv := New(ServerParams{
		Config:  Config{Port: 0},
		Checker: health,
		WAL:     fakeWAL{},
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		Info:    InstanceInfo{Version: "1.2.3-test", SlotName: "test_slot", Publication: "test_pub"},
	})
	srv.startedAt = time.Now().Add(-90 * time.Second)

	rec := httptest.NewRecorder()
	srv.router.ServeHTTP(rec, httptest.NewRequest("GET", "/api/stats", nil))

	if rec.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp statsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if !resp.Streaming || resp.CurrentLSN != "0/ABC" || !resp.Ready {
		t.Errorf("WAL/readiness state not reflected: %+v", resp)
	}
	if resp.UptimeSeconds < 89 {
		t.Errorf("uptime not tracked: %f", resp.UptimeSeconds)
	}
	if resp.Version != "1.2.3-test" || resp.SlotName != "test_slot" || resp.Publication != "test_pub" {
		t.Errorf("instance info not surfaced: version=%q slot=%q publication=%q",
			resp.Version, resp.SlotName, resp.Publication)
	}

	// Other tests share the registry, so only assert lower bounds on the
	// aggregated operations map.
	if resp.Operations["INSERT"] < 3 || resp.Operations["UPDATE"] < 2 {
		t.Errorf("operations map wrong: %+v", resp.Operations)
	}

	var table *tableStats
	for i := range resp.Tables {
		if resp.Tables[i].Table == "statstest.users" {
			table = &resp.Tables[i]
		}
	}
	if table == nil {
		t.Fatalf("expected statstest.users in tables: %+v", resp.Tables)
	}
	if table.Insert != 3 || table.Update != 2 || table.Total != 5 {
		t.Errorf("per-operation counts wrong: %+v", *table)
	}

	var sink *sinkStats
	for i := range resp.Sinks {
		if resp.Sinks[i].Name == "statstest-sink" {
			sink = &resp.Sinks[i]
		}
	}
	if sink == nil {
		t.Fatalf("expected statstest-sink in sinks: %+v", resp.Sinks)
	}
	if sink.Succeeded != 4 || sink.Failed != 1 || !sink.Healthy {
		t.Errorf("sink stats wrong: %+v", *sink)
	}
	if sink.ErrorsByType["timeout"] != 2 {
		t.Errorf("errors_by_type not surfaced: %+v", sink.ErrorsByType)
	}
}

func TestHistoryEndpointReturnsSamples(t *testing.T) {
	srv := New(ServerParams{
		Config: Config{Port: 0},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	// Drive the sampler directly instead of starting its goroutine.
	srv.sampler.collect()
	srv.sampler.collect()

	rec := httptest.NewRecorder()
	srv.router.ServeHTTP(rec, httptest.NewRequest("GET", "/api/history", nil))

	if rec.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		IntervalSeconds float64  `json:"interval_seconds"`
		Samples         []sample `json:"samples"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if resp.IntervalSeconds != samplerInterval.Seconds() {
		t.Errorf("interval_seconds = %f, want %f", resp.IntervalSeconds, samplerInterval.Seconds())
	}
	if len(resp.Samples) != 2 {
		t.Fatalf("expected 2 samples, got %d", len(resp.Samples))
	}
	// The registry already has events from other tests in this package, so
	// the gathered counter must be positive and non-decreasing.
	if resp.Samples[0].T == 0 || resp.Samples[1].T < resp.Samples[0].T {
		t.Errorf("sample timestamps wrong: %+v", resp.Samples)
	}
	if resp.Samples[1].EventsTotal < resp.Samples[0].EventsTotal {
		t.Errorf("events_total must be non-decreasing: %+v", resp.Samples)
	}
}

func TestSamplerCapsRingBuffer(t *testing.T) {
	s := newSampler()
	for range samplerCapacity + 10 {
		s.collect()
	}
	if got := len(s.snapshot()); got != samplerCapacity {
		t.Errorf("ring buffer not capped: len=%d, want %d", got, samplerCapacity)
	}
}

func TestStatsEndpointHandlesNilDependencies(t *testing.T) {
	srv := New(ServerParams{
		Config: Config{Port: 0},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	rec := httptest.NewRecorder()
	srv.router.ServeHTTP(rec, httptest.NewRequest("GET", "/api/stats", nil))

	if rec.Code != 200 {
		t.Fatalf("stats must degrade gracefully with nil deps, got %d", rec.Code)
	}
	var resp statsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if resp.DLQ.Enabled {
		t.Error("DLQ must report disabled when nil")
	}
}
