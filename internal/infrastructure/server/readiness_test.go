package server

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/emoss08/gtc/internal/core/domain"
)

type fakeStalls []domain.SinkStall

func (f fakeStalls) Stalls() []domain.SinkStall { return f }

func readinessOf(t *testing.T, health *HealthStatus, stalls fakeStalls) (int, readinessResponse) {
	t.Helper()
	srv := New(ServerParams{
		Config:  Config{Port: 0},
		Checker: health,
		WAL:     fakeWAL{},
		Stalls:  stalls,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	rec := httptest.NewRecorder()
	srv.router.ServeHTTP(rec, httptest.NewRequest("GET", "/readiness", nil))

	var resp readinessResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON %q: %v", rec.Body.String(), err)
	}
	return rec.Code, resp
}

func TestReadinessReflectsStalledSink(t *testing.T) {
	health := NewHealthStatus()
	health.SetReady(true)
	health.UpdateSinkStatus(map[string]bool{"meilisearch": true, "redis-json": true})

	code, resp := readinessOf(t, health, nil)
	if code != http.StatusOK || resp.Status != "ready" {
		t.Fatalf("healthy and not stalled must be ready, got %d %+v", code, resp)
	}

	stall := domain.SinkStall{
		Sink:     "meilisearch",
		Since:    time.Now().Add(-time.Minute).UTC(),
		Attempts: 12,
		Error:    "dial tcp: connection refused",
	}
	code, resp = readinessOf(t, health, fakeStalls{stall})
	if code != http.StatusServiceUnavailable || resp.Status != "not_ready" {
		t.Fatalf("a stalled sink must make GTC not ready, got %d %+v", code, resp)
	}
	if len(resp.StalledSinks) != 1 || resp.StalledSinks[0].Sink != "meilisearch" ||
		resp.StalledSinks[0].Attempts != 12 {
		t.Errorf("readiness must name the stalled sink, got %+v", resp.StalledSinks)
	}
}

func TestReadinessNamesUnhealthySinks(t *testing.T) {
	health := NewHealthStatus()
	health.SetReady(false)
	health.UpdateSinkStatus(map[string]bool{"meilisearch": false, "redis-json": true})

	code, resp := readinessOf(t, health, nil)
	if code != http.StatusServiceUnavailable || resp.Status != "not_ready" {
		t.Fatalf("an unhealthy sink must make GTC not ready, got %d %+v", code, resp)
	}
	if len(resp.UnhealthySinks) != 1 || resp.UnhealthySinks[0] != "meilisearch" {
		t.Errorf("readiness must name the unhealthy sink, got %v", resp.UnhealthySinks)
	}
}
