package server

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// sample is one point of dashboard history. Counters are cumulative; the UI
// derives rates from deltas so the server stays stateless about windows.
type sample struct {
	T           int64   `json:"t"` // unix milliseconds
	EventsTotal float64 `json:"events_total"`
	WALLagBytes float64 `json:"wal_lag_bytes"`
	Inflight    float64 `json:"inflight"`
	DLQEntries  float64 `json:"dlq_entries"`
}

const (
	samplerInterval = 5 * time.Second
	samplerCapacity = 360 // 30 minutes at 5s
)

// sampler keeps a ring buffer of pipeline history so charts have real data
// the moment the dashboard opens, surviving page reloads.
type sampler struct {
	mu      sync.Mutex
	samples []sample
	stop    chan struct{}
	once    sync.Once
}

func newSampler() *sampler {
	return &sampler{
		samples: make([]sample, 0, samplerCapacity),
		stop:    make(chan struct{}),
	}
}

func (s *sampler) run() {
	ticker := time.NewTicker(samplerInterval)
	defer ticker.Stop()

	s.collect()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			s.collect()
		}
	}
}

func (s *sampler) close() {
	s.once.Do(func() { close(s.stop) })
}

func (s *sampler) collect() {
	point := sample{T: time.Now().UnixMilli()}

	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return
	}
	for _, fam := range families {
		switch fam.GetName() {
		case "cdc_events_received_total":
			for _, m := range fam.GetMetric() {
				point.EventsTotal += metricValue(m)
			}
		case "cdc_wal_lag_bytes":
			point.WALLagBytes = singleValue(fam)
		case "cdc_inflight_events":
			point.Inflight = singleValue(fam)
		case "cdc_dlq_entries":
			point.DLQEntries = singleValue(fam)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.samples = append(s.samples, point)
	if len(s.samples) > samplerCapacity {
		s.samples = s.samples[len(s.samples)-samplerCapacity:]
	}
}

func (s *sampler) snapshot() []sample {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]sample, len(s.samples))
	copy(out, s.samples)
	return out
}

func (s *Server) handleHistory(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"interval_seconds": samplerInterval.Seconds(),
		"samples":          s.sampler.snapshot(),
	})
}
