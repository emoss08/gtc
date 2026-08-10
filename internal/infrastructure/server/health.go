package server

import (
	"context"
	"sync/atomic"
	"time"
)

type HealthStatus struct {
	ready      atomic.Bool
	sinkStatus atomic.Value
}

func NewHealthStatus() *HealthStatus {
	h := &HealthStatus{}
	h.sinkStatus.Store(make(map[string]bool))
	return h
}

func (h *HealthStatus) SetReady(ready bool) {
	h.ready.Store(ready)
}

func (h *HealthStatus) IsReady() bool {
	return h.ready.Load()
}

func (h *HealthStatus) UpdateSinkStatus(statuses map[string]bool) {
	h.sinkStatus.Store(statuses)
}

func (h *HealthStatus) SinkStatuses() map[string]bool {
	return h.sinkStatus.Load().(map[string]bool)
}

type SinkHealthChecker interface {
	HealthCheckAll(ctx context.Context) map[string]error
}

// StartHealthMonitor periodically probes the sinks and the WAL stream and
// updates readiness: ready means the replication stream is live and every
// registered sink is healthy. The returned function stops the monitor.
func StartHealthMonitor(
	status *HealthStatus,
	checker SinkHealthChecker,
	walHealthy func() bool,
	interval time.Duration,
) func() {
	done := make(chan struct{})

	check := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		results := checker.HealthCheckAll(ctx)
		cancel()

		statuses := make(map[string]bool)
		allHealthy := true
		for name, err := range results {
			healthy := err == nil
			statuses[name] = healthy
			if !healthy {
				allHealthy = false
			}
		}

		status.UpdateSinkStatus(statuses)
		status.SetReady(allHealthy && walHealthy())
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		check()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				check()
			}
		}
	}()

	return func() {
		close(done)
	}
}
