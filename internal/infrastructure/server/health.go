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

func StartHealthMonitor(
	status *HealthStatus,
	checker SinkHealthChecker,
	interval time.Duration,
) func() {
	done := make(chan struct{})

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
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
				status.SetReady(allHealthy && len(results) > 0)
			}
		}
	}()

	return func() {
		close(done)
	}
}
