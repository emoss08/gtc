package server

import (
	"encoding/json"
	"net/http"
	"sort"
	"time"

	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// WALInfo exposes the reader state the dashboard needs.
type WALInfo interface {
	Streaming() bool
	CurrentLSN() string
}

type sinkStats struct {
	Name         string             `json:"name"`
	Healthy      bool               `json:"healthy"`
	BreakerState float64            `json:"breaker_state"` // 0 closed, 1 half-open, 2 open
	Succeeded    float64            `json:"succeeded"`
	Failed       float64            `json:"failed"`
	Filtered     float64            `json:"filtered"`
	Retries      float64            `json:"retries"`
	ErrorsByType map[string]float64 `json:"errors_by_type"`
}

type tableStats struct {
	Table    string  `json:"table"`
	Insert   float64 `json:"insert"`
	Update   float64 `json:"update"`
	Delete   float64 `json:"delete"`
	Read     float64 `json:"read"`
	Truncate float64 `json:"truncate"`
	Total    float64 `json:"total"`
}

type statsResponse struct {
	UptimeSeconds float64                      `json:"uptime_seconds"`
	Version       string                       `json:"version"`
	SlotName      string                       `json:"slot_name"`
	Publication   string                       `json:"publication"`
	Ready         bool                         `json:"ready"`
	Streaming     bool                         `json:"streaming"`
	CurrentLSN    string                       `json:"current_lsn"`
	WALLagBytes   float64                      `json:"wal_lag_bytes"`
	Inflight      float64                      `json:"inflight"`
	EventsTotal   float64                      `json:"events_total"`
	Operations    map[string]float64           `json:"operations"`
	Sinks         []sinkStats                  `json:"sinks"`
	Tables        []tableStats                 `json:"tables"`
	Backfill      []domain.BackfillTableStatus `json:"backfill"`
	DLQ           dlqStats                     `json:"dlq"`
}

type dlqStats struct {
	Enabled      bool    `json:"enabled"`
	Entries      int64   `json:"entries"`
	ParkedTotal  float64 `json:"parked_total"`
	RetriedTotal float64 `json:"retried_total"`
}

// handleStats shapes live state (reader, health, backfill, DLQ) plus the
// process's own Prometheus counters into one JSON document for the dashboard.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	resp := statsResponse{
		UptimeSeconds: time.Since(s.startedAt).Seconds(),
		Version:       s.info.Version,
		SlotName:      s.info.SlotName,
		Publication:   s.info.Publication,
		Operations:    map[string]float64{},
		Sinks:         []sinkStats{},
		Tables:        []tableStats{},
		Backfill:      []domain.BackfillTableStatus{},
	}

	if s.checker != nil {
		resp.Ready = s.checker.IsReady()
	}
	if s.wal != nil {
		resp.Streaming = s.wal.Streaming()
		resp.CurrentLSN = s.wal.CurrentLSN()
	}
	if s.backfill != nil {
		if status := s.backfill.Status(); status != nil {
			resp.Backfill = status
		}
	}
	if s.dlq != nil {
		resp.DLQ.Enabled = true
		if n, err := s.dlq.Len(r.Context()); err == nil {
			resp.DLQ.Entries = n
		}
	}

	sinks := make(map[string]*sinkStats)
	sink := func(name string) *sinkStats {
		if st, ok := sinks[name]; ok {
			return st
		}
		st := &sinkStats{Name: name}
		sinks[name] = st
		return st
	}
	if s.checker != nil {
		for name, healthy := range s.checker.SinkStatuses() {
			sink(name).Healthy = healthy
		}
	}

	tables := make(map[string]*tableStats)

	families, err := prometheus.DefaultGatherer.Gather()
	if err == nil {
		for _, fam := range families {
			switch fam.GetName() {
			case "cdc_wal_lag_bytes":
				resp.WALLagBytes = singleValue(fam)
			case "cdc_inflight_events":
				resp.Inflight = singleValue(fam)
			case "cdc_events_received_total":
				for _, m := range fam.GetMetric() {
					labels := labelMap(m)
					name := labels["schema"] + "." + labels["table"]
					ts, ok := tables[name]
					if !ok {
						ts = &tableStats{Table: name}
						tables[name] = ts
					}
					v := metricValue(m)
					ts.Total += v
					op := labels["operation"]
					resp.Operations[op] += v
					switch op {
					case "INSERT":
						ts.Insert += v
					case "UPDATE":
						ts.Update += v
					case "DELETE":
						ts.Delete += v
					case "READ":
						ts.Read += v
					case "TRUNCATE":
						ts.Truncate += v
					}
					resp.EventsTotal += v
				}
			case "cdc_events_processed_total":
				for _, m := range fam.GetMetric() {
					labels := labelMap(m)
					st := sink(labels["sink"])
					if labels["status"] == "success" {
						st.Succeeded += metricValue(m)
					} else {
						st.Failed += metricValue(m)
					}
				}
			case "cdc_events_filtered_total":
				for _, m := range fam.GetMetric() {
					sink(labelMap(m)["sink"]).Filtered += metricValue(m)
				}
			case "cdc_retry_attempts_total":
				for _, m := range fam.GetMetric() {
					sink(labelMap(m)["sink"]).Retries += metricValue(m)
				}
			case "cdc_circuit_breaker_state":
				for _, m := range fam.GetMetric() {
					sink(labelMap(m)["sink"]).BreakerState = metricValue(m)
				}
			case "cdc_sink_errors_total":
				for _, m := range fam.GetMetric() {
					labels := labelMap(m)
					st := sink(labels["sink"])
					if st.ErrorsByType == nil {
						st.ErrorsByType = map[string]float64{}
					}
					st.ErrorsByType[labels["error_type"]] += metricValue(m)
				}
			case "cdc_dlq_parked_total":
				for _, m := range fam.GetMetric() {
					resp.DLQ.ParkedTotal += metricValue(m)
				}
			case "cdc_dlq_retried_total":
				for _, m := range fam.GetMetric() {
					resp.DLQ.RetriedTotal += metricValue(m)
				}
			}
		}
	}

	for _, st := range sinks {
		resp.Sinks = append(resp.Sinks, *st)
	}
	sort.Slice(resp.Sinks, func(i, j int) bool { return resp.Sinks[i].Name < resp.Sinks[j].Name })

	for _, ts := range tables {
		resp.Tables = append(resp.Tables, *ts)
	}
	sort.Slice(resp.Tables, func(i, j int) bool {
		if resp.Tables[i].Total != resp.Tables[j].Total {
			return resp.Tables[i].Total > resp.Tables[j].Total
		}
		return resp.Tables[i].Table < resp.Tables[j].Table
	})

	_ = json.NewEncoder(w).Encode(resp)
}

func labelMap(m *dto.Metric) map[string]string {
	out := make(map[string]string, len(m.GetLabel()))
	for _, l := range m.GetLabel() {
		out[l.GetName()] = l.GetValue()
	}
	return out
}

func metricValue(m *dto.Metric) float64 {
	if c := m.GetCounter(); c != nil {
		return c.GetValue()
	}
	if g := m.GetGauge(); g != nil {
		return g.GetValue()
	}
	return 0
}

func singleValue(fam *dto.MetricFamily) float64 {
	metrics := fam.GetMetric()
	if len(metrics) == 0 {
		return 0
	}
	return metricValue(metrics[0])
}
