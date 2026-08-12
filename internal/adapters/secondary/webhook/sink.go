// Package webhook delivers change events to an HTTP endpoint, one POST per
// event, so anything that can serve a URL — a serverless function, an
// internal service, a third-party integration — can consume the stream
// without a Redis or search dependency.
package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/bytedance/sonic"

	"github.com/emoss08/gtc/internal/core/domain"
)

// Signature and correlation headers sent with every delivery.
const (
	HeaderSignature = "X-GTC-Signature"
	HeaderTimestamp = "X-GTC-Timestamp"
	HeaderEventID   = "X-GTC-Event-Id"
	HeaderTable     = "X-GTC-Table"
	HeaderOperation = "X-GTC-Operation"
)

// errorBodyLimit bounds how much of a failing response is quoted back in the
// error, so a receiver returning an HTML page cannot flood the logs.
const errorBodyLimit = 512

// TableFilter decides which tables this sink receives. It is satisfied by the
// shared key resolver without coupling this package to Redis.
type TableFilter interface {
	ShouldProcess(schema, table string) bool
}

type Sink struct {
	client *http.Client
	config Config
	filter TableFilter
	logger *slog.Logger
}

var _ domain.Sink = (*Sink)(nil)

type SinkParams struct {
	Config Config
	Filter TableFilter
	Logger *slog.Logger
}

func NewSink(p SinkParams) *Sink {
	return &Sink{
		client: &http.Client{
			Timeout: p.Config.Timeout,
			Transport: &http.Transport{
				MaxIdleConns:        p.Config.MaxIdleConns,
				MaxIdleConnsPerHost: p.Config.MaxIdleConns,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		config: p.Config,
		filter: p.Filter,
		logger: p.Logger.With(slog.String("component", "webhook")),
	}
}

func (s *Sink) Name() string { return "webhook" }

func (s *Sink) Initialize(context.Context) error {
	s.logger.Info("sink initialized",
		slog.String("url", s.config.URL),
		slog.Bool("signed", s.config.SigningSecret != ""),
	)
	return nil
}

func (s *Sink) Shutdown(context.Context) error {
	s.client.CloseIdleConnections()
	return nil
}

// HealthCheck is a no-op: probing the endpoint would deliver traffic a
// receiver did not ask for, and delivery failures already surface through
// the circuit breaker.
func (s *Sink) HealthCheck(context.Context) error { return nil }

// payload mirrors the Redis stream sink's shape so a consumer can be moved
// between transports without changing how it parses events.
type payload struct {
	EventID               string         `json:"event_id"`
	Operation             string         `json:"operation"`
	Schema                string         `json:"schema"`
	Table                 string         `json:"table"`
	OldData               map[string]any `json:"old_data"`
	NewData               map[string]any `json:"new_data"`
	UnchangedToastColumns []string       `json:"unchanged_toast_columns"`
	Metadata              metadata       `json:"metadata"`
}

type metadata struct {
	LSN           string    `json:"lsn"`
	TransactionID uint32    `json:"transaction_id"`
	Timestamp     time.Time `json:"timestamp"`
}

func (s *Sink) Process(ctx context.Context, event domain.CDCEvent) error {
	if s.filter != nil && !s.filter.ShouldProcess(event.Schema, event.Table) {
		s.logger.Debug("skipping event, table not configured",
			slog.String("table", event.FullTableName()),
			slog.String("event_id", event.ID),
		)
		return nil
	}

	body, err := sonic.Marshal(payload{
		EventID:               event.ID,
		Operation:             event.Operation.String(),
		Schema:                event.Schema,
		Table:                 event.Table,
		OldData:               event.OldData,
		NewData:               event.NewData,
		UnchangedToastColumns: event.UnchangedToastColumns,
		Metadata: metadata{
			LSN:           event.Metadata.LSN,
			TransactionID: event.Metadata.TransactionID,
			Timestamp:     event.Metadata.Timestamp,
		},
	})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.config.URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "gtc")
	// Delivery is at-least-once, so receivers need a stable key to dedupe on.
	req.Header.Set(HeaderEventID, event.ID)
	req.Header.Set(HeaderTable, event.FullTableName())
	req.Header.Set(HeaderOperation, event.Operation.String())
	if s.config.AuthHeader != "" {
		req.Header.Set("Authorization", s.config.AuthHeader)
	}
	if s.config.SigningSecret != "" {
		timestamp := strconv.FormatInt(time.Now().Unix(), 10)
		req.Header.Set(HeaderTimestamp, timestamp)
		req.Header.Set(HeaderSignature, Sign(s.config.SigningSecret, timestamp, body))
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("post event %s: %w", event.ID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, errorBodyLimit))
		return fmt.Errorf("webhook returned %s for event %s: %s",
			resp.Status, event.ID, bytes.TrimSpace(detail))
	}
	// Drain so the connection can be reused for the next event.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, errorBodyLimit))

	s.logger.Debug("event delivered",
		slog.String("event_id", event.ID),
		slog.String("table", event.FullTableName()),
		slog.Int("status", resp.StatusCode),
	)
	return nil
}

// Sign returns the value of the X-GTC-Signature header: an HMAC-SHA256 over
// "<timestamp>.<body>", hex encoded. Binding the timestamp into the signature
// lets receivers reject replayed deliveries.
func Sign(secret, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp))
	mac.Write([]byte("."))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}
