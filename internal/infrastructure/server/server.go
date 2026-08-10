package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/emoss08/gtc/internal/core/ports"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server struct {
	router     *chi.Mux
	httpServer *http.Server
	logger     *slog.Logger
	checker    HealthChecker
	backfill   ports.BackfillManager
}

type HealthChecker interface {
	IsReady() bool
	SinkStatuses() map[string]bool
}

type Config struct {
	Port int
}

type ServerParams struct {
	Config   Config
	Checker  HealthChecker
	Backfill ports.BackfillManager // nil when backfill is disabled
	Logger   *slog.Logger
}

func New(p ServerParams) *Server {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(30 * time.Second))

	s := &Server{
		router:   r,
		logger:   p.Logger.With(slog.String("component", "http_server")),
		checker:  p.Checker,
		backfill: p.Backfill,
		httpServer: &http.Server{
			Addr:         fmt.Sprintf(":%d", p.Config.Port),
			Handler:      r,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
			IdleTimeout:  60 * time.Second,
		},
	}

	s.registerRoutes()
	return s
}

func (s *Server) registerRoutes() {
	s.router.Get("/health", s.handleHealth)
	s.router.Get("/readiness", s.handleReadiness)
	s.router.Handle("/metrics", promhttp.Handler())
	s.router.Get("/backfill", s.handleBackfillStatus)
	s.router.Post("/backfill", s.handleBackfillTrigger)
}

func (s *Server) handleBackfillStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if s.backfill == nil {
		w.WriteHeader(http.StatusNotImplemented)
		_, _ = w.Write([]byte(`{"error":"backfill is disabled"}`))
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"tables": s.backfill.Status()})
}

func (s *Server) handleBackfillTrigger(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if s.backfill == nil {
		w.WriteHeader(http.StatusNotImplemented)
		_, _ = w.Write([]byte(`{"error":"backfill is disabled"}`))
		return
	}

	var req struct {
		Table string `json:"table"`
		All   bool   `json:"all"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid JSON body"}`))
		return
	}

	switch {
	case req.All:
		if err := s.backfill.EnqueueAll(r.Context()); err != nil {
			s.writeBackfillError(w, err)
			return
		}
	case req.Table != "":
		schema, table := "public", req.Table
		if idx := strings.IndexByte(req.Table, '.'); idx > 0 {
			schema, table = req.Table[:idx], req.Table[idx+1:]
		}
		if err := s.backfill.EnqueueTable(schema, table); err != nil {
			s.writeBackfillError(w, err)
			return
		}
	default:
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"provide {\"table\":\"schema.table\"} or {\"all\":true}"}`))
		return
	}

	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"status":"enqueued"}`))
}

func (s *Server) writeBackfillError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusConflict)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"healthy"}`))
}

func (s *Server) handleReadiness(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if s.checker == nil || !s.checker.IsReady() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"not_ready"}`))
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ready"}`))
}

func (s *Server) Start() error {
	s.logger.Info("starting HTTP server", slog.String("addr", s.httpServer.Addr))
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info("stopping HTTP server")
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) Router() *chi.Mux {
	return s.router
}
