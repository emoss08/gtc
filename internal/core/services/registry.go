package services

import (
	"context"
	"sync"

	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/emoss08/gtc/internal/core/ports"
)

type sinkRegistry struct {
	mu    sync.RWMutex
	sinks map[string]domain.Sink
}

func NewSinkRegistry() ports.SinkRegistry {
	return &sinkRegistry{
		sinks: make(map[string]domain.Sink),
	}
}

type RegistryParams struct {
	Sinks []domain.Sink
}

func NewSinkRegistryWithSinks(params RegistryParams) ports.SinkRegistry {
	registry := &sinkRegistry{
		sinks: make(map[string]domain.Sink),
	}
	for _, sink := range params.Sinks {
		_ = registry.Register(sink)
	}
	return registry
}

func (r *sinkRegistry) Register(sink domain.Sink) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.sinks[sink.Name()]; exists {
		return domain.ErrSinkAlreadyExists
	}

	r.sinks[sink.Name()] = sink
	return nil
}

func (r *sinkRegistry) Unregister(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.sinks[name]; !exists {
		return domain.ErrSinkNotFound
	}

	delete(r.sinks, name)
	return nil
}

func (r *sinkRegistry) Get(name string) (domain.Sink, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	sink, exists := r.sinks[name]
	if !exists {
		return nil, domain.ErrSinkNotFound
	}

	return sink, nil
}

func (r *sinkRegistry) All() []domain.Sink {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]domain.Sink, 0, len(r.sinks))
	for _, sink := range r.sinks {
		result = append(result, sink)
	}
	return result
}

func (r *sinkRegistry) InitializeAll(ctx context.Context) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, sink := range r.sinks {
		if err := sink.Initialize(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (r *sinkRegistry) ShutdownAll(ctx context.Context) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var lastErr error
	for _, sink := range r.sinks {
		if err := sink.Shutdown(ctx); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (r *sinkRegistry) HealthCheckAll(ctx context.Context) map[string]error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	results := make(map[string]error)
	for name, sink := range r.sinks {
		results[name] = sink.HealthCheck(ctx)
	}
	return results
}
