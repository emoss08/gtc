package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/emoss08/gtc/internal/adapters/primary/backfill"
	"github.com/emoss08/gtc/internal/adapters/primary/wal"
	meilisink "github.com/emoss08/gtc/internal/adapters/secondary/meilisearch"
	redissink "github.com/emoss08/gtc/internal/adapters/secondary/redis"
	redisjsonsink "github.com/emoss08/gtc/internal/adapters/secondary/redisjson"
	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/emoss08/gtc/internal/core/ports"
	"github.com/emoss08/gtc/internal/core/services"
	"github.com/emoss08/gtc/internal/infrastructure/config"
	"github.com/emoss08/gtc/internal/infrastructure/resilience"
	"github.com/emoss08/gtc/internal/infrastructure/server"
	"go.uber.org/fx"
)

const healthCheckInterval = 10 * time.Second

func Module() fx.Option {
	return fx.Options(
		fx.Provide(
			config.Load,
			config.LoadSinksConfig,
			newBackfillCoordinator,
			newWALReader,
			func(r *wal.Reader) ports.WALReader { return r },
			newSinks,
			newSinkRegistry,
			newCDCService,
			server.NewHealthStatus,
			newHTTPServer,
		),
		fx.Invoke(run),
	)
}

// newBackfillCoordinator returns nil when backfill is disabled.
func newBackfillCoordinator(cfg *config.Config, logger *slog.Logger) (*backfill.Coordinator, error) {
	if cfg.Backfill.Mode == "off" {
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	db, err := backfill.NewDB(ctx, cfg.DatabaseURL, cfg.Backfill.StateTable)
	if err != nil {
		return nil, err
	}

	return backfill.NewCoordinator(db, backfill.Config{
		SlotName:        cfg.SlotName,
		PublicationName: cfg.PublicationName,
		ChunkSize:       cfg.Backfill.ChunkSize,
		ChunkDelay:      cfg.Backfill.ChunkDelay,
		ExcludedTables:  cfg.ExcludedTables,
	}, logger), nil
}

func newWALReader(
	cfg *config.Config,
	coordinator *backfill.Coordinator,
	logger *slog.Logger,
) *wal.Reader {
	walCfg := wal.Config{
		DatabaseURL:       cfg.DatabaseURL,
		SlotName:          cfg.SlotName,
		PublicationName:   cfg.PublicationName,
		StandbyTimeout:    cfg.StandbyTimeout,
		AutoCreateSlot:    cfg.AutoCreateSlot,
		AutoCreatePub:     cfg.AutoCreatePub,
		SlotRetryInterval: cfg.SlotRetryInterval,
		SlotRetryTimeout:  cfg.SlotRetryTimeout,
	}

	if coordinator != nil {
		walCfg.Controller = coordinator
		if cfg.Backfill.Mode == "auto" {
			// A freshly created slot means a brand-new deployment whose
			// sinks are empty: backfill everything, once.
			var once sync.Once
			walCfg.StreamStarted = func(slotCreated bool) {
				if !slotCreated {
					return
				}
				once.Do(func() {
					go func() {
						if err := coordinator.EnqueueAll(context.Background()); err != nil {
							logger.Error("failed to enqueue initial backfill",
								slog.String("error", err.Error()))
						}
					}()
				})
			}
		}
	}

	return wal.NewReader(walCfg, logger)
}

// newSinks builds every sink whose backing store is configured (presence of
// REDIS_URL, REDIS_JSON_URL, MEILISEARCH_URL), each wrapped with retry and
// circuit-breaker resilience.
func newSinks(
	cfg *config.Config,
	sinksCfg *config.SinksConfig,
	logger *slog.Logger,
) ([]domain.Sink, error) {
	resCfg := resilience.ResilienceConfig{
		MaxRetries:          cfg.Resilience.MaxRetries,
		InitialBackoff:      cfg.Resilience.RetryBackoffInitial,
		MaxBackoff:          cfg.Resilience.RetryBackoffMax,
		CircuitOpenDuration: cfg.Resilience.CircuitBreakerTimeout,
		FailureThreshold:    cfg.Resilience.CircuitBreakerThreshold,
	}

	var sinks []domain.Sink

	if rc := redissink.LoadConfig(); rc.Enabled {
		resolver, err := redissink.NewKeyResolver(redissink.KeyResolverParams{
			Resolver:       &sinksCfg.RedisStream,
			Filter:         &sinksCfg.RedisStream,
			Prefix:         rc.StreamPrefix,
			DefaultPattern: redissink.DefaultStreamKeyPattern,
		})
		if err != nil {
			return nil, err
		}
		sink, err := redissink.NewSink(redissink.StreamSinkParams{
			Config:      rc,
			KeyResolver: resolver,
			Logger:      logger,
		})
		if err != nil {
			return nil, err
		}
		sinks = append(sinks, resilience.NewResilientSink(sink, resCfg))
	}

	if jc := redisjsonsink.LoadConfig(); jc.Enabled {
		resolver, err := redissink.NewKeyResolver(redissink.KeyResolverParams{
			Resolver:       &sinksCfg.RedisJSON,
			Filter:         &sinksCfg.RedisJSON,
			Prefix:         jc.Prefix,
			DefaultPattern: redissink.DefaultJSONKeyPattern,
		})
		if err != nil {
			return nil, err
		}
		sink, err := redisjsonsink.NewSink(redisjsonsink.JSONSinkParams{
			Config:      jc,
			KeyResolver: resolver,
			Logger:      logger,
		})
		if err != nil {
			return nil, err
		}
		sinks = append(sinks, resilience.NewResilientSink(sink, resCfg))
	}

	if mc := meilisink.LoadConfig(); mc.Enabled {
		mapper := meilisink.NewTableMapper(meilisink.TableMapperParams{
			Resolver: &sinksCfg.Meilisearch,
		})
		sink := meilisink.NewSink(meilisink.SinkParams{
			Config: mc,
			Mapper: mapper,
			Logger: logger,
		})
		sinks = append(sinks, resilience.NewResilientSink(sink, resCfg))
	}

	return sinks, nil
}

func newSinkRegistry(sinks []domain.Sink) ports.SinkRegistry {
	return services.NewSinkRegistryWithSinks(services.RegistryParams{Sinks: sinks})
}

func newCDCService(
	cfg *config.Config,
	reader ports.WALReader,
	registry ports.SinkRegistry,
	logger *slog.Logger,
) ports.CDCService {
	// The backfill progress table lives in the source database and churns
	// constantly during a backfill; never mirror it to sinks.
	excluded := make(map[string]struct{}, len(cfg.ExcludedTables)+1)
	for t := range cfg.ExcludedTables {
		excluded[t] = struct{}{}
	}
	if cfg.Backfill.Mode != "off" && cfg.Backfill.StateTable != "" {
		excluded[cfg.Backfill.StateTable] = struct{}{}
	}

	return services.NewCDCService(services.CDCServiceParams{
		WALReader: reader,
		Registry:  registry,
		Logger:    logger,
		Config: services.CDCServiceConfig{
			ParallelSinks:  cfg.ParallelSinks,
			ProcessTimeout: cfg.ProcessTimeout,
			ExcludedTables: excluded,
		},
	})
}

func newHTTPServer(
	cfg *config.Config,
	coordinator *backfill.Coordinator,
	health *server.HealthStatus,
	logger *slog.Logger,
) *server.Server {
	var manager ports.BackfillManager
	if coordinator != nil {
		manager = coordinator
	}
	return server.New(server.ServerParams{
		Config:   server.Config{Port: cfg.HTTPPort},
		Checker:  health,
		Backfill: manager,
		Logger:   logger,
	})
}

func run(
	lc fx.Lifecycle,
	shutdowner fx.Shutdowner,
	svc ports.CDCService,
	srv *server.Server,
	health *server.HealthStatus,
	registry ports.SinkRegistry,
	reader *wal.Reader,
	coordinator *backfill.Coordinator,
	logger *slog.Logger,
) {
	runCtx, cancel := context.WithCancel(context.Background())
	var stopMonitor func()

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			if coordinator != nil {
				coordinator.Start(runCtx)
			}

			go func() {
				if err := svc.Start(runCtx); err != nil && !errors.Is(err, context.Canceled) {
					logger.Error("CDC service exited", slog.String("error", err.Error()))
					_ = shutdowner.Shutdown()
				}
			}()

			go func() {
				if err := srv.Start(); err != nil {
					logger.Error("HTTP server exited", slog.String("error", err.Error()))
					_ = shutdowner.Shutdown()
				}
			}()

			stopMonitor = server.StartHealthMonitor(
				health,
				registry,
				reader.Streaming,
				healthCheckInterval,
			)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if stopMonitor != nil {
				stopMonitor()
			}
			cancel()

			err := svc.Stop(ctx)
			if srvErr := srv.Stop(ctx); srvErr != nil {
				logger.Error("failed to stop HTTP server", slog.String("error", srvErr.Error()))
			}
			if coordinator != nil {
				coordinator.Close()
			}
			return err
		},
	})
}
