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
	outboxsink "github.com/emoss08/gtc/internal/adapters/secondary/outbox"
	redissink "github.com/emoss08/gtc/internal/adapters/secondary/redis"
	redisjsonsink "github.com/emoss08/gtc/internal/adapters/secondary/redisjson"
	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/emoss08/gtc/internal/core/ports"
	"github.com/emoss08/gtc/internal/core/services"
	"github.com/emoss08/gtc/internal/infrastructure/config"
	"github.com/emoss08/gtc/internal/infrastructure/dlq"
	"github.com/emoss08/gtc/internal/infrastructure/resilience"
	"github.com/emoss08/gtc/internal/infrastructure/server"
	"github.com/emoss08/gtc/internal/infrastructure/transform"
	"go.uber.org/fx"
)

const healthCheckInterval = 10 * time.Second

func Module() fx.Option {
	return fx.Options(
		fx.Provide(
			config.Load,
			config.LoadSinksConfig,
			newBackfillCoordinator,
			newDLQManager,
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
func newBackfillCoordinator(
	cfg *config.Config,
	sinksCfg *config.SinksConfig,
	logger *slog.Logger,
) (*backfill.Coordinator, error) {
	if cfg.Backfill.Mode == "off" {
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	db, err := backfill.NewDB(ctx, cfg.DatabaseURL, cfg.Backfill.StateTable)
	if err != nil {
		return nil, err
	}

	excluded := make(map[string]struct{}, len(cfg.ExcludedTables)+1)
	for t := range cfg.ExcludedTables {
		excluded[t] = struct{}{}
	}
	// Backfilling the outbox would republish historical messages.
	if sinksCfg.Outbox.Enabled() {
		schema, table := sinksCfg.Outbox.SchemaTable()
		excluded[schema+"."+table] = struct{}{}
	}

	return backfill.NewCoordinator(db, backfill.Config{
		SlotName:        cfg.SlotName,
		PublicationName: cfg.PublicationName,
		ChunkSize:       cfg.Backfill.ChunkSize,
		ChunkDelay:      cfg.Backfill.ChunkDelay,
		ExcludedTables:  excluded,
	}, logger), nil
}

// newDLQManager returns nil when the DLQ is disabled or Redis is not
// configured; failures then always stall the pipeline (the safe default).
func newDLQManager(cfg *config.Config, logger *slog.Logger) (*dlq.Manager, error) {
	rc := redissink.LoadConfig()
	if !cfg.DLQ.Enabled || rc.URL == "" {
		if cfg.DLQ.Enabled {
			logger.Info("dead-letter queue unavailable without REDIS_URL; sink failures will stall the pipeline")
		}
		return nil, nil
	}

	store, err := dlq.NewRedisStore(rc.URL, rc.StreamPrefix, cfg.DLQ.MaxEntries)
	if err != nil {
		return nil, err
	}

	return dlq.NewManager(dlq.ManagerParams{
		Store:        store,
		Threshold:    cfg.DLQ.Threshold,
		RetryTimeout: cfg.ProcessTimeout,
		Logger:       logger,
	}), nil
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
	dlqManager *dlq.Manager,
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

	outboxCfg := sinksCfg.Outbox
	outboxSchema, outboxTable := "", ""
	if outboxCfg.Enabled() {
		outboxSchema, outboxTable = outboxCfg.SchemaTable()
	}

	// Layering, innermost out: resilience (retries + breaker), then the
	// DLQ (parks poison events, stores the resilience-wrapped sink as its
	// retry target), then transforms — so retries and parked entries carry
	// the already-transformed event and masking is never re-applied. When
	// an outbox is configured, its table is kept out of the data-mirroring
	// sinks — outbox rows are messages, not data.
	wrapResilienceAndDLQ := func(sink domain.Sink) domain.Sink {
		wrapped := domain.Sink(resilience.NewResilientSink(sink, resCfg))
		if dlqManager != nil {
			wrapped = dlqManager.WrapSink(wrapped)
		}
		return wrapped
	}

	addSink := func(
		sink domain.Sink,
		global *config.TransformSpec,
		tables map[string]config.TransformSpec,
	) error {
		wrapped, err := transform.NewSink(wrapResilienceAndDLQ(sink), global, tables, logger)
		if err != nil {
			return err
		}
		if outboxCfg.Enabled() {
			wrapped = outboxsink.ExcludeTable(wrapped, outboxSchema, outboxTable)
		}
		sinks = append(sinks, wrapped)
		return nil
	}

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
		if err := addSink(sink, sinksCfg.RedisStream.Transform, sinksCfg.RedisStream.TableTransforms()); err != nil {
			return nil, err
		}
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
		if err := addSink(sink, sinksCfg.RedisJSON.Transform, sinksCfg.RedisJSON.TableTransforms()); err != nil {
			return nil, err
		}
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
		if err := addSink(sink, sinksCfg.Meilisearch.Transform, sinksCfg.Meilisearch.TableTransforms()); err != nil {
			return nil, err
		}
	}

	if outboxCfg.Enabled() {
		rc := redissink.LoadConfig()
		sink, err := outboxsink.NewSink(outboxsink.Config{
			Schema:             outboxSchema,
			Table:              outboxTable,
			StreamPrefix:       outboxCfg.StreamPrefix,
			DefaultTopic:       outboxCfg.DefaultTopic,
			DeleteAfterPublish: outboxCfg.DeleteAfterPublish,
			Columns:            outboxCfg.Columns,
			RedisURL:           rc.URL,
			MaxStreamLen:       rc.MaxStreamLen,
			DatabaseURL:        backfill.NonReplicationURL(cfg.DatabaseURL),
		}, logger)
		if err != nil {
			return nil, err
		}
		// No transform wrapper: outbox rows are messages, and filtering or
		// masking them belongs to whoever writes the outbox.
		sinks = append(sinks, wrapResilienceAndDLQ(sink))
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
	dlqManager *dlq.Manager,
	health *server.HealthStatus,
	logger *slog.Logger,
) *server.Server {
	var backfillManager ports.BackfillManager
	if coordinator != nil {
		backfillManager = coordinator
	}
	var dlqPort ports.DLQManager
	if dlqManager != nil {
		dlqPort = dlqManager
	}
	return server.New(server.ServerParams{
		Config:   server.Config{Port: cfg.HTTPPort},
		Checker:  health,
		Backfill: backfillManager,
		DLQ:      dlqPort,
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
