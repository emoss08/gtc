package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/joho/godotenv"
	"go.uber.org/fx"

	"github.com/emoss08/gtc/internal/infrastructure/doctor"
)

// Overridden at release time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	for _, arg := range os.Args[1:] {
		if arg == "--version" || arg == "-v" {
			fmt.Printf("gtc %s\n", version)
			return
		}
	}

	_ = godotenv.Load()

	// `gtc doctor` checks prerequisites (PostgreSQL settings, privileges,
	// sink reachability) and exits instead of starting the pipeline.
	if len(os.Args) > 1 && os.Args[1] == "doctor" {
		os.Exit(doctor.Run(context.Background(), os.Stdout))
	}

	logger := newLogger()
	logger.Info("starting GTC", slog.String("version", version))

	fx.New(
		fx.Provide(func() *slog.Logger { return logger }),
		Module(),
	).Run()
}

func newLogger() *slog.Logger {
	level := slog.LevelInfo
	switch strings.ToUpper(os.Getenv("LOG_LEVEL")) {
	case "DEBUG":
		level = slog.LevelDebug
	case "WARN":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	}

	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
}
