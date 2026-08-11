package main

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/joho/godotenv"
	"go.uber.org/fx"
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
