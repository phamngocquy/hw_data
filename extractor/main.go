package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/phamngocquy/hw_data/extractor/pkg/logger"
	"github.com/phamngocquy/hw_data/extractor/pkg/metrics"
	"github.com/phamngocquy/hw_data/extractor/pkg/openweather"
)

const hourLayout = "20060102 15"

var version = "dev"

type config struct {
	From, To       time.Time
	RunID          string
	DatabaseURL    string
	RawTemplate    string
	PushgatewayURL string
	Log            logger.Config
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "extractor:", err)
		os.Exit(1)
	}
}

func run() error {
	startedAt := time.Now().UTC()
	cfg, err := loadConfig(startedAt)
	if err != nil {
		return err
	}

	log, logFile, err := logger.New(cfg.Log)
	if err != nil {
		return err
	}
	defer logFile.Close()
	log = log.With("run_id", cfg.RunID, "source", openweather.SourceName)
	log.Info("extract started",
		"from", cfg.From.Format(time.RFC3339), "to", cfg.To.Format(time.RFC3339), "version", version)

	res, err := openweather.Extract(context.Background(), openweather.Params{
		DatabaseURL:     cfg.DatabaseURL,
		RawFileTemplate: cfg.RawTemplate,
		From:            cfg.From,
		To:              cfg.To,
	}, log)

	status := "success"
	if err != nil {
		status = "failure"
		log.Error("extract failed", "error", err)
	}
	pushMetrics(cfg, metrics.Run{
		RunID: cfg.RunID, Source: openweather.SourceName, Status: status, Version: version,
		WindowStart: cfg.From, WindowEnd: cfg.To,
		StartedAt: startedAt, FinishedAt: time.Now().UTC(),
		OutputFile: res.OutputFile, FileVersion: res.FileVersion, Rows: res.Rows,
	}, log)
	if err != nil {
		return err
	}

	fmt.Println(res.OutputFile) // the orchestrator reads this line
	return nil
}

func pushMetrics(cfg config, r metrics.Run, log *slog.Logger) {
	if cfg.PushgatewayURL == "" {
		return
	}
	if err := metrics.Push(cfg.PushgatewayURL, r); err != nil {
		log.Warn("metrics push failed", "error", err)
	}
}

func loadConfig(startedAt time.Time) (config, error) {
	fromFlag := flag.String("from", "", `start hour "YYYYMMDD HH" (e.g. "20261020 16"); exports [from, from + 1h)`)
	runID := flag.String("run-id", "", "orchestrator run id (default: manual-<timestamp>)")
	flag.Parse()

	from, err := time.ParseInLocation(hourLayout, *fromFlag, time.UTC)
	if err != nil {
		return config{}, fmt.Errorf(`--from: want "YYYYMMDD HH": %w`, err)
	}
	if *runID == "" {
		*runID = "manual-" + startedAt.Format("20060102T150405Z")
	}
	if os.Getenv("RAW_FILE_TEMPLATE") == "" {
		return config{}, fmt.Errorf("RAW_FILE_TEMPLATE is required (see extractor/.env.example)")
	}

	return config{
		From:           from,
		To:             from.Add(time.Hour),
		RunID:          *runID,
		DatabaseURL:    env("DATABASE_URL", "postgres://etl:etl@localhost:5432/etl?sslmode=disable"),
		RawTemplate:    os.Getenv("RAW_FILE_TEMPLATE"),
		PushgatewayURL: os.Getenv("PUSHGATEWAY_URL"),
		Log:            logger.Config{File: env("LOG_FILE", "logs/etl.log"), Level: env("LOG_LEVEL", "info")},
	}, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
