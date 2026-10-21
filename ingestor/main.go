package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/phamngocquy/hw_data/ingestor/pkg/logger"
	"github.com/phamngocquy/hw_data/ingestor/pkg/metrics"
	"github.com/phamngocquy/hw_data/ingestor/pkg/openweather"
)

var version = "dev"

type config struct {
	DatabaseURL string
	Interval    time.Duration
	HTTPAddr    string
	Log         logger.Config
	OpenWeather openWeatherConfig
}

type openWeatherConfig struct {
	APIKey   string
	Location string
	Lat, Lon string
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ingestor:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	log, logFile, err := logger.New(cfg.Log)
	if err != nil {
		return err
	}
	defer logFile.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	store, err := openweather.NewStore(ctx, pool)
	if err != nil {
		return fmt.Errorf("prepare table: %w", err)
	}
	poller := &openweather.Poller{
		Client:   openweather.NewClient(cfg.OpenWeather.APIKey),
		Store:    store,
		Location: cfg.OpenWeather.Location,
		Lat:      cfg.OpenWeather.Lat,
		Lon:      cfg.OpenWeather.Lon,
		Interval: cfg.Interval,
	}
	metrics.SourceInfo.WithLabelValues(openweather.SourceName, poller.Target(), cfg.Interval.String(), version).Set(1)
	go serveHTTP(cfg.HTTPAddr, pool, log)

	log.Info("ingestor started", "version", version)
	poller.Run(ctx, log)
	log.Info("ingestor stopped")
	return nil
}

func serveHTTP(addr string, pool *pgxpool.Pool, log *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		if err := pool.Ping(r.Context()); err != nil {
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ok")
	})
	mux.Handle("GET /metrics", promhttp.Handler())

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Error("http server stopped", "error", err)
		os.Exit(1)
	}
}

func loadConfig() (config, error) {
	interval, err := time.ParseDuration(env("FETCH_INTERVAL", "30s"))
	if err != nil {
		return config{}, fmt.Errorf("FETCH_INTERVAL: %w", err)
	}
	cfg := config{
		DatabaseURL: env("DATABASE_URL", "postgres://etl:etl@localhost:5432/etl?sslmode=disable"),
		Interval:    interval,
		HTTPAddr:    env("HTTP_ADDR", ":8080"),
		Log:         logger.Config{File: env("LOG_FILE", "logs/etl.log"), Level: env("LOG_LEVEL", "info")},
		OpenWeather: openWeatherConfig{
			APIKey:   os.Getenv("OPENWEATHER_API_KEY"),
			Location: env("OPENWEATHER_LOCATION", "singapore"),
			Lat:      env("OPENWEATHER_LAT", "1.2897"),
			Lon:      env("OPENWEATHER_LON", "103.8501"),
		},
	}
	if cfg.OpenWeather.APIKey == "" {
		return config{}, fmt.Errorf("OPENWEATHER_API_KEY is required")
	}
	return cfg, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
