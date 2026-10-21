package logger

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
)

type Config struct {
	File  string
	Level string
}

func New(cfg Config) (*slog.Logger, io.Closer, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(cfg.Level)); err != nil {
		return nil, nil, fmt.Errorf("LOG_LEVEL: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(cfg.File), 0o755); err != nil {
		return nil, nil, err
	}
	f, err := os.OpenFile(cfg.File, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, err
	}
	handler := slog.NewTextHandler(io.MultiWriter(os.Stderr, f), &slog.HandlerOptions{Level: level})
	return slog.New(handler).With("component", "ingestor"), f, nil
}
