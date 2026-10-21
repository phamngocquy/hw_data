package openweather

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const createTable = `
CREATE TABLE IF NOT EXISTS open_weather_raw (
    id         BIGSERIAL PRIMARY KEY,
    data       JSONB       NOT NULL,
    run_at     TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`

const createRunAtIndex = `CREATE INDEX IF NOT EXISTS open_weather_raw_run_at_idx ON open_weather_raw (run_at)`

type Store struct {
	pool *pgxpool.Pool
}

// NewStore also creates the table if it is missing.
func NewStore(ctx context.Context, pool *pgxpool.Pool) (*Store, error) {
	for _, stmt := range []string{createTable, createRunAtIndex} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return nil, err
		}
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Insert(ctx context.Context, data []byte, runAt time.Time) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO open_weather_raw (data, run_at) VALUES ($1, $2) RETURNING id`,
		data, runAt,
	).Scan(&id)
	return id, err
}
