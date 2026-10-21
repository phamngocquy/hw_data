package openweather

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/phamngocquy/hw_data/extractor/pkg/lakefile"
)

// Shared by all data files, always UTC: 20261020 16:00:00.
const timeLayout = "20060102 15:04:05"

type Params struct {
	DatabaseURL     string
	RawFileTemplate string
	From            time.Time // inclusive
	To              time.Time // exclusive; names the output file
}

type Result struct {
	OutputFile  string
	FileVersion int
	Rows        int
}

type rawRecord struct {
	ID        int64           `json:"id"`
	RunAt     string          `json:"run_at"`
	CreatedAt string          `json:"created_at"`
	UpdatedAt string          `json:"updated_at"`
	Data      json.RawMessage `json:"data"`
}

func Extract(ctx context.Context, p Params, log *slog.Logger) (Result, error) {
	content, rows, err := readRows(ctx, p.DatabaseURL, p.From, p.To)
	if err != nil {
		return Result{}, err
	}
	log.Info("extracted rows", "rows", rows,
		"window_start", p.From.Format(time.RFC3339), "window_end", p.To.Format(time.RFC3339))

	file, version, err := lakefile.Write(RawFilePath(p.RawFileTemplate, p.To), content)
	if err != nil {
		return Result{Rows: rows}, err
	}
	log.Info("data saved", "file", file, "version", version, "rows", rows)
	return Result{OutputFile: file, FileVersion: version, Rows: rows}, nil
}

func RawFilePath(template string, t time.Time) string {
	t = t.UTC()
	// Longer tokens first, so YYYYMMDD and HHMM win over YYYY and MM at the same position.
	return strings.NewReplacer(
		"{source_name}", SourceName,
		"YYYYMMDD", t.Format("20060102"),
		"HHMM", t.Format("1504"),
		"YYYY", t.Format("2006"),
		"MM", t.Format("01"),
		"DD", t.Format("02"),
	).Replace(template)
}

// readRows returns the rows with run_at in [from, to) as JSON Lines, and their count.
func readRows(ctx context.Context, databaseURL string, from, to time.Time) ([]byte, int, error) {
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return nil, 0, err
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx,
		`SELECT id, data, run_at, created_at, updated_at
		   FROM open_weather_raw
		  WHERE run_at >= $1 AND run_at < $2
		  ORDER BY id`,
		from, to)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	count := 0
	for rows.Next() {
		var r rawRecord
		var runAt, createdAt, updatedAt time.Time
		if err := rows.Scan(&r.ID, &r.Data, &runAt, &createdAt, &updatedAt); err != nil {
			return nil, 0, err
		}
		r.RunAt = runAt.UTC().Format(timeLayout)
		r.CreatedAt = createdAt.UTC().Format(timeLayout)
		r.UpdatedAt = updatedAt.UTC().Format(timeLayout)
		if err := enc.Encode(r); err != nil {
			return nil, 0, err
		}
		count++
	}
	return buf.Bytes(), count, rows.Err()
}
