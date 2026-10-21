package openweather

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/phamngocquy/hw_data/ingestor/pkg/metrics"
)

type Poller struct {
	Client   *Client
	Store    *Store
	Location string
	Lat, Lon string
	Interval time.Duration
}

func (p *Poller) Target() string {
	return fmt.Sprintf("%s lat=%s lon=%s", p.Location, p.Lat, p.Lon)
}

func (p *Poller) Run(ctx context.Context, log *slog.Logger) {
	log = log.With("source", SourceName)
	log.Info("poller started", "target", p.Target(), "interval", p.Interval.String())

	ticker := time.NewTicker(p.Interval)
	defer ticker.Stop()
	for {
		p.pollOnce(ctx, log)
		select {
		case <-ctx.Done():
			log.Info("poller stopped")
			return
		case <-ticker.C:
		}
	}
}

func (p *Poller) pollOnce(ctx context.Context, log *slog.Logger) {
	runAt := time.Now().UTC()

	start := time.Now()
	data, err := p.Client.FetchCurrent(ctx, p.Lat, p.Lon)
	elapsed := time.Since(start)
	metrics.APIRequestDuration.WithLabelValues(SourceName).Observe(elapsed.Seconds())
	if err != nil {
		metrics.APIRequests.WithLabelValues(SourceName, "failure").Inc()
		log.Error("api request failed", "duration_ms", elapsed.Milliseconds(), "error", err)
		return
	}
	metrics.APIRequests.WithLabelValues(SourceName, "success").Inc()
	log.Info("api request succeeded", "duration_ms", elapsed.Milliseconds())

	id, err := p.Store.Insert(ctx, data, runAt)
	if err != nil {
		metrics.DBInserts.WithLabelValues(SourceName, "failure").Inc()
		log.Error("db insert failed", "error", err)
		return
	}
	metrics.DBInserts.WithLabelValues(SourceName, "success").Inc()
	metrics.LastSuccess.WithLabelValues(SourceName).SetToCurrentTime()
	log.Info("data saved", "table", "open_weather_raw", "id", id, "run_at", runAt.Format(time.RFC3339))
}
