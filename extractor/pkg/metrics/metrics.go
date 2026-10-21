package metrics

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

type Run struct {
	RunID       string
	Source      string
	Status      string
	Version     string
	WindowStart time.Time
	WindowEnd   time.Time
	StartedAt   time.Time
	FinishedAt  time.Time
	OutputFile  string
	FileVersion int
	Rows        int
}

// Add (POST) replaces only the metrics sent, so a failed run keeps the last success timestamp.
func Push(url string, r Run) error {
	reg := prometheus.NewRegistry()

	info := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "extractor_run_info",
		Help: "Parameters and metadata of the last extractor run (always 1).",
	}, []string{
		"run_id", "status", "window_start", "window_end", "output_file",
		"file_version", "started_at", "finished_at", "version",
	})
	info.WithLabelValues(
		r.RunID, r.Status, rfc3339(r.WindowStart), rfc3339(r.WindowEnd), r.OutputFile,
		strconv.Itoa(r.FileVersion), rfc3339(r.StartedAt), rfc3339(r.FinishedAt), r.Version,
	).Set(1)

	success := 0.0
	if r.Status == "success" {
		success = 1
	}
	reg.MustRegister(info,
		gauge("extractor_rows_extracted", "Rows exported by the last run.", float64(r.Rows)),
		gauge("extractor_run_duration_seconds", "Duration of the last run.", r.FinishedAt.Sub(r.StartedAt).Seconds()),
		gauge("extractor_last_run_success", "1 if the last run succeeded, else 0.", success),
	)
	if r.Status == "success" {
		reg.MustRegister(gauge("extractor_last_success_timestamp_seconds",
			"Unix time of the last successful run.", float64(r.FinishedAt.Unix())))
	}

	return push.New(url, "extractor").Grouping("source", r.Source).Gatherer(reg).Add()
}

func gauge(name, help string, value float64) prometheus.Gauge {
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
	g.Set(value)
	return g
}

func rfc3339(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}
