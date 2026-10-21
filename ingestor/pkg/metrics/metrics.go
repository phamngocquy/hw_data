package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	APIRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ingestor_api_requests_total",
		Help: "Source API requests by result (success|failure).",
	}, []string{"source", "status"})

	APIRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ingestor_api_request_duration_seconds",
		Help:    "Latency of source API requests.",
		Buckets: prometheus.DefBuckets,
	}, []string{"source"})

	DBInserts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ingestor_db_inserts_total",
		Help: "Inserts of raw responses into Postgres by result (success|failure).",
	}, []string{"source", "status"})

	LastSuccess = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ingestor_last_success_timestamp_seconds",
		Help: "Unix time of the last response that was fetched and saved.",
	}, []string{"source"})

	SourceInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ingestor_source_info",
		Help: "What each source polls and how often (always 1).",
	}, []string{"source", "target", "fetch_interval", "version"})
)
