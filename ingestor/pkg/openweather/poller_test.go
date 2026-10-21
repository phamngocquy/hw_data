package openweather

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/phamngocquy/hw_data/ingestor/pkg/metrics"
)

func TestPollOnceCountsFailedRequestUnderSourceLabel(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "down", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client := NewClient("secret")
	client.BaseURL = server.URL
	p := &Poller{Client: client, Lat: "1.2897", Lon: "103.8501", Interval: time.Minute}

	p.pollOnce(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)))

	if got := testutil.ToFloat64(metrics.APIRequests.WithLabelValues(SourceName, "failure")); got != 1 {
		t.Fatalf(`ingestor_api_requests_total{source="open_weather",status="failure"} = %v; want 1`, got)
	}
}
