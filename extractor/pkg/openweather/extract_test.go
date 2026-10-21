package openweather

import (
	"testing"
	"time"
)

func TestRawFilePathFillsTemplate(t *testing.T) {
	runAt := time.Date(2026, 10, 20, 1, 0, 0, 0, time.UTC)
	got := RawFilePath("data/{source_name}/raw/year=YYYY/month=MM/day=DD/YYYYMMDD_HHMM.json", runAt)
	want := "data/open_weather/raw/year=2026/month=10/day=20/20261020_0100.json"
	if got != want {
		t.Fatalf("RawFilePath = %s; want %s", got, want)
	}
}
