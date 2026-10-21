package openweather

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFetchCurrentReturnsRawBody(t *testing.T) {
	const body = `{"id":1880252,"name":"Singapore","dt":1792590500}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/data/2.5/weather" {
			t.Errorf("path = %s", r.URL.Path)
		}
		q := r.URL.Query()
		if q.Get("lat") != "1.2897" || q.Get("lon") != "103.8501" || q.Get("appid") != "secret" {
			t.Errorf("query = %s", r.URL.RawQuery)
		}
		fmt.Fprint(w, body)
	}))
	defer server.Close()

	client := NewClient("secret")
	client.BaseURL = server.URL

	got, err := client.FetchCurrent(context.Background(), "1.2897", "103.8501")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != body {
		t.Fatalf("body = %s", got)
	}
}

func TestFetchCurrentFailsOnNon200(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"cod":401,"message":"Invalid API key"}`, http.StatusUnauthorized)
	}))
	defer server.Close()

	client := NewClient("secret")
	client.BaseURL = server.URL

	_, err := client.FetchCurrent(context.Background(), "1.2897", "103.8501")
	if err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("err = %v; want status 401", err)
	}
}

func TestFetchCurrentErrorDoesNotLeakAPIKey(t *testing.T) {
	client := NewClient("secret")
	client.BaseURL = "http://127.0.0.1:1"

	_, err := client.FetchCurrent(context.Background(), "1.2897", "103.8501")
	if err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatalf("err = %v; want an error without the API key", err)
	}
}
