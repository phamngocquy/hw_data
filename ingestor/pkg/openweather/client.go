package openweather

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

type Client struct {
	BaseURL string
	APIKey  string
	HTTP    *http.Client
}

func NewClient(apiKey string) *Client {
	return &Client{
		BaseURL: "https://api.openweathermap.org",
		APIKey:  apiKey,
		HTTP:    &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *Client) FetchCurrent(ctx context.Context, lat, lon string) ([]byte, error) {
	query := url.Values{"lat": {lat}, "lon": {lon}, "appid": {c.APIKey}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/data/2.5/weather?"+query.Encode(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.HTTP.Do(req)
	if err != nil {
		// Drop the request URL from the error so the API key never reaches the logs.
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Err
		}
		return nil, fmt.Errorf("openweather request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("openweather status %d: %s", resp.StatusCode, body)
	}
	return body, nil
}
