package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

// metricsClient emits distribution metrics to a backend.
// All metrics are submitted as distributions to support percentile aggregation.
type metricsClient interface {
	// distribution records a single sample for a distribution metric.
	distribution(name string, value float64, tags ...string)
	// close flushes any pending data.
	close()
}

// noopMetrics is a no-op metricsClient used when no backend is configured.
// It exists because kong cannot bind a nil interface value.
type noopMetrics struct{}

func (noopMetrics) distribution(string, float64, ...string) {}
func (noopMetrics) close()                                  {}

// metricsFlags are CLI flags for configuring metrics emission.
type metricsFlags struct {
	StatsdAddr    string   `help:"DogStatsD address (host:port) for emitting metrics. Auto-detected from DD_AGENT_HOST if not set."`
	DatadogAPIKey string   `help:"DataDog API key for direct metric submission (no agent required)." env:"DATADOG_API_KEY"`
	MetricsTags   []string `help:"Additional metric tags in key:value format. May be repeated." name:"metrics-tag"`
}

// detectStatsdAddr returns the DogStatsD address from the environment, or empty
// if DD_AGENT_HOST is not set.
func detectStatsdAddr() string {
	host := os.Getenv("DD_AGENT_HOST")
	if host == "" {
		return ""
	}
	port := os.Getenv("DD_DOGSTATSD_PORT")
	if port == "" {
		port = "8125"
	}
	return net.JoinHostPort(host, port)
}

// newMetricsClient returns a metricsClient based on the configured flags.
// If no explicit backend is configured, auto-detects a local DD agent.
// Returns a no-op client if no metrics backend is available.
func (f *metricsFlags) newMetricsClient() metricsClient {
	if f.StatsdAddr != "" {
		if c := newStatsdClient(f.StatsdAddr, f.MetricsTags); c != nil {
			slog.Info("metrics: using DogStatsD", "addr", f.StatsdAddr)
			return c
		}
		slog.Warn("failed to connect to DogStatsD, metrics disabled", "addr", f.StatsdAddr)
		return noopMetrics{}
	}
	if f.DatadogAPIKey != "" {
		slog.Info("metrics: using Datadog HTTP API")
		return newDatadogAPIClient(f.DatadogAPIKey, f.MetricsTags)
	}
	// Auto-detect local DD agent.
	if addr := detectStatsdAddr(); addr != "" {
		if c := newStatsdClient(addr, f.MetricsTags); c != nil {
			slog.Info("metrics: auto-detected DogStatsD agent", "addr", addr)
			return c
		}
	}
	slog.Debug("metrics: no backend configured, metrics disabled")
	return noopMetrics{}
}

// ── DogStatsD (UDP) ─────────────────────────────────────────────────────────

type statsdClient struct {
	conn net.Conn
	tags []string
}

func newStatsdClient(addr string, baseTags []string) *statsdClient {
	conn, err := net.DialTimeout("udp", addr, 2*time.Second)
	if err != nil {
		return nil
	}
	return &statsdClient{conn: conn, tags: baseTags}
}

func (s *statsdClient) distribution(name string, value float64, tags ...string) {
	s.send(fmt.Sprintf("%s:%g|d", name, value), tags)
}

func (s *statsdClient) send(stat string, extraTags []string) {
	allTags := append(s.tags, extraTags...)
	if len(allTags) > 0 {
		stat += "|#" + strings.Join(allTags, ",")
	}
	s.conn.Write([]byte(stat)) //nolint:errcheck,gosec
}

func (s *statsdClient) close() {
	s.conn.Close() //nolint:errcheck,gosec
}

// ── DataDog HTTP API (v1 distribution_points) ───────────────────────────────

const datadogDistURL = "https://api.datadoghq.com/api/v1/distribution_points"

type datadogAPIClient struct {
	apiKey string
	tags   []string
	http   *http.Client
}

func newDatadogAPIClient(apiKey string, baseTags []string) *datadogAPIClient {
	return &datadogAPIClient{
		apiKey: apiKey,
		tags:   baseTags,
		http:   &http.Client{Timeout: 5 * time.Second},
	}
}

func (d *datadogAPIClient) distribution(name string, value float64, tags ...string) {
	allTags := append(d.tags, tags...)
	now := time.Now().Unix()

	// v1 distribution_points format: points is [[timestamp, [value, ...]]]
	payload := map[string]interface{}{
		"series": []map[string]interface{}{
			{
				"metric": name,
				"points": []interface{}{
					[]interface{}{now, []float64{value}},
				},
				"tags": allTags,
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		slog.Warn("metrics: failed to marshal payload", "error", err)
		return
	}

	req, err := http.NewRequest("POST", datadogDistURL, bytes.NewReader(body))
	if err != nil {
		slog.Warn("metrics: failed to create request", "error", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("DD-API-KEY", d.apiKey)

	resp, err := d.http.Do(req)
	if err != nil {
		slog.Warn("metrics: failed to submit to Datadog API", "error", err)
		return
	}
	defer resp.Body.Close() //nolint:errcheck,gosec
	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		slog.Warn("metrics: Datadog API returned error", "status", resp.StatusCode, "metric", name, "body", string(respBody))
	}
}

func (d *datadogAPIClient) close() {}
