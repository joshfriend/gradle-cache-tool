package gradlecache

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

// MetricsClient emits distribution metrics to a backend.
type MetricsClient interface {
	Distribution(name string, value float64, tags ...string)
	Close()
}

// NoopMetrics is a no-op MetricsClient used when no backend is configured.
type NoopMetrics struct{}

func (NoopMetrics) Distribution(string, float64, ...string) {}
func (NoopMetrics) Close()                                  {}

// MetricsFlags are CLI flags for configuring metrics emission.
type MetricsFlags struct {
	StatsdAddr    string
	DatadogAPIKey string
	MetricsTags   []string
	DDLogPath     string // optional: path to a file tailed by the DD agent for log-based metrics
}

// DetectStatsdAddr returns the DogStatsD address from the environment, or empty
// if DD_AGENT_HOST is not set.
func DetectStatsdAddr() string {
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

// NewMetricsClient returns a MetricsClient based on the configured flags.
// When DDLogPath is set, each metric is also written as a JSON log line to
// that file for collection by the Datadog Agent.
func (f *MetricsFlags) NewMetricsClient() MetricsClient {
	var client MetricsClient
	if f.StatsdAddr != "" {
		if c := NewStatsdClient(f.StatsdAddr, f.MetricsTags); c != nil {
			slog.Debug("metrics: using DogStatsD", "addr", f.StatsdAddr)
			client = c
		} else {
			slog.Warn("failed to connect to DogStatsD, metrics disabled", "addr", f.StatsdAddr)
			client = NoopMetrics{}
		}
	} else if f.DatadogAPIKey != "" {
		slog.Debug("metrics: using Datadog HTTP API")
		client = NewDatadogAPIClient(f.DatadogAPIKey, f.MetricsTags)
	} else if addr := DetectStatsdAddr(); addr != "" {
		if c := NewStatsdClient(addr, f.MetricsTags); c != nil {
			slog.Debug("metrics: auto-detected DogStatsD agent", "addr", addr)
			client = c
		}
	}
	if client == nil {
		slog.Debug("metrics: no backend configured, metrics disabled")
		client = NoopMetrics{}
	}

	if f.DDLogPath != "" {
		lm, err := newLoggedMetrics(client, f.DDLogPath)
		if err != nil {
			slog.Warn("failed to open DD log file, log-based metrics disabled", "path", f.DDLogPath, "error", err)
			return client
		}
		slog.Debug("metrics: also logging to DD agent file", "path", f.DDLogPath)
		return lm
	}
	return client
}

// ── DogStatsD (UDP) ─────────────────────────────────────────────────────────

// StatsdClient sends metrics via DogStatsD UDP protocol.
type StatsdClient struct {
	conn net.Conn
	tags []string
}

// NewStatsdClient creates a new statsd client, or nil if connection fails.
func NewStatsdClient(addr string, baseTags []string) *StatsdClient {
	conn, err := net.DialTimeout("udp", addr, 2*time.Second)
	if err != nil {
		return nil
	}
	return &StatsdClient{conn: conn, tags: baseTags}
}

func (s *StatsdClient) Distribution(name string, value float64, tags ...string) {
	s.send(fmt.Sprintf("%s:%g|d", name, value), tags)
}

func (s *StatsdClient) send(stat string, extraTags []string) {
	allTags := append(s.tags, extraTags...)
	if len(allTags) > 0 {
		stat += "|#" + strings.Join(allTags, ",")
	}
	s.conn.Write([]byte(stat)) //nolint:errcheck,gosec
}

func (s *StatsdClient) Close() {
	s.conn.Close() //nolint:errcheck,gosec
}

// ── DataDog HTTP API (v1 distribution_points) ───────────────────────────────

const datadogDistURL = "https://api.datadoghq.com/api/v1/distribution_points"

// DatadogAPIClient sends metrics via the Datadog HTTP API.
type DatadogAPIClient struct {
	apiKey string
	tags   []string
	http   *http.Client
}

// NewDatadogAPIClient creates a new Datadog API client.
func NewDatadogAPIClient(apiKey string, baseTags []string) *DatadogAPIClient {
	return &DatadogAPIClient{
		apiKey: apiKey,
		tags:   baseTags,
		http:   &http.Client{Timeout: 5 * time.Second},
	}
}

func (d *DatadogAPIClient) Distribution(name string, value float64, tags ...string) {
	allTags := append(d.tags, tags...)
	now := time.Now().Unix()

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

func (d *DatadogAPIClient) Close() {}
