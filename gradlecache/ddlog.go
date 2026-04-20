package gradlecache

import (
	"log/slog"
	"os"
)

// loggedMetrics wraps a MetricsClient and also writes each metric as a JSON
// log line to a file tailed by the Datadog Agent. This allows a single
// Distribution call to emit both a DogStatsD metric and a structured log event.
type loggedMetrics struct {
	inner  MetricsClient
	logger *slog.Logger
	file   *os.File
}

func newLoggedMetrics(inner MetricsClient, path string) (*loggedMetrics, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	logger := slog.New(slog.NewJSONHandler(f, nil))
	return &loggedMetrics{inner: inner, logger: logger, file: f}, nil
}

func (m *loggedMetrics) Distribution(name string, value float64, tags ...string) {
	m.inner.Distribution(name, value, tags...)
	m.logger.Info(name, "value", value, "tags", tags)
}

func (m *loggedMetrics) Close() {
	m.inner.Close()
	_ = m.file.Close()
}
