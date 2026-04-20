package gradlecache

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoggedMetrics_WritesJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dd.log")

	inner := &recordingMetrics{}
	lm, err := newLoggedMetrics(inner, path)
	if err != nil {
		t.Fatal(err)
	}

	lm.Distribution("gradle_cache.restore.duration_ms", 1234, "cache_key:foo")
	lm.Close()

	// Verify inner client received the metric.
	if len(inner.calls) != 1 {
		t.Fatalf("expected 1 inner call, got %d", len(inner.calls))
	}
	if inner.calls[0].name != "gradle_cache.restore.duration_ms" {
		t.Errorf("expected metric name, got %s", inner.calls[0].name)
	}

	// Verify JSON was written to the file.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("expected valid JSON, got: %s", string(data))
	}
	if m["msg"] != "gradle_cache.restore.duration_ms" {
		t.Errorf("expected metric name as msg, got %v", m["msg"])
	}
	if m["value"] != 1234.0 {
		t.Errorf("expected value=1234, got %v", m["value"])
	}
}

func TestLoggedMetrics_Appends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dd.log")

	inner := NoopMetrics{}
	lm, err := newLoggedMetrics(inner, path)
	if err != nil {
		t.Fatal(err)
	}

	lm.Distribution("metric.one", 1)
	lm.Distribution("metric.two", 2)
	lm.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(lines))
	}
}

func TestLoggedMetrics_InvalidPath(t *testing.T) {
	_, err := newLoggedMetrics(NoopMetrics{}, "/nonexistent/dir/dd.log")
	if err == nil {
		t.Error("expected error for invalid path")
	}
}

func TestLoggedMetrics_ClosesInner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dd.log")
	inner := &recordingMetrics{}
	lm, err := newLoggedMetrics(inner, path)
	if err != nil {
		t.Fatal(err)
	}
	lm.Close()
	if !inner.closed {
		t.Error("expected inner client to be closed")
	}
}

type metricCall struct {
	name  string
	value float64
	tags  []string
}

type recordingMetrics struct {
	calls  []metricCall
	closed bool
}

func (r *recordingMetrics) Distribution(name string, value float64, tags ...string) {
	r.calls = append(r.calls, metricCall{name, value, tags})
}

func (r *recordingMetrics) Close() {
	r.closed = true
}
