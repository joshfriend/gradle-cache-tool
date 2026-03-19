package main

import (
	"net"
	"testing"
	"time"
)

func TestStatsdClientTiming(t *testing.T) {
	// Start a UDP listener to capture the statsd packet.
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pc.Close() }()

	addr := pc.LocalAddr().String()
	client := newStatsdClient(addr, []string{"env:test"})
	if client == nil {
		t.Fatal("expected non-nil statsd client")
	}
	defer client.close()

	client.timing("gradle_cache.restore.duration_ms", 1234, "cache_key:foo")

	buf := make([]byte, 1024)
	_ = pc.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _, err := pc.ReadFrom(buf)
	if err != nil {
		t.Fatal(err)
	}

	got := string(buf[:n])
	want := "gradle_cache.restore.duration_ms:1234|d|#env:test,cache_key:foo"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestStatsdClientGauge(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pc.Close() }()

	addr := pc.LocalAddr().String()
	client := newStatsdClient(addr, nil)
	if client == nil {
		t.Fatal("expected non-nil statsd client")
	}
	defer client.close()

	client.gauge("gradle_cache.save.size_bytes", 5678)

	buf := make([]byte, 1024)
	_ = pc.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _, err := pc.ReadFrom(buf)
	if err != nil {
		t.Fatal(err)
	}

	got := string(buf[:n])
	want := "gradle_cache.save.size_bytes:5678|g"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestEmitHelperNilSafe(t *testing.T) {
	// Should not panic when metrics is nil.
	emitTiming(nil, "test.metric", 100)
	emitGauge(nil, "test.metric", 200)
}

func TestMetricsFlagsNoneConfigured(t *testing.T) {
	// Unset DD env vars so auto-detection doesn't interfere.
	t.Setenv("DD_AGENT_HOST", "")
	f := &metricsFlags{}
	m := f.newMetricsClient()
	// May be non-nil if a local DD agent happens to be running — that's fine.
	// Just verify it doesn't panic.
	if m != nil {
		m.close()
	}
}
