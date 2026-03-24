package main

import (
	"net"
	"testing"
	"time"
)

func TestStatsdClientDistribution(t *testing.T) {
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

	client.distribution("gradle_cache.restore.duration_ms", 1234, "cache_key:foo")

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

func TestStatsdClientDistributionFloat(t *testing.T) {
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

	client.distribution("gradle_cache.restore.speed_mbps", 155.6)

	buf := make([]byte, 1024)
	_ = pc.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _, err := pc.ReadFrom(buf)
	if err != nil {
		t.Fatal(err)
	}

	got := string(buf[:n])
	want := "gradle_cache.restore.speed_mbps:155.6|d"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestNoopMetrics(t *testing.T) {
	// noopMetrics should not panic.
	var m metricsClient = noopMetrics{}
	m.distribution("test.metric", 100)
	m.close()
}

func TestMetricsFlagsNoneConfigured(t *testing.T) {
	// Unset DD env vars so auto-detection doesn't interfere.
	t.Setenv("DD_AGENT_HOST", "")
	f := &metricsFlags{}
	m := f.newMetricsClient()
	// Should always return a non-nil client (noopMetrics when no backend configured).
	m.close()
}
