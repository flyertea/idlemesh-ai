package worker

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/flyertea/idlemesh-ai/internal/api"
	"github.com/flyertea/idlemesh-ai/internal/idle"
)

func TestAdmissionRequiresContinuousAvailableResources(t *testing.T) {
	a := Admission{config: AdmissionConfig{CPUPercent: 25, MinMemoryMiB: 2048, IdleFor: 6 * time.Second}}
	now := time.Now()
	ready := idle.Snapshot{CPUPercent: 8, AvailableMemory: 3 << 30}
	cases := []struct {
		after  time.Duration
		sample idle.Snapshot
		err    error
		want   string
	}{
		{0, ready, idle.ErrWarmingUp, "warming up resource sampling"},
		{2, ready, nil, "waiting for 6s of available resources"},
		{6, ready, nil, "waiting for 6s of available resources"},
		{8, idle.Snapshot{CPUPercent: 8, AvailableMemory: 3 << 30, OnBattery: true}, nil, "running on battery power"},
		{10, ready, nil, "waiting for 6s of available resources"},
		{12, idle.Snapshot{CPUPercent: 30, AvailableMemory: 3 << 30}, nil, "CPU is busy"},
		{14, idle.Snapshot{CPUPercent: 8, AvailableMemory: 1 << 30}, nil, "available memory is below the configured minimum"},
		{16, ready, errors.New("power sensor unavailable"), "host resources could not be measured"},
		{18, ready, nil, "waiting for 6s of available resources"},
		{24, ready, nil, ""},
	}
	for _, tc := range cases {
		if got := a.check(tc.sample, tc.err, now.Add(tc.after*time.Second)); got != tc.want {
			t.Errorf("at %ds: got %q, want %q", tc.after, got, tc.want)
		}
	}
}

type batteryProbe struct {
	battery atomic.Bool
	sampled chan bool
}

func (p *batteryProbe) Reset() {}

func (p *batteryProbe) Sample(ctx context.Context) (idle.Snapshot, error) {
	battery := p.battery.Load()
	select {
	case p.sampled <- battery:
	case <-ctx.Done():
		return idle.Snapshot{}, ctx.Err()
	}
	return idle.Snapshot{AvailableMemory: 3 << 30, OnBattery: battery}, nil
}

func TestWorkerSkipsClaimOnBatteryAndResumes(t *testing.T) {
	claim := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/worker/claim" {
			t.Errorf("unexpected endpoint %s", r.URL.Path)
		}
		select {
		case claim <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := api.NewClient(server.URL, "", false)
	if err != nil {
		t.Fatal(err)
	}
	probe := &batteryProbe{sampled: make(chan bool)}
	probe.battery.Store(true)
	a := &Admission{
		config: AdmissionConfig{CPUPercent: 25, MinMemoryMiB: 2048, CheckInterval: 50 * time.Millisecond},
		probe:  probe,
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, client, Echo(0), Config{
			ID: "test", Engine: "echo", PollInterval: time.Millisecond, JobTimeout: time.Second,
			Logger: log.New(io.Discard, "", 0), Admission: a,
		})
	}()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	for range 2 {
		select {
		case <-claim:
			t.Fatal("worker claimed on battery")
		case battery := <-probe.sampled:
			if !battery {
				t.Fatal("expected battery sample")
			}
		case <-ctx.Done():
			t.Fatal("worker did not measure battery state")
		}
	}
	probe.battery.Store(false)
	select {
	case <-claim:
		t.Fatal("worker claimed before sampling AC state")
	case battery := <-probe.sampled:
		if battery {
			t.Fatal("expected AC sample")
		}
	case <-ctx.Done():
		t.Fatal("worker did not measure the change to AC")
	}
	select {
	case <-claim:
	case <-ctx.Done():
		t.Fatal("worker did not resume claiming on AC")
	}
}
