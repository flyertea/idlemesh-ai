//go:build darwin || linux || windows

package idle

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestHostProbe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var probe Probe
	if _, err := probe.Sample(ctx); !errors.Is(err, ErrWarmingUp) {
		t.Fatalf("first sample returned %v", err)
	}
	if _, err := hostMemory(); err != nil {
		t.Fatalf("read host memory: %v", err)
	}
	if _, err := hostOnBattery(ctx); err != nil {
		t.Skipf("host power source unavailable: %v", err)
	}
	for {
		select {
		case <-ctx.Done():
			t.Fatal("CPU counters did not advance")
		case <-time.After(250 * time.Millisecond):
		}
		snapshot, err := probe.Sample(ctx)
		if errors.Is(err, ErrWarmingUp) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.CPUPercent < 0 || snapshot.CPUPercent > 100 {
			t.Fatalf("out-of-range CPU use: %.1f", snapshot.CPUPercent)
		}
		t.Logf("CPU %.1f%%, memory %d MiB, on battery %t", snapshot.CPUPercent, snapshot.AvailableMemory>>20, snapshot.OnBattery)
		break
	}
	probe.Reset()
	if _, err := probe.Sample(ctx); !errors.Is(err, ErrWarmingUp) {
		t.Fatalf("after reset, sample returned %v", err)
	}
}
