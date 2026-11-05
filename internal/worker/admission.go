package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/flyertea/idlemesh-ai/internal/idle"
)

type AdmissionConfig struct {
	CPUPercent    float64
	MinMemoryMiB  uint64
	IdleFor       time.Duration
	CheckInterval time.Duration
}

type hostProbe interface {
	Sample(context.Context) (idle.Snapshot, error)
	Reset()
}

// Admission checks host resources before each claim. A worker uses it serially.
type Admission struct {
	config AdmissionConfig
	probe  hostProbe
	last   time.Time
	since  time.Time
	reason string
}

func NewAdmission(config AdmissionConfig) (*Admission, error) {
	if math.IsNaN(config.CPUPercent) || config.CPUPercent <= 0 || config.CPUPercent > 100 {
		return nil, errors.New("--idle-cpu-percent must be greater than 0 and at most 100")
	}
	if config.MinMemoryMiB == 0 || config.MinMemoryMiB > math.MaxUint64>>20 {
		return nil, errors.New("--idle-min-memory-mib must be positive and fit in a byte count")
	}
	if config.IdleFor < 0 || config.IdleFor > time.Hour {
		return nil, errors.New("--idle-for must be between 0s and 1h")
	}
	if config.CheckInterval < 250*time.Millisecond || config.CheckInterval > time.Minute {
		return nil, errors.New("--idle-check must be between 250ms and 1m")
	}
	return &Admission{config: config, probe: new(idle.Probe)}, nil
}

func (a *Admission) reset() {
	a.probe.Reset()
	a.last = time.Time{}
	a.since = time.Time{}
}

func (a *Admission) wait(ctx context.Context, logger *log.Logger) bool {
	for ctx.Err() == nil {
		if !a.last.IsZero() && !wait(ctx, time.Until(a.last.Add(a.config.CheckInterval))) {
			return false
		}
		if !a.last.IsZero() && time.Since(a.last) > 3*a.config.CheckInterval {
			a.reset()
		}
		started := time.Now()
		sampleCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		sample, err := a.probe.Sample(sampleCtx)
		cancel()
		if ctx.Err() != nil {
			return false
		}
		a.last = started
		reason := a.check(sample, err, started)
		if reason != a.reason {
			if reason == "" {
				logger.Printf("resources ready; accepting jobs (CPU %.1f%%, available memory %d MiB)", sample.CPUPercent, sample.AvailableMemory>>20)
			} else if err != nil && !errors.Is(err, idle.ErrWarmingUp) {
				logger.Printf("paused claiming: %s: %v", reason, err)
			} else if reason == "CPU is busy" || reason == "available memory is below the configured minimum" {
				logger.Printf("paused claiming: %s (CPU %.1f%%, available memory %d MiB)", reason, sample.CPUPercent, sample.AvailableMemory>>20)
			} else {
				logger.Printf("paused claiming: %s", reason)
			}
			a.reason = reason
		}
		if reason == "" {
			return true
		}
	}
	return false
}

func (a *Admission) check(sample idle.Snapshot, err error, now time.Time) string {
	var reason string
	switch {
	case errors.Is(err, idle.ErrWarmingUp):
		reason = "warming up resource sampling"
	case err != nil || math.IsNaN(sample.CPUPercent) || sample.CPUPercent < 0 || sample.CPUPercent > 100:
		reason = "host resources could not be measured"
	case sample.OnBattery:
		reason = "running on battery power"
	case sample.CPUPercent > a.config.CPUPercent:
		reason = "CPU is busy"
	case sample.AvailableMemory < a.config.MinMemoryMiB<<20:
		reason = "available memory is below the configured minimum"
	}
	if reason != "" {
		a.since = time.Time{}
		return reason
	}
	if a.since.IsZero() {
		a.since = now
	}
	if now.Sub(a.since) < a.config.IdleFor {
		return fmt.Sprintf("waiting for %s of available resources", a.config.IdleFor)
	}
	return ""
}
