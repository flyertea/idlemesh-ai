// Package idle samples system resources for the worker admission policy.
package idle

import (
	"context"
	"errors"
	"fmt"
)

type Snapshot struct {
	CPUPercent      float64
	AvailableMemory uint64
	OnBattery       bool
}

// ErrWarmingUp indicates that the probe needs another CPU reading.
var ErrWarmingUp = errors.New("waiting for a CPU load sample")

type cpuTimes struct {
	total uint64
	idle  uint64
}

// Probe measures host-wide CPU use over successive calls. Its zero value is
// ready to use. A probe belongs to one worker goroutine.
type Probe struct {
	previous cpuTimes
}

// Reset starts a new CPU measurement interval.
func (p *Probe) Reset() {
	p.previous = cpuTimes{}
}

// Sample reads CPU load, available physical memory, and the current power
// source. An error means the snapshot cannot be used to admit work.
func (p *Probe) Sample(ctx context.Context) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	times, err := hostCPU()
	if err != nil {
		p.Reset()
		return Snapshot{}, fmt.Errorf("sample CPU: %w", err)
	}
	load, valid := cpuPercent(p.previous, times)
	p.previous = times
	if !valid {
		return Snapshot{}, ErrWarmingUp
	}
	memory, err := hostMemory()
	if err != nil {
		return Snapshot{}, fmt.Errorf("sample available memory: %w", err)
	}
	onBattery, err := hostOnBattery(ctx)
	if err != nil {
		return Snapshot{}, fmt.Errorf("sample power source: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	return Snapshot{CPUPercent: load, AvailableMemory: memory, OnBattery: onBattery}, nil
}

func cpuPercent(prev, next cpuTimes) (float64, bool) {
	if prev.total == 0 || next.total <= prev.total || next.idle < prev.idle {
		return 0, false
	}
	total, idle := next.total-prev.total, next.idle-prev.idle
	if idle > total {
		return 0, false
	}
	return 100 * float64(total-idle) / float64(total), true
}
