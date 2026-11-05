package idle

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func hostCPU() (cpuTimes, error) {
	raw, err := os.ReadFile("/proc/stat")
	if err != nil {
		return cpuTimes{}, err
	}
	return parseProcStat(string(raw))
}

func parseProcStat(raw string) (cpuTimes, error) {
	first, _, _ := strings.Cut(raw, "\n")
	fields := strings.Fields(first)
	if len(fields) < 5 || fields[0] != "cpu" {
		return cpuTimes{}, errors.New("missing aggregate CPU counters in /proc/stat")
	}
	var times cpuTimes
	// Guest ticks are included in the first two columns. Treat I/O wait
	// as unavailable capacity for background work.
	for i, field := range fields[1:min(9, len(fields))] {
		tick, err := strconv.ParseUint(field, 10, 64)
		if err != nil || math.MaxUint64-times.total < tick {
			return cpuTimes{}, errors.New("invalid CPU counter in /proc/stat")
		}
		times.total += tick
		if i == 3 {
			times.idle = tick
		}
	}
	return times, nil
}

func hostMemory() (uint64, error) {
	raw, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	return parseProcMeminfo(string(raw))
}

func parseProcMeminfo(raw string) (uint64, error) {
	for line := range strings.SplitSeq(raw, "\n") {
		value, found := strings.CutPrefix(line, "MemAvailable:")
		if !found {
			continue
		}
		fields := strings.Fields(value)
		if len(fields) != 2 || fields[1] != "kB" {
			return 0, errors.New("invalid MemAvailable in /proc/meminfo")
		}
		kib, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil || kib > math.MaxUint64/1024 {
			return 0, errors.New("invalid MemAvailable in /proc/meminfo")
		}
		return kib * 1024, nil
	}
	return 0, errors.New("missing MemAvailable in /proc/meminfo")
}

func hostOnBattery(ctx context.Context) (bool, error) {
	return linuxOnBattery(ctx, "/sys/class/power_supply")
}

func linuxOnBattery(ctx context.Context, root string) (bool, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return false, err
	}
	var battery, charging, online, offline, unknownLinePower bool
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		path := filepath.Join(root, entry.Name())
		kind, err := powerProperty(path, "type", false)
		if err != nil {
			return false, err
		}
		scope, err := powerProperty(path, "scope", true)
		if err != nil {
			return false, err
		}
		if scope == "Device" {
			continue
		}
		if scope != "" && scope != "System" && scope != "Unknown" {
			return false, fmt.Errorf("invalid power supply scope for %s", entry.Name())
		}
		switch kind {
		case "Battery", "UPS":
			present, err := powerProperty(path, "present", true)
			if err != nil {
				return false, err
			}
			if present == "0" {
				continue
			}
			if present != "" && present != "1" {
				return false, fmt.Errorf("invalid battery presence for %s", entry.Name())
			}
			battery = true
			status, err := powerProperty(path, "status", true)
			if err != nil {
				return false, err
			}
			if status == "Discharging" {
				return true, nil
			}
			charging = charging || status == "Charging"
		case "Mains", "Wireless", "BrickID", "Apple_Brick_ID",
			"USB", "USB_DCP", "USB_CDP", "USB_ACA", "USB_C", "USB_PD", "USB_PD_DRP":
			state, err := powerProperty(path, "online", true)
			if err != nil {
				return false, err
			}
			switch state {
			case "1":
				online = true
			case "0":
				offline = true
			default:
				unknownLinePower = true
			}
		default:
			return false, fmt.Errorf("unknown power supply type %q for %s", kind, entry.Name())
		}
	}
	switch {
	case !battery, charging, online:
		return false, nil
	case offline && !unknownLinePower:
		return true, nil
	default:
		return false, errors.New("system battery found with unknown power source")
	}
}

func powerProperty(path, property string, optional bool) (string, error) {
	raw, err := os.ReadFile(filepath.Join(path, property))
	if optional && errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	return strings.TrimSpace(string(raw)), err
}
