package idle

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/ebitengine/purego"
	"golang.org/x/sys/unix"
)

type machHost struct {
	port         uint32
	statistics   func(uint32, int32, *uint32, *uint32) int32
	statistics64 func(uint32, int32, *machVMStatistics, *uint32) int32
}

// Hold one send right for the lifetime of the process.
var openMachHost = sync.OnceValues(func() (machHost, error) {
	lib, err := purego.Dlopen("/usr/lib/libSystem.B.dylib", purego.RTLD_LAZY|purego.RTLD_LOCAL)
	if err != nil {
		return machHost{}, err
	}
	var self func() uint32
	var host machHost
	for _, symbol := range []struct {
		name   string
		target any
	}{
		{"mach_host_self", &self},
		{"host_statistics", &host.statistics},
		{"host_statistics64", &host.statistics64},
	} {
		address, err := purego.Dlsym(lib, symbol.name)
		if err != nil {
			purego.Dlclose(lib)
			return machHost{}, err
		}
		purego.RegisterFunc(symbol.target, address)
	}
	host.port = self()
	if host.port == 0 {
		purego.Dlclose(lib)
		return machHost{}, errors.New("mach_host_self returned no host port")
	}
	return host, nil
})

func hostCPU() (cpuTimes, error) {
	const hostCPULoadInfo = 3
	const cpuStateIdle = 2
	var ticks [4]uint32
	count := uint32(len(ticks))
	host, err := openMachHost()
	if err != nil {
		return cpuTimes{}, err
	}
	result := host.statistics(host.port, hostCPULoadInfo, &ticks[0], &count)
	if result != 0 || count != uint32(len(ticks)) {
		return cpuTimes{}, fmt.Errorf("Mach CPU counters unavailable (code %d, count %d)", result, count)
	}
	times := cpuTimes{idle: uint64(ticks[cpuStateIdle])}
	for _, tick := range ticks {
		times.total += uint64(tick)
	}
	return times, nil
}

// machVMStatistics matches vm_statistics64_data_t in <mach/vm_statistics.h>.
type machVMStatistics struct {
	free        uint32
	_           [3]uint32 // Active, inactive, and wired page counts.
	_           [9]uint64 // Cumulative VM event counts.
	_           uint32    // Purgeable page count.
	speculative uint32
	_           [4]uint64 // Cumulative compression and swap counts.
	_           [2]uint32 // Compressor and throttled page counts.
	fileBacked  uint32
	_           uint32    // Anonymous page count.
	_           [2]uint64 // Uncompressed and swapped compressor page counts.
}

func hostMemory() (uint64, error) {
	pressure, err := unix.SysctlUint32("kern.memorystatus_vm_pressure_level")
	if err != nil {
		return 0, fmt.Errorf("read VM pressure: %w", err)
	}
	// The sysctl returns the userspace NOTE_MEMORYSTATUS_PRESSURE_* flag.
	const pressureNormal = 1
	if pressure != pressureNormal {
		return 0, fmt.Errorf("macOS reports non-normal memory pressure (level %d)", pressure)
	}
	const hostVMInfo64 = 4
	var stats machVMStatistics
	const words = uint32(unsafe.Sizeof(stats) / 4)
	const fileBackedWords = uint32(unsafe.Offsetof(stats.fileBacked)/4 + 1)
	count := words
	host, err := openMachHost()
	if err != nil {
		return 0, err
	}
	result := host.statistics64(host.port, hostVMInfo64, &stats, &count)
	if result != 0 || count < fileBackedWords || count > words {
		return 0, fmt.Errorf("Mach VM counters unavailable (code %d, count %d)", result, count)
	}
	return macAvailablePages(stats) * uint64(os.Getpagesize()), nil
}

func macAvailablePages(stats machVMStatistics) uint64 {
	// XNU counts speculative pages under both free and file-backed pages.
	// Exclude inactive anonymous pages, which may require swapping.
	duplicate := min(stats.free, stats.fileBacked, stats.speculative)
	return uint64(stats.free) + uint64(stats.fileBacked) - uint64(duplicate)
}

func hostOnBattery(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/usr/bin/pmset", "-g", "batt")
	cmd.Env = append(cmd.Environ(), "LC_ALL=C")
	data, err := cmd.Output()
	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		return false, fmt.Errorf("read pmset power source: %w", err)
	}
	return parsePMSetPower(string(data))
}

func parsePMSetPower(output string) (bool, error) {
	first, _, _ := strings.Cut(output, "\n")
	switch strings.TrimSpace(first) {
	case "Now drawing from 'AC Power'":
		return false, nil
	case "Now drawing from 'Battery Power'", "Now drawing from 'UPS Power'":
		return true, nil
	default:
		return false, errors.New("pmset did not report a recognized power source")
	}
}
