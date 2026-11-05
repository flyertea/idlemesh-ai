package idle

import (
	"context"
	"errors"
	"fmt"
	"unsafe"

	"golang.org/x/sys/windows"
)

var (
	kernel32                     = windows.NewLazySystemDLL("kernel32.dll")
	getSystemTimes               = kernel32.NewProc("GetSystemTimes")
	getActiveProcessorGroupCount = kernel32.NewProc("GetActiveProcessorGroupCount")
	globalMemoryStatusEx         = kernel32.NewProc("GlobalMemoryStatusEx")
	getSystemPowerStatus         = kernel32.NewProc("GetSystemPowerStatus")
)

func hostCPU() (cpuTimes, error) {
	if err := getActiveProcessorGroupCount.Find(); err != nil {
		return cpuTimes{}, err
	}
	groups, _, _ := getActiveProcessorGroupCount.Call()
	// GetSystemTimes covers only the primary group on systems with multiple
	// processor groups. Do not mistake a partial CPU reading for a host sample.
	if groups != 1 {
		return cpuTimes{}, errors.New("host CPU load unavailable with multiple Windows processor groups")
	}
	if err := getSystemTimes.Find(); err != nil {
		return cpuTimes{}, err
	}
	var idle, kernel, user windows.Filetime
	ok, _, err := getSystemTimes.Call(uintptr(unsafe.Pointer(&idle)), uintptr(unsafe.Pointer(&kernel)), uintptr(unsafe.Pointer(&user)))
	if ok == 0 {
		return cpuTimes{}, fmt.Errorf("GetSystemTimes: %w", err)
	}
	// Kernel time includes idle time.
	return cpuTimes{total: ticks(kernel) + ticks(user), idle: ticks(idle)}, nil
}

func ticks(ft windows.Filetime) uint64 {
	return uint64(ft.HighDateTime)<<32 | uint64(ft.LowDateTime)
}

type memoryStatusEx struct {
	length              uint32
	memoryLoad          uint32
	totalPhysical       uint64
	availablePhysical   uint64
	totalPageFile       uint64
	availablePageFile   uint64
	totalVirtual        uint64
	availableVirtual    uint64
	availableVirtualExt uint64
}

func hostMemory() (uint64, error) {
	if err := globalMemoryStatusEx.Find(); err != nil {
		return 0, err
	}
	var status memoryStatusEx
	status.length = uint32(unsafe.Sizeof(status))
	ok, _, err := globalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&status)))
	if ok == 0 {
		return 0, fmt.Errorf("GlobalMemoryStatusEx: %w", err)
	}
	return status.availablePhysical, nil
}

type systemPowerStatus struct {
	acLineStatus        byte
	batteryFlag         byte
	batteryLifePercent  byte
	systemStatusFlag    byte
	batteryLifeTime     uint32
	batteryFullLifeTime uint32
}

func hostOnBattery(_ context.Context) (bool, error) {
	if err := getSystemPowerStatus.Find(); err != nil {
		return false, err
	}
	var status systemPowerStatus
	ok, _, err := getSystemPowerStatus.Call(uintptr(unsafe.Pointer(&status)))
	if ok == 0 {
		return false, fmt.Errorf("GetSystemPowerStatus: %w", err)
	}
	return windowsOnBattery(status)
}

func windowsOnBattery(status systemPowerStatus) (bool, error) {
	const noSystemBattery = 128
	if status.acLineStatus == 1 || status.batteryFlag == noSystemBattery {
		return false, nil
	}
	if status.acLineStatus == 0 {
		return true, nil
	}
	return false, errors.New("GetSystemPowerStatus reported an unknown AC power status")
}
