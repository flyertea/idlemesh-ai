package idle

import (
	"testing"
	"unsafe"
)

func TestWindowsPower(t *testing.T) {
	tests := []struct {
		ac      byte
		flag    byte
		battery bool
		unknown bool
	}{
		{1, 255, false, false},
		{0, 255, true, false},
		{0, 1, true, false},
		{255, 128, false, false},
		{255, 255, false, true},
	}
	for _, tc := range tests {
		battery, err := windowsOnBattery(systemPowerStatus{acLineStatus: tc.ac, batteryFlag: tc.flag})
		if battery != tc.battery || (err != nil) != tc.unknown {
			t.Errorf("AC %d and flag %d: got (%t, %v), want battery=%t, unknown=%t", tc.ac, tc.flag, battery, err, tc.battery, tc.unknown)
		}
	}
}

func TestWindowsStructureSizes(t *testing.T) {
	if size := unsafe.Sizeof(memoryStatusEx{}); size != 64 {
		t.Errorf("MEMORYSTATUSEX is %d bytes, want 64", size)
	}
	if size := unsafe.Sizeof(systemPowerStatus{}); size != 12 {
		t.Errorf("SYSTEM_POWER_STATUS is %d bytes, want 12", size)
	}
}
