package idle

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestParseProcStat(t *testing.T) {
	times, err := parseProcStat("cpu  100 10 20 150 9 8 7 6 99 99\ncpu0 99 99 99 99\n")
	if err != nil || times != (cpuTimes{310, 150}) {
		t.Fatalf("unexpected counters: %#v, %v", times, err)
	}
	for _, invalid := range []string{"", "cpu0 1 2 3 4", "cpu 1 2 3", "cpu 1 2 3 nope", "cpu 18446744073709551615 1 0 0"} {
		if _, err := parseProcStat(invalid); err == nil {
			t.Errorf("accepted malformed counters %q", invalid)
		}
	}
}

func TestParseProcMeminfo(t *testing.T) {
	memory, err := parseProcMeminfo("MemTotal: 8192 kB\nMemFree: 128 kB\nMemAvailable: 4096 kB\n")
	if err != nil || memory != 4<<20 {
		t.Fatalf("unexpected memory %d, %v", memory, err)
	}
	for _, invalid := range []string{"MemFree: 8192 kB", "MemAvailable: -1 kB", "MemAvailable: 1 MB", "MemAvailable: 18014398509481984 kB"} {
		if _, err := parseProcMeminfo(invalid); err == nil {
			t.Errorf("accepted malformed memory %q", invalid)
		}
	}
}

func TestLinuxPower(t *testing.T) {
	type supply map[string]string
	tests := []struct {
		name     string
		supplies []supply
		battery  bool
		unknown  bool
	}{
		{name: "desktop"},
		{name: "desktop with mouse", supplies: []supply{{"type": "Battery", "scope": "Device", "status": "Discharging"}}},
		{name: "empty removable slot", supplies: []supply{{"type": "Battery", "present": "0"}}},
		{name: "laptop unplugged", supplies: []supply{{"type": "Battery", "present": "1", "status": "Discharging"}}, battery: true},
		{name: "laptop charging", supplies: []supply{{"type": "Battery", "status": "Charging"}}},
		{name: "full and plugged in", supplies: []supply{{"type": "Battery", "status": "Full"}, {"type": "Mains", "online": "1"}}},
		{name: "full and unplugged", supplies: []supply{{"type": "Battery", "status": "Full"}, {"type": "USB_PD", "online": "0"}}, battery: true},
		{name: "battery draining while connected", supplies: []supply{{"type": "Mains", "online": "1"}, {"type": "Battery", "status": "Discharging"}}, battery: true},
		{name: "ambiguous battery state", supplies: []supply{{"type": "Battery", "status": "Unknown"}}, unknown: true},
		{name: "missing online status", supplies: []supply{{"type": "Battery", "status": "Full"}, {"type": "USB"}}, unknown: true},
		{name: "missing device type", supplies: []supply{{"scope": "System"}}, unknown: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			for i, supply := range tc.supplies {
				path := filepath.Join(root, string(rune('a'+i)))
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
				for key, value := range supply {
					if err := os.WriteFile(filepath.Join(path, key), []byte(value+"\n"), 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			battery, err := linuxOnBattery(context.Background(), root)
			if battery != tc.battery || (err != nil) != tc.unknown {
				t.Fatalf("got (%t, %v), expected battery=%t, unknown=%t", battery, err, tc.battery, tc.unknown)
			}
		})
	}
}
