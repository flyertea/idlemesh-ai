package idle

import "testing"

func TestPMSetPower(t *testing.T) {
	tests := []struct {
		line    string
		battery bool
		unknown bool
	}{
		{"Now drawing from 'AC Power'\n -InternalBattery-0\t100%; charged\n", false, false},
		{"Now drawing from 'Battery Power'\n -InternalBattery-0\t83%; discharging\n", true, false},
		{"Now drawing from 'UPS Power'", true, false},
		{"No data", false, true},
	}
	for _, tc := range tests {
		onBattery, err := parsePMSetPower(tc.line)
		if onBattery != tc.battery || (err != nil) != tc.unknown {
			t.Errorf("%q: got (%t, %v), want battery=%t, unknown=%t", tc.line, onBattery, err, tc.battery, tc.unknown)
		}
	}
}

func TestMacAvailablePages(t *testing.T) {
	for _, tc := range []struct {
		free, fileBacked, speculative uint32
		want                          uint64
	}{
		{50, 80, 10, 120},
		{50, 5, 10, 50},
		{50, 0, 10, 50},
	} {
		stats := machVMStatistics{free: tc.free, fileBacked: tc.fileBacked, speculative: tc.speculative}
		if pages := macAvailablePages(stats); pages != tc.want {
			t.Errorf("%+v: got %d pages, want %d", stats, pages, tc.want)
		}
	}
}
