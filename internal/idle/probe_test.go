package idle

import "testing"

func TestCPUPercent(t *testing.T) {
	tests := []struct {
		name string
		prev cpuTimes
		next cpuTimes
		want float64
		ok   bool
	}{
		{"first reading", cpuTimes{}, cpuTimes{400, 300}, 0, false},
		{"no tick change", cpuTimes{400, 300}, cpuTimes{400, 300}, 0, false},
		{"counter reset", cpuTimes{400, 300}, cpuTimes{200, 100}, 0, false},
		{"idle reset", cpuTimes{400, 300}, cpuTimes{500, 250}, 0, false},
		{"inconsistent deltas", cpuTimes{400, 300}, cpuTimes{500, 450}, 0, false},
		{"half busy", cpuTimes{400, 300}, cpuTimes{500, 350}, 50, true},
		{"fully busy", cpuTimes{400, 300}, cpuTimes{500, 300}, 100, true},
		{"fully idle", cpuTimes{400, 300}, cpuTimes{500, 400}, 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			load, valid := cpuPercent(tc.prev, tc.next)
			if load != tc.want || valid != tc.ok {
				t.Fatalf("got (%v, %v), want (%v, %v)", load, valid, tc.want, tc.ok)
			}
		})
	}
}
