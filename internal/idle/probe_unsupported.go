//go:build !darwin && !linux && !windows

package idle

import (
	"context"
	"errors"
)

var errUnsupported = errors.New("host resource sampling is unavailable on this operating system")

func hostCPU() (cpuTimes, error) {
	return cpuTimes{}, errUnsupported
}

func hostMemory() (uint64, error) {
	return 0, errUnsupported
}

func hostOnBattery(context.Context) (bool, error) {
	return false, errUnsupported
}
