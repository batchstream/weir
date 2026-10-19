//go:build !((linux && (amd64 || arm64)) || (darwin && arm64))

package testexecution

import (
	"fmt"
	"os"
	"runtime"
)

func checkLoadedDriver(os.FileInfo) error {
	return fmt.Errorf("kernel loaded-driver identity unavailable on %s/%s; validation is unsupported", runtime.GOOS, runtime.GOARCH)
}
