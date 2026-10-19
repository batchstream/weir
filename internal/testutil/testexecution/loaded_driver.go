package testexecution

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
)

func verifyRunningDriver(input Executable, target Target) ([]byte, error) {
	if input.Role != "driver" || target.OS != runtime.GOOS || target.Arch != runtime.GOARCH {
		return nil, fmt.Errorf("driver must be the actual native running test executable")
	}
	if sensitivePath(filepath.ToSlash(input.Path)) {
		return nil, fmt.Errorf("sensitive driver path forbidden")
	}
	file, before, err := openRegularFile(input.Path, maxBinary)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	// The kernel check binds this pinned file descriptor to the loaded executable
	// backing object. Neither an executable pathname nor an init-time path stat is
	// a process image identity. A same-byte copy at a new inode is still rejected.
	if err := checkLoadedDriver(before); err != nil {
		return nil, err
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxBinary+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > maxBinary {
		return nil, fmt.Errorf("driver file exceeds bound")
	}
	after, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !sameFileSnapshot(before, after) {
		return nil, fmt.Errorf("loaded driver backing file changed during read")
	}
	if err := checkLoadedDriver(after); err != nil {
		return nil, err
	}
	object := executableObject{input: input, target: target, raw: raw, stat: after}
	return verifyExecutableObject(object)
}
func sameFileSnapshot(left, right os.FileInfo) bool {
	return os.SameFile(left, right) && left.Size() == right.Size() && left.Mode() == right.Mode() && left.ModTime().Equal(right.ModTime())
}
