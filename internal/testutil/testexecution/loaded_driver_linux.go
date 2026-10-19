//go:build linux && (amd64 || arm64)

package testexecution

import (
	"fmt"
	"os"
	"syscall"
)

func checkLoadedDriver(candidate os.FileInfo) error {
	// Pin and identify procfs before following its intentional executable magic
	// link. A normal directory/symlink masquerading as /proc is not kernel proof.
	proc, err := os.Open("/proc/self")
	if err != nil {
		return fmt.Errorf("kernel loaded-driver procfs unavailable: %w", err)
	}
	defer proc.Close()
	var filesystem syscall.Statfs_t
	if err := syscall.Fstatfs(int(proc.Fd()), &filesystem); err != nil {
		return fmt.Errorf("kernel loaded-driver procfs identity unavailable: %w", err)
	}
	const procSuperMagic = 0x9fa0 // Linux uapi linux/magic.h, PROC_SUPER_MAGIC.
	if filesystem.Type != procSuperMagic {
		return fmt.Errorf("kernel loaded-driver identity unavailable: /proc/self is not procfs")
	}
	descriptor, err := syscall.Openat(int(proc.Fd()), "exe", syscall.O_RDONLY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("kernel loaded-driver executable object unavailable: %w", err)
	}
	image := os.NewFile(uintptr(descriptor), "/proc/self/exe")
	if image == nil {
		_ = syscall.Close(descriptor)
		return fmt.Errorf("kernel loaded-driver executable descriptor unavailable")
	}
	defer image.Close()
	loaded, err := image.Stat()
	if err != nil {
		return fmt.Errorf("kernel loaded-driver fstat unavailable: %w", err)
	}
	if !loaded.Mode().IsRegular() || !sameFileSnapshot(candidate, loaded) {
		return fmt.Errorf("driver descriptor is not the kernel-loaded executable backing object")
	}
	return nil
}
