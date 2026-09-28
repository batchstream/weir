//go:build linux || darwin

package main

import (
	"errors"
	"os"
	"syscall"
)

// Own a nonblocking duplicate of an inherited pipe so the standard Go poller
// can implement deadlines and interrupt Read on Close. No writer goroutine.
func evidencePipe(source *os.File) (*os.File, error) {
	info, err := source.Stat()
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeNamedPipe == 0 {
		return nil, errors.New("evidence stdio must be a pipe")
	}
	fd, err := syscall.Dup(int(source.Fd()))
	if err != nil {
		return nil, err
	}
	syscall.CloseOnExec(fd)
	if err = syscall.SetNonblock(fd, true); err != nil {
		syscall.Close(fd)
		return nil, err
	}
	file := os.NewFile(uintptr(fd), "evidence-pipe")
	return file, nil
}
