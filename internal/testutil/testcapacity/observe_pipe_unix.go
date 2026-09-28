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

// Called only after the sampling reader has joined. The descriptor belongs to
// a pollable nonblocking pipe; EAGAIN means no premature input is pending.
func checkObservationInput(input *os.File) error {
	connection, err := input.SyscallConn()
	if err != nil {
		return err
	}
	var readErr error
	err = connection.Control(func(fd uintptr) {
		var raw [1]byte
		n, e := syscall.Read(int(fd), raw[:])
		switch {
		case n > 0:
			readErr = errors.New("observer control byte received")
		case e == nil:
			readErr = errors.New("observer EOF before completion")
		case !errors.Is(e, syscall.EAGAIN) && !errors.Is(e, syscall.EWOULDBLOCK):
			readErr = e
		}
	})
	return errors.Join(err, readErr)
}
