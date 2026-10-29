package backend

import (
	"errors"
	"io"
	"os"
	"syscall"
)

// ReadRegularFile reads a configured file. Opening without blocking and
// checking the opened file refuses special files, such as a FIFO.
func ReadRegularFile(name string) ([]byte, error) {
	file, err := os.OpenFile(name, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("not a regular file")
	}
	return io.ReadAll(file)
}
