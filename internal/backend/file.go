package backend

import (
	"errors"
	"os"
)

// ReadRegularFile reads a configured file. It refuses special files, such as
// a FIFO, whose open could block startup.
func ReadRegularFile(name string) ([]byte, error) {
	info, err := os.Stat(name)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("not a regular file")
	}
	return os.ReadFile(name)
}
