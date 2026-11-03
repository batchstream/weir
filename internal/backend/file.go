package backend

import (
	"crypto/x509"
	"errors"
	"io"
	"os"
	"strings"
	"syscall"
	"unicode"
	"unicode/utf8"
)

// ValidText reports whether configured text is valid UTF-8 without control
// characters.
func ValidText(value string) bool {
	return utf8.ValidString(value) && strings.IndexFunc(value, unicode.IsControl) < 0
}

// CertPool reads a configured PEM file of trusted certificates. Its errors do
// not repeat the path.
func CertPool(name string) (*x509.CertPool, error) {
	data, err := ReadRegularFile(name)
	if err != nil {
		return nil, errors.New("CA file unavailable")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(data) {
		return nil, errors.New("invalid CA file")
	}
	return roots, nil
}

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
