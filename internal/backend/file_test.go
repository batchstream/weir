package backend

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestReadRegularFileRefusesSpecialFiles(t *testing.T) {
	directory := t.TempDir()
	regular := filepath.Join(directory, "regular")
	if err := os.WriteFile(regular, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if data, err := ReadRegularFile(regular); err != nil || string(data) != "data" {
		t.Fatal("regular file", string(data), err)
	}
	fifo := filepath.Join(directory, "fifo")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{fifo, directory, filepath.Join(directory, "missing")} {
		done := make(chan error, 1)
		go func() { _, err := ReadRegularFile(name); done <- err }()
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("special or missing file accepted", name)
			}
		case <-time.After(time.Second):
			t.Fatal("read blocked", name)
		}
	}
}
