package main

import (
	"errors"
	"io"
	"os"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

func validOptions() options {
	opts := options{Mode: "probe", Role: "probe", Owner: "weir-volume-" + strings.Repeat("a", 32), Revision: strings.Repeat("b", 40), Helper: strings.Repeat("c", 64), Image: "sha256:" + strings.Repeat("d", 64), TTL: 55 * time.Second}
	return opts
}

func TestValidation(t *testing.T) {
	opts := validOptions()
	previous := sourceRevision
	sourceRevision = opts.Revision
	defer func() { sourceRevision = previous }()
	if err := validate(opts); err != nil {
		t.Fatal(err)
	}
	cases := []options{opts, opts, opts, opts, opts, opts, opts, opts}
	cases[0].TTL = 301 * time.Second
	cases[1].TTL = 56 * time.Second
	cases[2].TTL = 0
	cases[3].Owner = "../foreign"
	cases[4].Revision = strings.Repeat("e", 40)
	cases[5].Helper = "bad"
	cases[6].Image = "latest"
	cases[7].Mode = "holder"
	for i, candidate := range cases {
		if err := validate(candidate); err == nil {
			t.Errorf("case %d accepted", i)
		}
	}
	opts.Role = "holder"
	opts.Mode = "holder"
	opts.TTL = 300 * time.Second
	if err := validate(opts); err != nil {
		t.Fatal(err)
	}
	opts.Mode = "copy"
	if err := validate(opts); err != nil {
		t.Fatal(err)
	}
	opts.Role = "probe"
	if err := validate(opts); err == nil {
		t.Fatal("copy observer accepted writable probe role")
	}
}

func TestFiniteSignalAndTTL(t *testing.T) {
	stop := make(chan os.Signal, 1)
	stop <- syscall.SIGTERM
	if err := waitForStop(time.Second, stop); err != nil {
		t.Fatal(err)
	}
	if err := waitForStop(time.Millisecond, stop); err == nil {
		t.Fatal("TTL must fail, not claim controlled shutdown")
	}
}

func TestUnsupportedFailsBeforePaths(t *testing.T) {
	if runtime.GOOS == "linux" {
		return
	}
	opts := validOptions()
	err := execute(opts)
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("unsupported execute: %v", err)
	}
}

type boundedWriter struct {
	remaining int
	calls     int
	failure   error
	short     bool
}

func (w *boundedWriter) Write(data []byte) (int, error) {
	w.calls++
	if w.short {
		return 0, nil
	}
	if w.remaining >= len(data) {
		w.remaining -= len(data)
		return len(data), nil
	}
	n := w.remaining
	w.remaining = 0
	return n, w.failure
}

func TestFillRequiresBoundedENOSPC(t *testing.T) {
	cases := []struct {
		name    string
		bytes   int
		failure error
		short   bool
		success bool
	}{
		{name: "ENOSPC", bytes: capacity - 1234, failure: syscall.ENOSPC, success: true},
		{name: "empty", failure: syscall.ENOSPC},
		{name: "wrong error", bytes: 123, failure: errors.New("write failed")},
		{name: "short write", short: true},
		{name: "unbounded", bytes: 64 << 20},
		{name: "late ENOSPC", bytes: capacity + 1, failure: syscall.ENOSPC},
	}
	for _, item := range cases {
		t.Run(item.name, func(t *testing.T) {
			writer := &boundedWriter{remaining: item.bytes, failure: item.failure, short: item.short}
			bytes, attempts, err := boundedFill(writer)
			if (err == nil) != item.success {
				t.Fatalf("bytes=%d attempts=%d error=%v", bytes, attempts, err)
			}
			if attempts > 273 || writer.calls != attempts || bytes > 273*(64<<10) {
				t.Fatal("unbounded fill")
			}
			if item.short && !errors.Is(err, io.ErrShortWrite) {
				t.Fatal("short write accepted")
			}
		})
	}
}
