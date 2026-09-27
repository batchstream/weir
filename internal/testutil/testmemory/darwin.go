//go:build integration && darwin

// Package testmemory owns only opt-in, bounded native test allocations.
package testmemory

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

const Budget = 256 << 20
const Tolerance = 8 << 20

type Pressure struct {
	base  uint64
	pages [][]byte
}

func Open(t *testing.T) *Pressure {
	t.Helper()
	if os.Getenv("WEIR_M19_NATIVE") != "1" {
		t.Skip("requires explicit M19 native opt-in")
	}
	base := Oracle(t)
	if base > 128<<20 {
		t.Fatal("frozen baseline ceiling 128 MiB", base)
	}
	pressure := &Pressure{base: base}
	t.Cleanup(func() { pressure.Set(t, 0) })
	t.Logf("owned pressure PID=%d baseline=%d budget=%d extra allocation ceiling=256MiB", os.Getpid(), base, Budget)
	return pressure
}
func (p *Pressure) Set(t *testing.T, percent uint64) {
	t.Helper()
	target := uint64(Budget) * percent / 100
	count := 0
	if target > p.base {
		count = int((target - p.base) / (1 << 20))
	}
	if count > 256 {
		t.Fatal("frozen extra allocation ceiling", count)
	}
	for len(p.pages) > count {
		last := len(p.pages) - 1
		if err := syscall.Munmap(p.pages[last]); err != nil {
			t.Error(err)
		}
		p.pages = p.pages[:last]
	}
	for len(p.pages) < count {
		page, err := syscall.Mmap(-1, 0, 1<<20, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANON)
		if err != nil {
			t.Fatal(err)
		}
		p.pages = append(p.pages, page)
		for i := 0; i < len(page); i += os.Getpagesize() {
			page[i] = 1
		}
	}
	t.Logf("mmap target=%d%% allocated=%d bytes", percent, len(p.pages)*(1<<20))
}
func Oracle(t *testing.T) uint64 {
	t.Helper()
	helper := os.Getenv("WEIR_M19_ORACLE")
	if !filepath.IsAbs(helper) {
		t.Fatal("explicit SDK oracle path required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, helper).Output()
	if err != nil {
		t.Fatal("SDK parent oracle", err)
	}
	var pid int
	var footprint uint64
	_, err = fmt.Sscanf(string(output), "%d %d", &pid, &footprint)
	if err != nil || pid != os.Getpid() || footprint == 0 {
		t.Fatal("oracle owner/signal", string(output), err)
	}
	return footprint
}
func Difference(a, b uint64) uint64 {
	if a > b {
		return a - b
	}
	return b - a
}
