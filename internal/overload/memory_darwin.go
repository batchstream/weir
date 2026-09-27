package overload

import (
	"fmt"
	"log/slog"
	"math"
	"os"
	"runtime"
	"sync"
	"unsafe"

	"github.com/ebitengine/purego"
)

// Apple's public rusage_info_v0 (SDK sys/resource.h), all counters in bytes
// where applicable. proc_pid_rusage copies out exactly the selected flavor.
type rusageV0 struct {
	uuid                                                   [16]byte
	userTime, systemTime, packageWakeups, interruptWakeups uint64
	pageins, wiredSize, residentSize, physicalFootprint    uint64
	procStart, procExit                                    uint64
}

var libproc struct {
	once sync.Once
	// One reference for the entire process lifetime, shared by every Guard.
	// Never unload while any sampler can call the registered system function.
	handle uintptr
	call   func(int32, int32, *rusageV0) int32
	err    error
}

func bindLibproc() {
	defer func() {
		if problem := recover(); problem != nil {
			libproc.err = fmt.Errorf("register proc_pid_rusage: %v", problem)
		}
		if libproc.err != nil {
			if libproc.handle != 0 {
				_ = purego.Dlclose(libproc.handle)
				libproc.handle = 0
			}
			libproc.call = nil
			slog.Error("Darwin process memory unavailable; admission closed", "error", libproc.err)
		}
	}()
	var usage rusageV0
	if unsafe.Sizeof(usage) != 96 || unsafe.Alignof(usage) != 8 || unsafe.Offsetof(usage.physicalFootprint) != 72 {
		libproc.err = fmt.Errorf("unsupported rusage_info_v0 layout")
		return
	}
	libproc.handle, libproc.err = purego.Dlopen("/usr/lib/libproc.dylib", purego.RTLD_NOW|purego.RTLD_LOCAL)
	if libproc.err != nil {
		return
	}
	var symbol uintptr
	symbol, libproc.err = purego.Dlsym(libproc.handle, "proc_pid_rusage")
	if libproc.err != nil {
		return
	}
	purego.RegisterFunc(&libproc.call, symbol)
}

func processObservation() observation {
	libproc.once.Do(bindLibproc)
	if libproc.err != nil {
		return footprintObservation(-1, 0)
	}
	var usage rusageV0
	// A typed, escaping pointer keeps the pointer-free buffer live for this
	// synchronous call. C neither retains it nor calls back into Go. No uintptr
	// retains Go memory. errno is unnecessary: every nonzero status is unknown.
	status := libproc.call(int32(os.Getpid()), 0, &usage)
	runtime.KeepAlive(&usage)
	return footprintObservation(status, usage.physicalFootprint)
}

func footprintObservation(status int32, footprint uint64) observation {
	cg := CgroupSnapshot{State: "not_applicable", Scope: "none"}
	o := observation{source: "darwin_phys_footprint", cgroup: cg}
	// The underlying ledger is signed; reject zero and wrapped negative data.
	if status == 0 && footprint > 0 && footprint <= math.MaxInt64 {
		o.bytes, o.processValid, o.low = footprint, true, true
	}
	return o
}
