//go:build darwin && arm64

package testexecution

import (
	"fmt"
	"os"
	"reflect"
	"runtime"
	"sync"
	"syscall"
	"unsafe"

	"github.com/ebitengine/purego"
)

// Apple's public sys/proc_info.h and libproc.h define these pointer-free arm64
// records and proc_pidinfo's C ABI. Sizes/offsets were independently checked by
// compiling the SDK headers; reject a different layout before any native call.
type driverRegionInfo struct {
	Protection, MaxProtection, Inheritance, Flags                                                                                                                                        uint32
	Offset                                                                                                                                                                               uint64
	Behavior, UserWiredCount, UserTag, PagesResident, PagesSharedPrivate, PagesSwapped, PagesDirtied, RefCount, ShadowDepth, ShareMode, PrivateResident, SharedResident, ObjectID, Depth uint32
	Address, Size                                                                                                                                                                        uint64
}
type driverVnodeStat struct {
	Dev                                                                                                        uint32
	Mode, Links                                                                                                uint16
	Inode                                                                                                      uint64
	UID, GID                                                                                                   uint32
	AccessTime, AccessNsec, ModifyTime, ModifyNsec, ChangeTime, ChangeNsec, BirthTime, BirthNsec, Size, Blocks int64
	BlockSize                                                                                                  int32
	Flags, Generation, RDev                                                                                    uint32
	Spare                                                                                                      [2]int64
}
type driverVnodeInfo struct {
	Stat      driverVnodeStat
	Type, Pad int32
	FSID      [2]int32
}
type driverVnodePath struct {
	Info driverVnodeInfo
	Path [1024]byte
}
type driverRegionPath struct {
	Region driverRegionInfo
	Vnode  driverVnodePath
}

// The five positional arguments are the public native ABI, not a Go options
// API. Call only synchronously; libproc never retains the Go buffer or calls back.
var driverLibproc struct {
	once   sync.Once
	handle uintptr
	call   func(int32, int32, uint64, *driverRegionPath, int32) int32
	err    error
}

func driverRegionLayoutValid() bool {
	var region driverRegionInfo
	var stat driverVnodeStat
	var vnode driverVnodeInfo
	var path driverVnodePath
	var output driverRegionPath
	return unsafe.Sizeof(region) == 96 && unsafe.Alignof(region) == 8 && unsafe.Offsetof(region.Protection) == 0 && unsafe.Offsetof(region.Address) == 80 && unsafe.Offsetof(region.Size) == 88 &&
		unsafe.Sizeof(stat) == 136 && unsafe.Alignof(stat) == 8 && unsafe.Offsetof(stat.Dev) == 0 && unsafe.Offsetof(stat.Mode) == 4 && unsafe.Offsetof(stat.Inode) == 8 && unsafe.Offsetof(stat.Size) == 88 && unsafe.Offsetof(stat.ModifyTime) == 40 && unsafe.Offsetof(stat.ModifyNsec) == 48 && unsafe.Offsetof(stat.ChangeTime) == 56 && unsafe.Offsetof(stat.ChangeNsec) == 64 &&
		unsafe.Sizeof(vnode) == 152 && unsafe.Offsetof(vnode.Type) == 136 && unsafe.Offsetof(vnode.FSID) == 144 && unsafe.Sizeof(path) == 1176 && unsafe.Offsetof(path.Path) == 152 &&
		unsafe.Sizeof(output) == 1272 && unsafe.Alignof(output) == 8 && unsafe.Offsetof(output.Region) == 0 && unsafe.Offsetof(output.Vnode) == 96
}
func bindDriverLibproc() {
	defer func() {
		if problem := recover(); problem != nil {
			driverLibproc.err = fmt.Errorf("register loaded-driver proc_pidinfo: %v", problem)
		}
		if driverLibproc.err != nil {
			if driverLibproc.handle != 0 {
				_ = purego.Dlclose(driverLibproc.handle)
				driverLibproc.handle = 0
			}
			driverLibproc.call = nil
		}
	}()
	if !driverRegionLayoutValid() {
		driverLibproc.err = fmt.Errorf("unsupported Darwin loaded-driver proc_regionwithpathinfo ABI")
		return
	}
	driverLibproc.handle, driverLibproc.err = purego.Dlopen("/usr/lib/libproc.dylib", purego.RTLD_NOW|purego.RTLD_LOCAL)
	if driverLibproc.err != nil {
		return
	}
	var symbol uintptr
	symbol, driverLibproc.err = purego.Dlsym(driverLibproc.handle, "proc_pidinfo")
	if driverLibproc.err != nil {
		return
	}
	purego.RegisterFunc(&driverLibproc.call, symbol)
	// Keep this libproc reference for process lifetime; never unload a callable ABI.
}

//go:noinline
func driverImageAnchor() {}

func checkLoadedDriver(candidate os.FileInfo) error {
	driverLibproc.once.Do(bindDriverLibproc)
	if driverLibproc.err != nil {
		return fmt.Errorf("kernel loaded-driver identity unavailable: %w", driverLibproc.err)
	}
	stat, ok := candidate.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("driver descriptor has no native Darwin fstat")
	}
	pc := uint64(reflect.ValueOf(driverImageAnchor).Pointer())
	var output driverRegionPath
	const regionPathFlavor = 8  // PROC_PIDREGIONPATHINFO, public sys/proc_info.h.
	const executeProtection = 4 // VM_PROT_EXECUTE, public mach/vm_prot.h.
	status := driverLibproc.call(int32(os.Getpid()), regionPathFlavor, pc, &output, int32(unsafe.Sizeof(output)))
	runtime.KeepAlive(&output)
	if status != int32(unsafe.Sizeof(output)) {
		return fmt.Errorf("kernel loaded-driver region observation unavailable: proc_pidinfo returned %d, need %d", status, unsafe.Sizeof(output))
	}
	region, vnode := output.Region, output.Vnode.Info.Stat
	if region.Protection&executeProtection == 0 || region.Address > pc || pc-region.Address >= region.Size || vnode.Inode == 0 || vnode.Mode&syscall.S_IFMT != syscall.S_IFREG {
		return fmt.Errorf("kernel loaded-driver region is not a file-backed executable mapping")
	}
	// Use kernel vnode identity, not vip_path: rename/replacement can make a path
	// point elsewhere while this VM mapping still owns the original backing file.
	if vnode.Dev != uint32(stat.Dev) || vnode.Inode != stat.Ino || vnode.Size != candidate.Size() || vnode.ModifyTime != stat.Mtimespec.Sec || vnode.ModifyNsec != stat.Mtimespec.Nsec || vnode.ChangeTime != stat.Ctimespec.Sec || vnode.ChangeNsec != stat.Ctimespec.Nsec {
		return fmt.Errorf("driver descriptor is not the kernel-loaded executable backing object")
	}
	return nil
}
