package overload

import "golang.org/x/sys/unix"

func processMemoryBudget() uint64 {
	var info unix.Sysinfo_t
	var budget uint64
	if unix.Sysinfo(&info) == nil {
		budget = uint64(info.Totalram) * uint64(info.Unit)
	}
	var limit unix.Rlimit
	if unix.Getrlimit(unix.RLIMIT_AS, &limit) == nil && limit.Cur != unix.RLIM_INFINITY {
		budget = smallerBudget(budget, limit.Cur)
	}
	return budget
}
