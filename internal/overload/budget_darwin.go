package overload

import "golang.org/x/sys/unix"

func processMemoryBudget() uint64 {
	budget, _ := unix.SysctlUint64("hw.memsize")
	var limit unix.Rlimit
	if unix.Getrlimit(unix.RLIMIT_AS, &limit) == nil && limit.Cur != unix.RLIM_INFINITY {
		budget = smallerBudget(budget, limit.Cur)
	}
	return budget
}
