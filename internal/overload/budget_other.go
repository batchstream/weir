//go:build !linux && !darwin

package overload

func processMemoryBudget() uint64 { return 0 }
