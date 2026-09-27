package overload

func newMemoryProfile() memoryProfile {
	profile := memoryProfile{proc: "/proc/self"}
	return profile
}
