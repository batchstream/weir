//go:build !linux

package overload

func newMemoryProfile() memoryProfile {
	profile := memoryProfile{}
	return profile
}
