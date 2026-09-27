//go:build !darwin

package overload

func processObservation() observation {
	cg := CgroupSnapshot{State: "not_applicable", Scope: "none"}
	o := observation{bytes: goBytes(), source: "go_sys_minus_released", processValid: true, cgroup: cg, low: true}
	return o
}
