//go:build integration && linux

package main

import (
	"context"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestNativeSelfObservation(t *testing.T) {
	if os.Getenv("WEIR_CAPACITY_INTEGRATION") != "1" {
		t.Skip("explicit owned Linux fixture required")
	}
	sampler, err := newSampler("client", "self")
	if err != nil {
		t.Fatal(err)
	}
	first := sampler.sample(context.Background(), nil)
	if len(first.Errors) > 0 {
		t.Fatal(first.Errors)
	}
	sampler.Target.StartTicks++
	changed := sampler.sample(context.Background(), nil)
	if len(changed.Errors) == 0 {
		t.Fatal("reused PID identity accepted")
	}
	t.Logf("native pid=%s uid=%d cgroup=%q namespace=%v hash=%s RSS=%d ticks=%d/%d", first.Process.Identity.PID, first.Process.Identity.UID, first.Process.Identity.Cgroup, first.Process.Identity.Namespaces, first.Process.Identity.SHA256, first.RSS, first.Process.UserTicks, first.Process.SystemTicks)
}
func TestNativeObservationExitedTarget(t *testing.T) {
	if os.Getenv("WEIR_CAPACITY_INTEGRATION") != "1" {
		t.Skip("explicit owned Linux fixture required")
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, binary, "-test.run=^TestNativeObservationChild$")
	child.Env = append(os.Environ(), "WEIR_OBSERVATION_CHILD=1")
	if err = child.Start(); err != nil {
		t.Fatal(err)
	}
	pid := strconv.Itoa(child.Process.Pid)
	err = child.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = readProcess(pid, true); err == nil {
		t.Fatal("exited target accepted")
	}
	t.Logf("child PID=%s Wait complete; exited target rejected", pid)
}
func TestNativeObservationChild(t *testing.T) {
	if strings.TrimSpace(os.Getenv("WEIR_OBSERVATION_CHILD")) != "1" {
		t.Skip("owned child only")
	}
}
