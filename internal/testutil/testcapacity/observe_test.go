package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestProcessParsing(t *testing.T) {
	good := "VmRSS:\t42 kB\nThreads:\t2\nUid:\t1000 1000 1000 1000\n"
	rss, threads, uid, err := processStatus(good)
	if err != nil || rss != 43008 || threads != 2 || uid != 1000 {
		t.Fatal(rss, threads, uid, err)
	}
	for _, raw := range []string{
		"",
		strings.ReplaceAll(good, "kB", "MB"),
		strings.ReplaceAll(good, "42", "18446744073709551615"),
		strings.ReplaceAll(good, "42", "-1"),
		good + "Threads: 2\n",
		strings.ReplaceAll(good, "1000 1000 1000 1000", "1000 0 1000 1000"),
	} {
		if _, _, _, err := processStatus(raw); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
	fields := make([]string, 24)
	for i := range fields {
		fields[i] = "0"
	}
	fields[0] = "S"
	fields[11] = "3"
	fields[12] = "5"
	fields[19] = "123"
	raw := "12 (name (with spaces)) " + strings.Join(fields, " ")
	start, user, system, err := processStat(raw)
	if err != nil || start != 123 || user != 3 || system != 5 {
		t.Fatal(start, user, system, err)
	}
	for _, bad := range []string{
		"",
		strings.Replace(raw, " S ", " Z ", 1),
		strings.Replace(raw, " 123 ", " 18446744073709551616 ", 1),
	} {
		if _, _, _, err = processStat(bad); err == nil {
			t.Fatal("bad stat accepted")
		}
	}
}

func TestBoundedObservationFiles(t *testing.T) {
	root := t.TempDir()
	name := filepath.Join(root, "file")
	if _, err := boundedFile(name); !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, []byte(strings.Repeat("x", fileLimit+1)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := boundedFile(name); err == nil {
		t.Fatal("long file accepted")
	}
	if err := os.Chmod(name, 0); err != nil {
		t.Fatal(err)
	}
	if os.Geteuid() != 0 {
		if _, err := boundedFile(name); !errors.Is(err, os.ErrPermission) {
			t.Fatal(err)
		}
	}
	if _, err := boundedNames(root, 0); err == nil {
		t.Fatal("directory bound")
	}
}

func TestTargetIdentityAndJVM(t *testing.T) {
	target := ProcessIdentity{UID: 1000, Cgroup: "0::/\n", Namespaces: map[string]string{"pid": "pid:[1]"}}
	if err := sameContainer(target, target); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []ProcessIdentity{
		{UID: 0, Cgroup: "0::/\n", Namespaces: target.Namespaces},
		{UID: 1001, Cgroup: "0::/\n", Namespaces: target.Namespaces},
		{UID: 1000, Cgroup: "0::/other\n", Namespaces: target.Namespaces},
		{UID: 1000, Cgroup: "0::/\n", Namespaces: map[string]string{"pid": "pid:[2]"}},
	} {
		if err := sameContainer(target, bad); err == nil {
			t.Fatal("identity accepted")
		}
	}
	if _, err := newSampler("unknown", "1"); err == nil {
		t.Fatal("role")
	}
	if _, err := newSampler("client", "1"); err == nil {
		t.Fatal("client target")
	}
	root := t.TempDir()
	if _, err := findJVM(root); err == nil {
		t.Fatal("missing JVM")
	}
	for _, pid := range []string{"3", "4"} {
		dir := filepath.Join(root, pid)
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "comm"), []byte("java\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "cmdline"), []byte("java\x00org.elasticsearch.bootstrap.Elasticsearch\x00"), 0600); err != nil {
			t.Fatal(err)
		}
		if pid == "3" {
			got, err := findJVM(root)
			if got != "3" || err != nil {
				t.Fatal(got, err)
			}
		}
	}
	if _, err := findJVM(root); err == nil {
		t.Fatal("duplicate JVM")
	}
	if err := os.Remove(filepath.Join(root, "3", "cmdline")); err != nil {
		t.Fatal(err)
	}
	if _, err := findJVM(root); err == nil {
		t.Fatal("exited JVM")
	}
}

func TestObservationCgroupParsing(t *testing.T) {
	files := map[string]string{
		"memory.current":        "10\n",
		"memory.max":            "100\n",
		"memory.swap.max":       "0\n",
		"pids.current":          "1\n",
		"pids.max":              "8\n",
		"cpu.max":               "100000 100000\n",
		"cpu.stat":              "usage_usec 1\nuser_usec 1\nsystem_usec 0\nnr_periods 1\nnr_throttled 0\nthrottled_usec 0\n",
		"memory.events":         "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n",
		"cpuset.cpus.effective": "0-3\n",
		"io.stat":               "",
	}
	if err := validateCgroup(files); err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string]string{
		"memory.current":        "-1",
		"memory.max":            "max",
		"pids.max":              "18446744073709551616",
		"cpu.max":               "100 percent",
		"cpu.stat":              "usage_usec 1\nusage_usec 2",
		"memory.events":         "oom 0",
		"io.stat":               "8:0 rbytes=-1",
		"cpuset.cpus.effective": "",
	} {
		original := files[name]
		files[name] = raw
		if err := validateCgroup(files); err == nil {
			t.Fatal(name)
		}
		files[name] = original
	}
	delete(files, "io.stat")
	if err := validateCgroup(files); err == nil {
		t.Fatal("missing io.stat")
	}
}

func TestObservationHTTPBounds(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/large" {
			_, _ = io.WriteString(w, strings.Repeat("x", fileLimit+1))
			return
		}
		<-r.Context().Done()
	}))
	defer server.Close()
	c, err := newClient(server.URL, "", 62)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, _, err = c.request(context.Background(), "GET", "/large", nil); err == nil {
		t.Fatal("response bound")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, _, err = c.request(ctx, "GET", "/timeout", nil)
	if err == nil || time.Since(started) > time.Second {
		t.Fatal(err)
	}
}

func TestEvidenceBlockedOutputAndCancellation(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer := &evidenceWriter{File: w, Context: ctx}
	started := time.Now()
	_, err = writer.Write([]byte(strings.Repeat("x", 1<<20)))
	if err == nil || time.Since(started) > 2*time.Second {
		t.Fatal(err)
	}
	cancel()
	if _, err = writer.Write([]byte("x")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	writer.Context = context.Background()
	writer.Bytes = 64 << 20
	if _, err = writer.Write([]byte("x")); err == nil {
		t.Fatal("total bound")
	}
}

func TestEvidenceJSONPipe(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	writer := &evidenceWriter{File: w, Context: context.Background()}
	encoder := json.NewEncoder(writer)
	value := map[string]int{"test": 1}
	if err = encoder.Encode(value); err != nil {
		t.Fatal(err)
	}
	raw := make([]byte, 11)
	if _, err = io.ReadFull(r, raw); err != nil {
		t.Fatal(err)
	}
	if string(raw) != "{\"test\":1}\n" {
		t.Fatal(fmt.Sprint(raw))
	}
}

func TestEvidenceInheritedBlockingPipe(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("Unix pipe fixture")
	}
	if os.Getenv("WEIR_EVIDENCE_PIPE_CHILD") == "1" {
		output, err := evidencePipe(os.Stdout)
		if err != nil {
			fmt.Fprintln(os.Stderr, "pipe-open:", err)
			t.Fatal(err)
		}
		writer := &evidenceWriter{File: output, Context: context.Background()}
		_, err = writer.Write([]byte(strings.Repeat("x", 1<<20)))
		output.Close()
		os.Stdout.Close()
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			fmt.Fprintln(os.Stderr, "pipe-write:", err)
			t.Fatal(err)
		}
		fmt.Fprintln(os.Stderr, "bounded-inherited-output")
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, executable, "-test.run=^TestEvidenceInheritedBlockingPipe$")
	child.Env = append(os.Environ(), "WEIR_EVIDENCE_PIPE_CHILD=1")
	var stderr bytes.Buffer
	child.Stderr = &stderr
	pipe, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer pipe.Close()
	started := time.Now()
	if err = child.Start(); err != nil {
		t.Fatal(err)
	}
	err = child.Wait()
	if err != nil || !strings.Contains(stderr.String(), "bounded-inherited-output") || time.Since(started) > 3*time.Second {
		t.Fatalf("Wait=%v stderr=%s", err, stderr.String())
	}
}

func TestEvidenceInputCancellation(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	done := make(chan error, 1)
	go func() {
		var raw [1]byte
		_, err := r.Read(raw[:])
		done <- err
	}()
	r.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("read survived close")
		}
	case <-time.After(time.Second):
		t.Fatal("input reader not joined")
	}
}

func TestJVMModuleMarkerAndLauncher(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "7")
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "comm"), []byte("java\n"), 0600); err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(directory, "cmdline")
	for _, raw := range []string{
		"java\x00-m\x00org.elasticsearch.server/org.elasticsearch.bootstrap.Elasticsearch\x00",
		"java\x00org.elasticsearch.bootstrap.Elasticsearch\x00",
	} {
		if err := os.WriteFile(name, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		pid, err := findJVM(root)
		if err != nil || pid != "7" {
			t.Fatal(pid, err)
		}
	}
	for _, raw := range []string{
		"java\x00org.elasticsearch.launcher.CliToolLauncher\x00",
		"java\x00-Dother=org.elasticsearch.bootstrap.Elasticsearch\x00",
	} {
		if err := os.WriteFile(name, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := findJVM(root); err == nil {
			t.Fatal("launcher/unrelated argument accepted")
		}
	}
}
