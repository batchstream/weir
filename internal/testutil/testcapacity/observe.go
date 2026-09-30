package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const fileLimit = 256 << 10

var observationEpoch = time.Now()

type ProcessIdentity struct {
	PID        string            `json:"pid"`
	StartTicks uint64            `json:"start_ticks"`
	SHA256     string            `json:"exe_sha256"`
	UID        uint64            `json:"uid"`
	Cgroup     string            `json:"cgroup"`
	Namespaces map[string]string `json:"namespaces"`
}
type ProcessSample struct {
	Identity    ProcessIdentity `json:"identity"`
	RSS         uint64          `json:"rss_bytes"`
	FD          int             `json:"fd"`
	Threads     uint64          `json:"threads"`
	UserTicks   uint64          `json:"user_ticks"`
	SystemTicks uint64          `json:"system_ticks"`
	Status      string          `json:"status"`
	Stat        string          `json:"stat"`
}
type Sample struct {
	Sequence           uint64            `json:"sequence"`
	Role               string            `json:"role"`
	MonotonicNS        int64             `json:"monotonic_ns"`
	EndMonotonicNS     int64             `json:"end_monotonic_ns"`
	DurationNS         int64             `json:"duration_ns"`
	Time               time.Time         `json:"time"`
	End                time.Time         `json:"end"`
	Files              map[string]string `json:"files"`
	Process            ProcessSample     `json:"process"`
	Observer           ProcessSample     `json:"observer"`
	FD                 int               `json:"fd"`
	RSS                uint64            `json:"rss_bytes"`
	Goroutines         int               `json:"goroutines,omitempty"`
	GOMAXPROCS         int               `json:"gomaxprocs,omitempty"`
	HeapAlloc          uint64            `json:"go_heap_alloc_bytes,omitempty"`
	Metrics            string            `json:"metrics,omitempty"`
	DB                 string            `json:"db,omitempty"`
	Errors             []string          `json:"errors,omitempty"`
	NetworkOmitted     bool              `json:"network_omitted,omitempty"`
	IdentityHashCached bool              `json:"identity_hash_cached,omitempty"`
}
type Sampler struct {
	Role, PID        string
	Target, Observer ProcessIdentity
	Previous         *Sample
	Sequence         uint64
	SkipNetwork      bool
	VerifyHash       bool
}

func boundedFile(name string) (string, error) {
	f, err := os.Open(name)
	if err != nil {
		return "", err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, fileLimit+1))
	if len(b) > fileLimit {
		return "", errors.New("file bound")
	}
	return string(b), err
}
func boundedNames(name string, limit int) ([]string, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	names, err := f.Readdirnames(limit + 1)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if len(names) > limit {
		return nil, errors.New("directory entry bound")
	}
	return names, nil
}
func unsigned(raw string) (uint64, error) {
	if raw == "" || strings.Trim(raw, "0123456789") != "" {
		return 0, errors.New("unsigned integer required")
	}
	return strconv.ParseUint(raw, 10, 64)
}
func processStat(raw string) (start, user, system uint64, err error) {
	// comm can contain spaces and parentheses. Fields after its last ')' start at 3.
	close := strings.LastIndex(raw, ") ")
	if close < 0 {
		err = errors.New("process stat comm")
		return
	}
	fields := strings.Fields(raw[close+2:])
	if len(fields) < 22 || fields[0] == "Z" || fields[0] == "X" {
		err = errors.New("process exited or short stat")
		return
	}
	user, err = unsigned(fields[11])
	if err != nil {
		return
	}
	system, err = unsigned(fields[12])
	if err != nil {
		return
	}
	start, err = unsigned(fields[19])
	if start == 0 && err == nil {
		err = errors.New("zero start time")
	}
	return
}
func processStatus(raw string) (rss, threads, uid uint64, err error) {
	seen := map[string]bool{}
	for _, line := range strings.Split(raw, "\n") {
		f := strings.Fields(line)
		if len(f) == 0 {
			continue
		}
		key := f[0]
		if key != "VmRSS:" && key != "Threads:" && key != "Uid:" {
			continue
		}
		if seen[key] {
			err = errors.New("duplicate process status")
			return
		}
		seen[key] = true
		switch key {
		case "VmRSS:":
			if len(f) != 3 || f[2] != "kB" {
				err = errors.New("RSS unit")
				return
			}
			rss, err = unsigned(f[1])
			if err != nil {
				return
			}
			if rss == 0 || rss > math.MaxUint64/1024 {
				err = errors.New("RSS range")
				return
			}
			rss *= 1024
		case "Threads:":
			if len(f) != 2 {
				err = errors.New("thread fields")
				return
			}
			threads, err = unsigned(f[1])
			if err != nil {
				return
			}
			if threads == 0 {
				err = errors.New("zero threads")
				return
			}
		case "Uid:":
			if len(f) != 5 {
				err = errors.New("UID fields")
				return
			}
			uid, err = unsigned(f[1])
			if err != nil {
				return
			}
			for _, value := range f[2:] {
				if value != f[1] {
					err = errors.New("mixed process UID")
					return
				}
			}
		}
	}
	if len(seen) != 3 {
		err = errors.New("missing process status")
	}
	return
}
func executableHash(pid string) (string, error) {
	f, err := os.Open("/proc/" + pid + "/exe")
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(f, (128<<20)+1))
	if n > 128<<20 {
		return "", errors.New("executable size bound")
	}
	return hex.EncodeToString(h.Sum(nil)), err
}
func readProcess(pid string, hash bool) (ProcessSample, error) {
	p := ProcessSample{}
	p.Identity.PID = pid
	var err error
	p.Stat, err = boundedFile("/proc/" + pid + "/stat")
	if err != nil {
		return p, err
	}
	p.Identity.StartTicks, p.UserTicks, p.SystemTicks, err = processStat(p.Stat)
	if err != nil {
		return p, err
	}
	p.Status, err = boundedFile("/proc/" + pid + "/status")
	if err != nil {
		return p, err
	}
	p.RSS, p.Threads, p.Identity.UID, err = processStatus(p.Status)
	if err != nil {
		return p, err
	}
	p.Identity.Cgroup, err = boundedFile("/proc/" + pid + "/cgroup")
	if err != nil {
		return p, err
	}
	if hash {
		p.Identity.SHA256, err = executableHash(pid)
		if err != nil {
			return p, err
		}
	}
	p.Identity.Namespaces = map[string]string{}
	for _, name := range []string{"pid", "mnt", "cgroup", "net", "user"} {
		value, e := os.Readlink("/proc/" + pid + "/ns/" + name)
		if e != nil {
			return p, e
		}
		p.Identity.Namespaces[name] = value
	}
	names, err := boundedNames("/proc/"+pid+"/fd", 4096)
	p.FD = len(names)
	return p, err
}
func sameContainer(target, observer ProcessIdentity) error {
	if target.UID == 0 || target.UID != observer.UID || target.Cgroup != "0::/\n" || target.Cgroup != observer.Cgroup || !reflect.DeepEqual(target.Namespaces, observer.Namespaces) {
		return errors.New("same nonroot UID/namespaces and visible cgroup-v2 leaf required; ancestors unknown")
	}
	return nil
}
func findJVM(root string) (string, error) {
	names, err := boundedNames(root, 1024)
	if err != nil {
		return "", err
	}
	found := ""
	for _, name := range names {
		if _, e := unsigned(name); e != nil {
			continue
		}
		comm, e := boundedFile(filepath.Join(root, name, "comm"))
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return "", e
		}
		if comm != "java\n" {
			continue
		}
		raw, e := boundedFile(filepath.Join(root, name, "cmdline"))
		if e != nil {
			return "", e
		}
		match := false
		for _, arg := range strings.Split(raw, "\x00") {
			if arg == "org.elasticsearch.bootstrap.Elasticsearch" || arg == "org.elasticsearch.server/org.elasticsearch.bootstrap.Elasticsearch" {
				match = true
			}
		}
		if !match {
			continue
		}
		if found != "" {
			return "", errors.New("multiple Elasticsearch JVMs")
		}
		found = name
	}
	if found == "" {
		return "", errors.New("Elasticsearch JVM not found")
	}
	return found, nil
}
func newSampler(role, pid string) (*Sampler, error) {
	if role != "weir" && role != "es" && role != "client" {
		return nil, errors.New("explicit observer role required")
	}
	if role == "client" {
		if pid != "self" {
			return nil, errors.New("client must self sample")
		}
		pid = strconv.Itoa(os.Getpid())
	}
	if role == "es" {
		found, err := findJVM("/proc")
		if err != nil {
			return nil, err
		}
		if pid != "java" && pid != found {
			return nil, errors.New("ES target mismatch")
		}
		pid = found
	}
	if _, err := unsigned(pid); err != nil {
		return nil, err
	}
	if role == "weir" {
		comm, err := boundedFile("/proc/" + pid + "/comm")
		if err != nil {
			return nil, err
		}
		if comm != "weir\n" {
			return nil, errors.New("Weir target comm mismatch")
		}
	}
	target, err := readProcess(pid, true)
	if err != nil {
		return nil, err
	}
	observer, err := readProcess(strconv.Itoa(os.Getpid()), true)
	if err != nil {
		return nil, err
	}
	if err = sameContainer(target.Identity, observer.Identity); err != nil {
		return nil, err
	}
	mount, err := boundedFile("/proc/self/mountinfo")
	if err != nil {
		return nil, err
	}
	visible := false
	for _, line := range strings.Split(mount, "\n") {
		f := strings.Fields(line)
		if len(f) > 6 && f[3] == "/" && f[4] == "/sys/fs/cgroup" && strings.Contains(line, " - cgroup2 ") {
			visible = true
		}
	}
	if !visible {
		return nil, errors.New("unsupported cgroup-v2 mount profile")
	}
	s := &Sampler{Role: role, PID: pid, Target: target.Identity, Observer: observer.Identity}
	return s, nil
}
func counterFile(raw string) (map[string]uint64, error) {
	result := map[string]uint64{}
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		f := strings.Fields(line)
		if len(f) != 2 {
			return nil, errors.New("counter fields")
		}
		if _, ok := result[f[0]]; ok {
			return nil, errors.New("duplicate counter")
		}
		v, err := unsigned(f[1])
		if err != nil {
			return nil, err
		}
		result[f[0]] = v
	}
	return result, nil
}
func validateCgroup(files map[string]string) error {
	for _, name := range []string{"memory.current", "memory.max", "memory.swap.max", "pids.current", "pids.max"} {
		if _, err := unsigned(strings.TrimSpace(files[name])); err != nil {
			return fmt.Errorf("%s: finite numeric profile required: %w", name, err)
		}
	}
	f := strings.Fields(files["cpu.max"])
	if len(f) != 2 {
		return errors.New("cpu.max fields")
	}
	for _, v := range f {
		n, e := unsigned(v)
		if e != nil || n == 0 {
			return errors.New("cpu.max finite quota/period")
		}
	}
	for name, keys := range map[string][]string{"memory.events": {"low", "high", "max", "oom", "oom_kill", "oom_group_kill"}, "cpu.stat": {"usage_usec", "user_usec", "system_usec", "nr_periods", "nr_throttled", "throttled_usec"}} {
		values, e := counterFile(files[name])
		if e != nil {
			return e
		}
		for _, key := range keys {
			if _, ok := values[key]; !ok {
				return fmt.Errorf("%s missing %s", name, key)
			}
		}
	}
	if strings.TrimSpace(files["cpuset.cpus.effective"]) == "" {
		return errors.New("empty cpuset")
	}
	// Empty io.stat is valid on this tmpfs-only fixture. Absence/read error is not.
	raw, ok := files["io.stat"]
	if !ok {
		return errors.New("missing io.stat")
	}
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return errors.New("io.stat fields")
		}
		device := strings.Split(fields[0], ":")
		if len(device) != 2 {
			return errors.New("io.stat device")
		}
		for _, v := range device {
			if _, e := unsigned(v); e != nil {
				return e
			}
		}
		for _, f := range fields[1:] {
			pair := strings.Split(f, "=")
			if len(pair) != 2 {
				return errors.New("io.stat counter")
			}
			if _, e := unsigned(pair[1]); e != nil {
				return e
			}
		}
	}
	return nil
}
func (o *Sampler) sample(ctx context.Context, diagnostic *Client) Sample {
	s := Sample{Sequence: o.Sequence, Role: o.Role, Time: time.Now().UTC(), MonotonicNS: time.Since(observationEpoch).Nanoseconds(), Files: map[string]string{}}
	record := func(err error) {
		if err != nil {
			s.Errors = append(s.Errors, err.Error())
		}
	}
	var err error
	hash := !o.SkipNetwork || o.VerifyHash
	s.IdentityHashCached = !hash
	s.Process, err = readProcess(o.PID, hash)
	record(err)
	s.Observer, err = readProcess(o.Observer.PID, hash)
	record(err)
	if !hash {
		s.Process.Identity.SHA256 = o.Target.SHA256
		s.Observer.Identity.SHA256 = o.Observer.SHA256
	}
	if !reflect.DeepEqual(s.Process.Identity, o.Target) || !reflect.DeepEqual(s.Observer.Identity, o.Observer) {
		record(errors.New("process identity changed"))
	}
	record(sameContainer(s.Process.Identity, s.Observer.Identity))
	if o.Role == "es" {
		pid, e := findJVM("/proc")
		record(e)
		if pid != o.PID {
			record(errors.New("JVM target changed"))
		}
	}
	for _, name := range []string{"memory.current", "memory.max", "memory.swap.max", "memory.events", "cpu.max", "cpu.stat", "pids.current", "pids.max", "cpuset.cpus.effective", "io.stat"} {
		value, e := boundedFile("/sys/fs/cgroup/" + name)
		record(e)
		if e == nil {
			s.Files[name] = value
		}
	}
	processFiles := []string{"limits"}
	if o.SkipNetwork {
		s.NetworkOmitted = true
	} else {
		processFiles = append(processFiles, "net/tcp", "net/tcp6")
	}
	for _, name := range processFiles {
		value, e := boundedFile("/proc/" + o.PID + "/" + name)
		record(e)
		if e == nil {
			s.Files[name] = value
		}
	}
	s.Files["status"] = s.Process.Status
	s.Files["stat"] = s.Process.Stat
	s.Files["cgroup"] = s.Process.Identity.Cgroup
	record(validateCgroup(s.Files))
	s.RSS = s.Process.RSS
	s.FD = s.Process.FD
	if o.Role == "client" {
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		s.Goroutines = runtime.NumGoroutine()
		s.GOMAXPROCS = runtime.GOMAXPROCS(0)
		s.HeapAlloc = memory.HeapAlloc
	}
	if diagnostic != nil {
		path := "/metrics"
		if o.Role == "es" {
			path = "/_nodes/_local/stats/process,jvm,os,fs,thread_pool,http?filter_path=nodes.*.process,nodes.*.jvm.mem,nodes.*.jvm.threads,nodes.*.os.cpu,nodes.*.fs.io_stats,nodes.*.thread_pool.write,nodes.*.thread_pool.get,nodes.*.http.current_open,nodes.*.http.total_opened"
		}
		call, cancel := context.WithTimeout(ctx, time.Second)
		code, raw, e := diagnostic.request(call, "GET", path, nil)
		cancel()
		record(e)
		if code != 200 {
			record(fmt.Errorf("diagnostic HTTP %d", code))
		}
		if e == nil && code == 200 {
			if o.Role == "es" {
				s.DB = string(raw)
			} else {
				s.Metrics = string(raw)
			}
		}
	}
	after, e := readProcess(o.PID, hash)
	record(e)
	if !hash {
		after.Identity.SHA256 = o.Target.SHA256
	}
	if !reflect.DeepEqual(after.Identity, o.Target) {
		record(errors.New("target exited/replaced during sample"))
	}
	if o.Previous != nil {
		previous := o.Previous
		if s.Process.UserTicks < previous.Process.UserTicks || s.Process.SystemTicks < previous.Process.SystemTicks || s.Observer.UserTicks < previous.Observer.UserTicks || s.Observer.SystemTicks < previous.Observer.SystemTicks {
			record(errors.New("process CPU counter decreased"))
		}
		for _, name := range []string{"cpu.stat", "memory.events"} {
			before, e := counterFile(previous.Files[name])
			record(e)
			now, e := counterFile(s.Files[name])
			record(e)
			for k, v := range before {
				if now[k] < v {
					record(errors.New(name + " counter decreased"))
				}
			}
		}
	}
	s.End = time.Now().UTC()
	s.EndMonotonicNS = time.Since(observationEpoch).Nanoseconds()
	s.DurationNS = s.EndMonotonicNS - s.MonotonicNS
	if s.DurationNS > int64(2*time.Second) {
		record(errors.New("sample duration bound"))
	}
	o.Sequence++
	o.Previous = &s
	return s
}

// Only pipes are supported: os.File deadlines stop a blocked consumer without
// leaking a writer goroutine. Regular files must be collected by the caller.
type evidenceWriter struct {
	File             *os.File
	Context          context.Context
	Bytes            int
	SingleWriteLimit int
}

func (w *evidenceWriter) Write(raw []byte) (int, error) {
	if err := context.Cause(w.Context); err != nil {
		return 0, err
	}
	singleWriteLimit := w.SingleWriteLimit
	if singleWriteLimit == 0 {
		singleWriteLimit = 1 << 20
	}
	if singleWriteLimit < 1<<20 || singleWriteLimit > 32<<20 || len(raw) > singleWriteLimit || w.Bytes+len(raw) > 64<<20 {
		return 0, errors.New("evidence output bound")
	}
	if err := w.File.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		return 0, err
	}
	n, err := w.File.Write(raw)
	w.Bytes += n
	return n, err
}
func observe(control *observationControl, encoder *json.Encoder, o *Sampler, seconds int) error {
	ctx := control.Context
	if seconds < 2 || seconds > 2698 {
		return errors.New("observer duration bound")
	}
	endpoint := "http://127.0.0.1:7449"
	if o.Role == "es" {
		endpoint = "http://127.0.0.1:9200"
	}
	if o.Role == "client" {
		return errors.New("client uses self sampling in trial")
	}
	c, err := newClient(endpoint, "", 62)
	if err != nil {
		return err
	}
	defer c.Close()
	c.Transport.MaxConnsPerHost = 1
	c.Transport.MaxIdleConns = 1
	c.Transport.MaxIdleConnsPerHost = 1
	kernel, err := boundedFile("/proc/version")
	if err != nil {
		return err
	}
	identity := map[string]any{"type": "identity", "role": o.Role, "target": o.Target, "observer": o.Observer, "exe_sha256": o.Target.SHA256, "goos": runtime.GOOS, "goarch": runtime.GOARCH, "kernel": kernel, "go": runtime.Version(), "scope": "visible cgroup-v2 leaf; hidden ancestors unknown", "process_cpu_unit": "USER_HZ ticks, no percentage conversion"}
	if err = encoder.Encode(identity); err != nil {
		return err
	}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for n := 0; n <= seconds/2; n++ {
		s := o.sample(ctx, c)
		if err := context.Cause(ctx); err != nil {
			return err
		}
		if err = encoder.Encode(s); err != nil {
			return err
		}
		if len(s.Errors) > 0 {
			return errors.New("invalid resource sample")
		}
		if n == seconds/2 {
			return control.finish(encoder, o, seconds)
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
		}
	}
	return errors.New("observer sample bound")
}
