package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type resourceSample struct {
	CPUQuota              string                       `json:"cpu_max"`
	ProcessIO             map[string]int64             `json:"process_io"`
	CgroupIO              map[string]map[string]uint64 `json:"cgroup_io"`
	IOPressure            string                       `json:"io_pressure"`
	BlockIODelayTicks     int64                        `json:"block_io_delay_ticks"`
	BlockIODelayAvailable bool                         `json:"block_io_delay_available"`
	Monotonic             float64                      `json:"monotonic"`
	WallTime              float64                      `json:"wall_time"`
	CPU                   map[string]int64             `json:"cpu"`
	MemoryCurrent         int64                        `json:"memory_current"`
	MemoryPeak            int64                        `json:"memory_peak"`
	MemoryEvents          map[string]int64             `json:"memory_events"`
	ProcessCPUTicks       int64                        `json:"process_cpu_ticks"`
	ProcessStartTicks     int64                        `json:"process_start_ticks"`
	RSSPages              int64                        `json:"rss_pages"`
	Metrics               string                       `json:"metrics,omitempty"`
}

func readFile(name string) (string, error) {
	file, err := os.Open(name)
	if err != nil {
		return "", err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, (64<<10)+1))
	if len(raw) > 64<<10 {
		return "", fmt.Errorf("observation file exceeds64KiB: %s", name)
	}
	return strings.TrimSpace(string(raw)), err
}

func scalar(name string) (int64, error) {
	raw, err := readFile(name)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(raw, 10, 64)
}

func counters(name string) (map[string]int64, error) {
	raw, err := readFile(name)
	if err != nil {
		return nil, err
	}
	result := make(map[string]int64)
	for _, line := range strings.Split(raw, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return nil, fmt.Errorf("invalid counters: %s", name)
		}
		n, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return nil, err
		}
		result[strings.TrimSuffix(fields[0], ":")] = n
	}
	return result, nil
}

func collect(pid int, started time.Time) (resourceSample, error) {
	sample := resourceSample{Monotonic: time.Since(started).Seconds(), WallTime: float64(time.Now().UnixNano()) / 1e9}
	var err error
	sample.CPUQuota, err = readFile("/sys/fs/cgroup/cpu.max")
	if err != nil {
		return sample, err
	}
	sample.ProcessIO, err = counters(fmt.Sprintf("/proc/%d/io", pid))
	if err != nil {
		return sample, err
	}
	rawIO, err := readFile("/sys/fs/cgroup/io.stat")
	if err != nil {
		return sample, err
	}
	sample.CgroupIO, err = ioCounters(rawIO)
	if err != nil {
		return sample, err
	}
	sample.IOPressure, err = readFile("/sys/fs/cgroup/io.pressure")
	if err != nil {
		return sample, err
	}
	sample.CPU, err = counters("/sys/fs/cgroup/cpu.stat")
	if err != nil {
		return sample, err
	}
	sample.MemoryCurrent, err = scalar("/sys/fs/cgroup/memory.current")
	if err != nil {
		return sample, err
	}
	sample.MemoryPeak, err = scalar("/sys/fs/cgroup/memory.peak")
	if err != nil {
		return sample, err
	}
	sample.MemoryEvents, err = counters("/sys/fs/cgroup/memory.events")
	if err != nil {
		return sample, err
	}
	raw, err := readFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return sample, err
	}
	fields := strings.Fields(raw[strings.LastIndex(raw, ")")+2:])
	if len(fields) < 20 {
		return sample, fmt.Errorf("invalid proc stat")
	}
	user, err := strconv.ParseInt(fields[11], 10, 64)
	if err != nil {
		return sample, err
	}
	system, err := strconv.ParseInt(fields[12], 10, 64)
	if err != nil {
		return sample, err
	}
	sample.ProcessCPUTicks = user + system
	if len(fields) > 39 {
		sample.BlockIODelayTicks, err = strconv.ParseInt(fields[39], 10, 64)
		if err != nil {
			return sample, err
		}
		sample.BlockIODelayAvailable = true
	}
	sample.ProcessStartTicks, err = strconv.ParseInt(fields[19], 10, 64)
	if err != nil {
		return sample, err
	}
	raw, err = readFile(fmt.Sprintf("/proc/%d/statm", pid))
	if err != nil {
		return sample, err
	}
	fields = strings.Fields(raw)
	if len(fields) < 2 {
		return sample, fmt.Errorf("invalid proc statm")
	}
	sample.RSSPages, err = strconv.ParseInt(fields[1], 10, 64)
	return sample, err
}

func ioCounters(raw string) (map[string]map[string]uint64, error) {
	result := make(map[string]map[string]uint64)
	for _, line := range strings.Split(raw, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if len(fields) > 17 || len(result) >= 64 {
			return nil, fmt.Errorf("IO counter bound")
		}
		if _, exists := result[fields[0]]; exists {
			return nil, fmt.Errorf("duplicate IO device")
		}
		device := make(map[string]uint64)
		for _, field := range fields[1:] {
			key, value, ok := strings.Cut(field, "=")
			if !ok || key == "" {
				return nil, fmt.Errorf("invalid IO counter")
			}
			if _, exists := device[key]; exists {
				return nil, fmt.Errorf("duplicate IO counter")
			}
			n, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return nil, err
			}
			device[key] = n
		}
		result[fields[0]] = device
	}
	return result, nil
}

func main() {
	pid := flag.Int("pid", 0, "observed process PID")
	output := flag.String("output", "", "task-owned sample file")
	metrics := flag.String("metrics", "", "owned Weir metrics URL")
	flag.Parse()
	if *pid <= 0 || *output == "" {
		panic("pid and output required")
	}
	file, err := os.OpenFile(*output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	started := time.Now()
	client := &http.Client{Timeout: 2 * time.Second}
	var targetStart int64
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	deadline := time.NewTimer(55 * time.Minute)
	defer deadline.Stop()
	for {
		sample, err := collect(*pid, started)
		if err != nil {
			panic(err)
		}
		if targetStart == 0 {
			targetStart = sample.ProcessStartTicks
		}
		if targetStart != sample.ProcessStartTicks {
			panic("observed process identity changed")
		}
		if *metrics != "" {
			response, e := client.Get(*metrics)
			if e != nil {
				panic(e)
			}
			raw, e := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
			response.Body.Close()
			if e != nil {
				panic(e)
			}
			if len(raw) > 1<<20 {
				panic("metrics exceed1MiB")
			}
			if response.StatusCode != http.StatusOK {
				panic("metrics status is not200")
			}
			sample.Metrics = string(raw)
		}
		if err := encoder.Encode(sample); err != nil {
			panic(err)
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			return
		}
	}
}
