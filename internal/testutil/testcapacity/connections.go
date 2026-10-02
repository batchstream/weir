package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ConnectionSample struct {
	Type        string         `json:"type"`
	Time        time.Time      `json:"time"`
	PID         string         `json:"pid"`
	StartTicks  uint64         `json:"start_ticks"`
	Port        uint16         `json:"port"`
	Established int            `json:"established"`
	States      map[string]int `json:"states"`
	SocketFDs   int            `json:"socket_fds"`
	LostFDs     int            `json:"lost_fds"`
}

type ConnectionObserveOptions struct {
	PID        string
	Port       int
	Seconds    int
	IntervalMS int
}

// Intersect the target's raw socket inodes with its network namespace table.
// Network tables alone also contain client sockets and other processes.
func connectionSample(opts ConnectionObserveOptions) (ConnectionSample, error) {
	sample := ConnectionSample{
		Type: "connection_sample", Time: time.Now().UTC(), PID: opts.PID,
		Port: uint16(opts.Port), States: make(map[string]int),
	}
	if _, err := unsigned(opts.PID); err != nil {
		return sample, errors.New("connection observer requires a numeric process PID")
	}
	root := filepath.Join("/proc", opts.PID)
	stat, err := boundedFile(filepath.Join(root, "stat"))
	if err != nil {
		return sample, err
	}
	sample.StartTicks, _, _, err = processStat(stat)
	if err != nil {
		return sample, err
	}
	names, err := boundedNames(filepath.Join(root, "fd"), 8192)
	if err != nil {
		return sample, err
	}
	owned := make(map[string]bool)
	for _, name := range names {
		target, err := os.Readlink(filepath.Join(root, "fd", name))
		if errors.Is(err, os.ErrNotExist) {
			sample.LostFDs++
			continue
		}
		if err != nil {
			return sample, fmt.Errorf("target socket ownership unavailable: %w", err)
		}
		if strings.HasPrefix(target, "socket:[") && strings.HasSuffix(target, "]") {
			owned[target[len("socket:["):len(target)-1]] = true
		}
	}
	sample.SocketFDs = len(owned)
	for _, table := range []string{"tcp", "tcp6"} {
		raw, err := boundedFile(filepath.Join(root, "net", table))
		if err != nil {
			return sample, err
		}
		states, err := ownedTCPStates(raw, owned, sample.Port)
		if err != nil {
			return sample, err
		}
		for state, count := range states {
			sample.States[state] += count
		}
	}
	sample.Established = sample.States["01"]
	// Refuse a recycled PID rather than attributing a new process's sockets.
	stat, err = boundedFile(filepath.Join(root, "stat"))
	if err != nil {
		return sample, err
	}
	endTicks, _, _, err := processStat(stat)
	if err != nil || endTicks != sample.StartTicks {
		return sample, errors.New("connection observer process identity changed")
	}
	return sample, nil
}

func ownedTCPStates(raw string, owned map[string]bool, port uint16) (map[string]int, error) {
	states := make(map[string]int)
	lines := strings.Split(strings.TrimSpace(raw), "\n")
	if len(lines) == 0 || !strings.Contains(lines[0], "local_address") {
		return nil, errors.New("invalid process TCP table header")
	}
	for _, line := range lines[1:] {
		fields := strings.Fields(line)
		if len(fields) < 10 {
			return nil, errors.New("short process TCP table row")
		}
		if !owned[fields[9]] {
			continue
		}
		_, localPort, found := strings.Cut(fields[1], ":")
		parsed, err := strconv.ParseUint(localPort, 16, 16)
		if !found || err != nil {
			return nil, errors.New("invalid TCP local endpoint")
		}
		if uint16(parsed) == port {
			if len(fields[3]) != 2 {
				return nil, errors.New("invalid TCP state")
			}
			states[fields[3]]++
		}
	}
	return states, nil
}

func connectionObserve(ctx context.Context, encoder *json.Encoder, opts ConnectionObserveOptions) error {
	if opts.Port < 1 || opts.Port > 65535 || opts.Seconds < 1 || opts.Seconds > 180 || opts.IntervalMS < 100 || opts.IntervalMS > 2000 {
		return errors.New("connection observation bounds")
	}
	deadline, cancel := context.WithTimeout(ctx, time.Duration(opts.Seconds)*time.Second)
	defer cancel()
	ticker := time.NewTicker(time.Duration(opts.IntervalMS) * time.Millisecond)
	defer ticker.Stop()
	var firstTicks uint64
	for {
		sample, err := connectionSample(opts)
		if err != nil {
			return err
		}
		if firstTicks != 0 && sample.StartTicks != firstTicks {
			return errors.New("observed database process restarted")
		}
		firstTicks = sample.StartTicks
		if err := encoder.Encode(sample); err != nil {
			return err
		}
		select {
		case <-deadline.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

type ConnectionProbeOptions struct {
	Backend     string    `json:"backend"`
	Target      string    `json:"target"`
	Pool        int       `json:"pool"`
	Workers     int       `json:"workers"`
	Queue       int       `json:"queue"`
	Rate        int       `json:"rate"`
	Seconds     int       `json:"seconds"`
	IdleSeconds int       `json:"idle_seconds"`
	StartAt     time.Time `json:"start_at"`
}

type ConnectionProbeReport struct {
	Type       string                 `json:"type"`
	PID        int                    `json:"pid"`
	Start      time.Time              `json:"start"`
	End        time.Time              `json:"end"`
	IdleEnd    time.Time              `json:"idle_end"`
	Closed     time.Time              `json:"closed"`
	Rate       int                    `json:"rate"`
	Planned    int                    `json:"planned"`
	Started    int                    `json:"started"`
	Success    int                    `json:"success"`
	ClientDrop int                    `json:"client_drop"`
	Failures   map[string]int         `json:"failures"`
	Options    ConnectionProbeOptions `json:"options"`
}

// Read-only probes share one pre-seeded corpus; they never setup, reset or audit.
func connectionProbe(ctx context.Context, encoder *json.Encoder, opts ConnectionProbeOptions) error {
	if opts.Pool < 1 || opts.Pool > 62 || opts.Workers < 1 || opts.Workers > 64 || opts.Queue < 0 || opts.Queue > 512 || opts.Rate < 1 || opts.Rate > 10000 || opts.Seconds < 1 || opts.Seconds > 120 || opts.IdleSeconds < 0 || opts.IdleSeconds > 45 {
		return errors.New("connection probe bounds")
	}
	if opts.Queue == 0 {
		opts.Queue = 256
	}
	if opts.StartAt.IsZero() {
		opts.StartAt = time.Now().Add(time.Second)
	}
	if time.Until(opts.StartAt) > 60*time.Second || time.Until(opts.StartAt) < -time.Second {
		return errors.New("connection probe start bound")
	}
	// Delay driver construction too: otherwise MongoDB heartbeat sockets would
	// already be present in the observer's pre-load baseline.
	if !waitConnectionTime(ctx, opts.StartAt) {
		return ctx.Err()
	}
	c, err := newClient(opts.Backend, opts.Target, opts.Pool)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			c.Close()
		}
	}()
	report := ConnectionProbeReport{
		Type: "connection_probe", PID: os.Getpid(), Start: opts.StartAt,
		Rate: opts.Rate, Planned: opts.Rate * opts.Seconds, Failures: make(map[string]int), Options: opts,
	}
	jobs := make(chan Operation, opts.Queue)
	var workers sync.WaitGroup
	var mu sync.Mutex
	for i := 0; i < opts.Workers; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for op := range jobs {
				call, cancel := context.WithDeadline(ctx, op.Planned.Add(time.Second))
				result := c.Call(call, op)
				cancel()
				mu.Lock()
				report.Started++
				if result.Class == "ok" {
					report.Success++
				} else {
					report.Failures[result.Class]++
				}
				mu.Unlock()
			}
		}()
	}
	for n := 0; n < report.Planned; n++ {
		planned := opts.StartAt.Add(time.Duration(n) * time.Second / time.Duration(opts.Rate))
		if !waitConnectionTime(ctx, planned) {
			break
		}
		op := Operation{Number: n, ID: fmt.Sprintf("read-%04d", n%corpusSize), Planned: planned}
		if time.Since(planned) > 100*time.Millisecond {
			mu.Lock()
			report.ClientDrop++
			mu.Unlock()
			continue
		}
		select {
		case jobs <- op:
		default:
			mu.Lock()
			report.ClientDrop++
			mu.Unlock()
		}
	}
	close(jobs)
	workers.Wait()
	report.End = time.Now().UTC()
	idleStart := opts.StartAt.Add(time.Duration(opts.Seconds) * time.Second)
	if report.End.After(idleStart) {
		idleStart = report.End
	}
	idleUntil := idleStart.Add(time.Duration(opts.IdleSeconds) * time.Second)
	if !waitConnectionTime(ctx, idleUntil) {
		return ctx.Err()
	}
	report.IdleEnd = time.Now().UTC()
	c.Close()
	closed = true
	report.Closed = time.Now().UTC()
	return encoder.Encode(report)
}

func waitConnectionTime(ctx context.Context, when time.Time) bool {
	timer := time.NewTimer(max(time.Until(when), 0))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
