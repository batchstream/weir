//go:build integration

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/batchstream/weir/internal/app"
	"github.com/batchstream/weir/internal/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	mode := flag.String("mode", "", "config, trial, observe, idle")
	backend := flag.String("backend", "http://elasticsearch:9200", "owned backend")
	target := flag.String("target", "", "weir target or empty direct")
	prefix := flag.String("prefix", "trial", "unique trial prefix")
	rate := flag.Int("rate", 50, "planned ops/s")
	seconds := flag.Int("seconds", 60, "measurement seconds")
	warm := flag.Int("warm", 20, "warm seconds")
	config := flag.String("config", "", "owned config")
	pid := flag.String("pid", "1", "observed namespace pid")
	diagnostics := flag.Bool("diagnostics", false, "read namespace loopback metrics")
	recovery := flag.Int("recovery-rate", 0, "after this trial, immediate120s recovery rate")
	flag.Parse()
	if os.Getenv("WEIR_CAPACITY_INTEGRATION") != "1" {
		return errors.New("explicit integration opt-in required")
	}
	if !regexp.MustCompile(`^[a-z0-9-]{1,32}$`).MatchString(*prefix) {
		return errors.New("prefix bound")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	encoder := json.NewEncoder(os.Stdout)
	if *mode == "config" {
		f, err := os.Open(*config)
		if err != nil {
			return err
		}
		defer f.Close()
		cfg, err := app.Decode(f)
		if err != nil {
			return err
		}
		limits := store.DefaultLimits()
		limits.Concurrency = 4
		limits.BatchOperations = 16
		out := map[string]any{"config": cfg, "store_effective": limits}
		return encoder.Encode(out)
	}
	if *mode == "snapshot" {
		return encoder.Encode(sample("self"))
	}
	if *mode == "idle" {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(45 * time.Minute):
			return nil
		}
	}
	if *mode == "observe" {
		if *pid == "java" {
			entries, err := os.ReadDir("/proc")
			if err != nil {
				return err
			}
			found := ""
			for _, entry := range entries {
				name := entry.Name()
				if !regexp.MustCompile(`^[0-9]+$`).MatchString(name) {
					continue
				}
				comm, e := boundedFile("/proc/" + name + "/comm")
				cmdline, _ := boundedFile("/proc/" + name + "/cmdline")
				if e == nil && comm == "java\n" && strings.Contains(cmdline, "org.elasticsearch.bootstrap.Elasticsearch") {
					if found != "" {
						return errors.New("multiple JVM processes")
					}
					found = name
				}
			}
			if found == "" {
				return errors.New("JVM process not found")
			}
			*pid = found
		}

		hash, err := executableHash(*pid)
		if err != nil {
			return err
		}
		kernel, _ := boundedFile("/proc/version")
		identity := map[string]any{"exe_sha256": hash, "pid": *pid, "goos": runtime.GOOS, "goarch": runtime.GOARCH, "kernel": kernel}
		if err = encoder.Encode(identity); err != nil {
			return err
		}
		c, err := newClient("http://127.0.0.1:7449", "")
		if err != nil {
			return err
		}
		defer c.Close()
		db, err := newClient(*backend, "")
		if err != nil {
			return err
		}
		defer db.Close()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for n := 0; n < 1350; n++ {
			s := sample(*pid)
			if *diagnostics {
				call, cancel := context.WithTimeout(ctx, time.Second)
				observeHTTP(call, c, &s)
				code, raw, e := db.request(call, "GET", "/_nodes/stats/process,jvm,os,fs,thread_pool,http?filter_path=nodes.*.process,nodes.*.jvm.mem,nodes.*.os.cpu,nodes.*.fs.io_stats,nodes.*.thread_pool.write,nodes.*.thread_pool.get,nodes.*.http.current_open,nodes.*.http.total_opened", nil)
				cancel()
				if e != nil || code != 200 {
					s.Errors = append(s.Errors, fmt.Sprintf("DB statistics %d %v", code, e))
				} else {
					s.DB = string(raw)
				}
			}
			if err = encoder.Encode(s); err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
		}
		return errors.New("observer duration bound")
	}
	if *mode == "setup" {
		c, err := newClient(*backend, "")
		if err != nil {
			return err
		}
		defer c.Close()
		call, cancel := context.WithTimeout(ctx, 45*time.Second)
		defer cancel()
		return c.setup(call)
	}
	if *mode != "trial" {
		return errors.New("unknown mode")
	}
	c, err := newClient(*backend, *target)
	if err != nil {
		return err
	}
	defer c.Close()
	deadline, cancel := context.WithTimeout(ctx, 6*time.Minute)
	defer cancel()
	if err = c.setup(deadline); err != nil {
		return err
	}
	begin := sample("self")
	start := map[string]any{"type": "client_start", "sample": begin}
	if err = encoder.Encode(start); err != nil {
		return err
	}
	samples := make(chan Sample, 100)
	done := make(chan struct{})
	sampleCtx, stopSamples := context.WithCancel(deadline)
	go func() {
		defer close(done)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-sampleCtx.Done():
				return
			case <-ticker.C:
				select {
				case samples <- sample("self"):
				default:
					return
				}
			}
		}
	}()
	opts := TrialOptions{Rate: *rate, WarmSeconds: *warm, Seconds: *seconds, Prefix: *prefix, Workers: 64}
	t, runErr := runTrial(deadline, c, opts)
	var rt *Trial
	if runErr == nil && *recovery > 0 {
		opts.Rate = *recovery
		opts.Seconds = 120
		opts.WarmSeconds = 0
		opts.Prefix = *prefix + "-recovery"
		rt, runErr = runTrial(deadline, c, opts)
	}
	stopSamples()
	<-done
	close(samples)
	if t == nil {
		return runErr
	}
	out := map[string]any{"type": "trial", "trial": t, "run_error": fmt.Sprint(runErr)}
	if err = encoder.Encode(out); err != nil {
		return err
	}
	if rt != nil {
		out = map[string]any{"type": "trial", "trial": rt, "run_error": fmt.Sprint(runErr)}
		if err = encoder.Encode(out); err != nil {
			return err
		}
	}
	for s := range samples {
		out = map[string]any{"type": "client_sample", "sample": s}
		if err = encoder.Encode(out); err != nil {
			return err
		}
	}
	for _, trial := range []*Trial{t, rt} {
		if trial == nil {
			continue
		}
		a, ae := c.audit(deadline, trial)
		out = map[string]any{"type": "audit", "prefix": trial.Options.Prefix, "audit": a, "error": fmt.Sprint(ae), "ledger": trial.Ledger}
		if err = encoder.Encode(out); err != nil {
			return err
		}
		if ae != nil {
			return ae
		}
	}
	c.Close()
	out = map[string]any{"type": "client_end", "sample": sample("self")}
	if err = encoder.Encode(out); err != nil {
		return err
	}
	return runErr
}
