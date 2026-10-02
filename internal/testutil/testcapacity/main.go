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

func run() (runErrFinal error) {
	mode := flag.String("mode", "", "config, trial, observe, idle, pace, connection-probe, connection-observe, profile-node")
	legacy := flag.Bool("legacy-expiry", false, "diagnostic-only original 5ms expiry")
	reservation := flag.Int("mutation-reservation", 0, "controller reservation including seeds")
	backend := flag.String("backend", "http://elasticsearch:9200", "owned backend")
	target := flag.String("target", "", "weir target or empty direct")
	prefix := flag.String("prefix", "trial", "unique trial prefix")
	rate := flag.Int("rate", 50, "planned ops/s")
	seconds := flag.Int("seconds", 60, "measurement seconds")
	warm := flag.Int("warm", 20, "warm seconds")
	config := flag.String("config", "", "owned config")
	routes := flag.String("routes", "", "owned routing config, empty for no routes")
	pid := flag.String("pid", "1", "observed namespace pid")
	role := flag.String("role", "", "explicit observe role: weir or es")
	recovery := flag.Int("recovery-rate", 0, "immediate recovery phase offered operations per second")
	recoverySeconds := flag.Int("recovery-seconds", 120, "bounded recovery duration")
	pool := flag.Int("pool", 62, "direct backend HTTP connection cap, matching Weir concurrency for comparisons")
	workers := flag.Int("workers", 64, "bounded client concurrency")
	writeEvery := flag.Int("write-every", 10, "one write every 10 operations, or 1 for write-only")
	suppressCongestion := flag.Bool("suppress-congestion", false, "integration-only causal control: suppress explicit database congestion feedback")
	arrivalExpiryMS := flag.Int("arrival-expiry-ms", 0, "comparison-only arrival expiry override, bounded to 100 ms")
	maxCatchup := flag.Int("max-catchup", 0, "comparison-only catchup override, bounded to 512")
	clientQueue := flag.Int("client-queue", 0, "bounded comparison client queue, maximum 512")
	loadDelayMS := flag.Int("load-delay-ms", 0, "integration fixture pause after setup for controller resource changes, at most 5000 ms")
	connectionPort := flag.Int("connection-port", 9200, "observed database process TCP listening port")
	connectionIntervalMS := flag.Int("connection-interval-ms", 200, "raw socket observation interval, 100-2000 ms")
	connectionIdleSeconds := flag.Int("connection-idle-seconds", 12, "read-only probe keeps clients open after load, at most 45 seconds")
	connectionStartAt := flag.String("connection-start-at", "", "shared RFC3339Nano load start for multiple probe processes")
	cpuProfile := flag.String("cpu-profile", "", "opt-in profile-node CPU profile output")
	flag.Parse()
	if *loadDelayMS < 0 || *loadDelayMS > 5000 {
		return errors.New("load delay bound")
	}
	if *recoverySeconds < 1 || *recoverySeconds > 120 {
		return errors.New("recovery duration bound")
	}
	if *writeEvery != 1 && *writeEvery != 10 {
		return errors.New("write ratio bound")
	}
	if os.Getenv("WEIR_CAPACITY_INTEGRATION") != "1" {
		return errors.New("explicit integration opt-in required")
	}
	if !regexp.MustCompile(`^[a-z0-9-]{1,32}$`).MatchString(*prefix) {
		return errors.New("prefix bound")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	stdout, err := evidencePipe(os.Stdout)
	if err != nil {
		return err
	}
	defer stdout.Close()
	output := &evidenceWriter{File: stdout, Context: ctx}
	if *arrivalExpiryMS != 0 || *maxCatchup != 0 || *clientQueue != 0 {
		output.SingleWriteLimit = 32 << 20
	}
	encoder := json.NewEncoder(output)
	if *mode == "profile-node" {
		opts := ProfileNodeOptions{Config: *config, Routes: *routes, Output: *cpuProfile}
		return profiledNode(ctx, opts)
	}
	if *mode == "connection-observe" {
		opts := ConnectionObserveOptions{
			PID: *pid, Port: *connectionPort, Seconds: *seconds, IntervalMS: *connectionIntervalMS,
		}
		return connectionObserve(ctx, encoder, opts)
	}
	if *mode == "connection-probe" {
		var start time.Time
		if *connectionStartAt != "" {
			var err error
			start, err = time.Parse(time.RFC3339Nano, *connectionStartAt)
			if err != nil {
				return errors.New("invalid connection probe shared start")
			}
		}
		opts := ConnectionProbeOptions{
			Backend: *backend, Target: *target, Pool: *pool, Workers: *workers,
			Queue: *clientQueue, Rate: *rate, Seconds: *seconds, IdleSeconds: *connectionIdleSeconds, StartAt: start,
		}
		return connectionProbe(ctx, encoder, opts)
	}
	if *mode == "serve-control" {
		return serveControl(ctx, *config, *routes, *suppressCongestion)
	}
	if *mode == "config" {
		cfg, err := app.Load(*config, *routes)
		if err != nil {
			return err
		}
		limits := store.DefaultLimits()
		limits.Concurrency = 4
		limits.BatchOperations = 16
		timing := map[string]any{
			"expiry_ms":            arrivalExpiry.Milliseconds(),
			"max_catchup_per_wake": catchupLimit,
			"deadline_ms":          callLifetime.Milliseconds(),
		}
		out := map[string]any{"config": cfg, "store_effective": limits, "timing": timing}
		return encoder.Encode(out)
	}
	if *mode == "snapshot" {
		sampler, err := newSampler("client", "self")
		if err != nil {
			return err
		}
		return encoder.Encode(sampler.sample(ctx, nil))
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
		sampler, err := newSampler(*role, *pid)
		if err != nil {
			return err
		}
		stdin, err := evidencePipe(os.Stdin)
		if err != nil {
			return err
		}
		control := newObservationControl(ctx, stdin)
		output.Context = control.Context
		err = observe(control, encoder, sampler, *seconds)
		return errors.Join(err, control.close())
	}
	if *mode == "setup" {
		c, err := newClient(*backend, "", *pool)
		if err != nil {
			return err
		}
		defer c.Close()
		call, cancel := context.WithTimeout(ctx, 45*time.Second)
		defer cancel()
		if *reservation != corpusSize {
			return errors.New("bootstrap reservation")
		}
		defer func() {
			receipt := map[string]any{"type": "mutation_receipt", "started": c.MutationsStarted.Load()}
			if e := encoder.Encode(receipt); e != nil {
				runErrFinal = e
			}
		}()
		return c.setup(call, encoder)
	}
	if *mode == "pace" {
		opts := TrialOptions{
			Rate:            *rate,
			Seconds:         *seconds,
			Workers:         64,
			Prefix:          "pace",
			TimingOnly:      true,
			WriteEvery:      *writeEvery,
			LegacyExpiry:    *legacy,
			ArrivalExpiryMS: *arrivalExpiryMS,
			MaxCatchup:      *maxCatchup,
			ClientQueue:     *clientQueue,
		}
		return pacing(ctx, encoder, opts)
	}
	if *mode != "trial" {
		return errors.New("unknown mode")
	}
	c, err := newClient(*backend, *target, *pool)
	if err != nil {
		return err
	}
	defer c.Close()
	planned := *rate*(*warm+*seconds) + *recovery*(*recoverySeconds)
	if *reservation != corpusSize+planned/(*writeEvery) || planned%(*writeEvery) != 0 || *reservation > 300000 {
		return errors.New("trial reservation")
	}
	defer func() {
		receipt := map[string]any{"type": "mutation_receipt", "started": c.MutationsStarted.Load()}
		if e := encoder.Encode(receipt); e != nil {
			runErrFinal = e
		}
	}()
	deadline, cancel := context.WithTimeout(ctx, 6*time.Minute)
	defer cancel()
	if err = c.setup(deadline, encoder); err != nil {
		return err
	}
	sampler, err := newSampler("client", "self")
	if err != nil {
		return err
	}
	sampler.SkipNetwork = *arrivalExpiryMS != 0 || *maxCatchup != 0 || *clientQueue != 0
	begin := sampler.sample(deadline, nil)
	start := map[string]any{"type": "client_start", "sample": begin}
	if err = encoder.Encode(start); err != nil {
		return err
	}
	if len(begin.Errors) != 0 {
		return errors.New("invalid initial client sample")
	}
	samples := make(chan Sample, 160)
	done := make(chan struct{})
	sampleCtx, stopSamples := context.WithCancel(deadline)
	defer stopSamples()
	go func() {
		defer close(done)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-sampleCtx.Done():
				return
			case <-ticker.C:
				s := sampler.sample(sampleCtx, nil)
				select {
				case samples <- s:
				default:
					cancel()
					return
				}
				if len(s.Errors) != 0 {
					cancel()
					return
				}
			}
		}
	}()
	if *loadDelayMS > 0 {
		select {
		case <-time.After(time.Duration(*loadDelayMS) * time.Millisecond):
		case <-deadline.Done():
			return deadline.Err()
		}
	}
	opts := TrialOptions{
		Rate:            *rate,
		WarmSeconds:     *warm,
		Seconds:         *seconds,
		Prefix:          *prefix,
		Workers:         *workers,
		WriteEvery:      *writeEvery,
		ArrivalExpiryMS: *arrivalExpiryMS,
		MaxCatchup:      *maxCatchup,
		ClientQueue:     *clientQueue,
	}
	t, runErr := runTrial(deadline, c, opts)
	var rt *Trial
	if runErr == nil && *recovery > 0 {
		opts.Rate = *recovery
		opts.Seconds = *recoverySeconds
		opts.WarmSeconds = 0
		opts.Prefix = *prefix + "-recovery"
		rt, runErr = runTrial(deadline, c, opts)
	}
	if t != nil && runErr == nil {
		last := t
		if rt != nil {
			last = rt
		}
		until := last.Start.Add(time.Duration(last.Options.WarmSeconds+last.Options.Seconds) * time.Second)
		if wait := time.Until(until); wait > 0 {
			time.Sleep(wait)
		}
	}
	stopSamples()
	<-done
	samples <- sampler.sample(ctx, nil)
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
		out = map[string]any{
			"type":   "audit",
			"prefix": trial.Options.Prefix,
			"audit":  a,
			"error":  fmt.Sprint(ae),
			"ledger": trial.Ledger,
		}
		if err = encoder.Encode(out); err != nil {
			return err
		}
		if ae != nil {
			return ae
		}
	}
	c.Close()
	sampler.VerifyHash = true
	out = map[string]any{"type": "client_end", "sample": sampler.sample(ctx, nil)}
	if err = encoder.Encode(out); err != nil {
		return err
	}
	return runErr
}
