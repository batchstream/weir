//go:build integration

package main

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"time"
)

// A timing-only diagnostic uses the same timer, construction, handoff and worker
// deadline path. It deliberately makes no RPC or database capacity claim.
func pacing(ctx context.Context, encoder *json.Encoder, opts TrialOptions) error {
	if opts.Seconds > 20 || (opts.Rate != 50 && opts.Rate != 200 && opts.Rate != 800) {
		return errors.New("frozen pacing probe bounds")
	}
	sampler, err := newSampler("client", "self")
	if err != nil {
		return err
	}
	samples := make([]Sample, 0, 12)
	samples = append(samples, sampler.sample(ctx, nil))
	done := make(chan struct{})
	sampleCtx, stop := context.WithCancel(ctx)
	go func() {
		defer close(done)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-sampleCtx.Done():
				return
			case <-ticker.C:
				if len(samples) >= 11 {
					return
				}
				samples = append(samples, sampler.sample(ctx, nil))
			}
		}
	}()
	t, err := runTrial(ctx, nil, opts)
	if t != nil && err == nil {
		until := t.Start.Add(time.Duration(opts.Seconds) * time.Second)
		if wait := time.Until(until); wait > 0 {
			time.Sleep(wait)
		}
	}
	stop()
	<-done
	samples = append(samples, sampler.sample(ctx, nil))
	hash, hashErr := executableHash("self")
	if hashErr != nil {
		return hashErr
	}
	kernel, kernelErr := boundedFile("/proc/version")
	if kernelErr != nil {
		return kernelErr
	}
	out := map[string]any{"kind": "native pacing diagnostic only", "trial": t, "samples": samples, "goos": runtime.GOOS, "goarch": runtime.GOARCH, "exe_sha256": hash, "kernel": kernel}
	if e := encoder.Encode(out); e != nil {
		return e
	}
	return err
}
