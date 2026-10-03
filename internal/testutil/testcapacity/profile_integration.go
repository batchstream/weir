//go:build integration

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/batchstream/weir/internal/app"
)

type ProfileNodeOptions struct {
	Config, Routes, Output string
}

// Profiling is opt-in and uses the same assembly and lifecycle as cmd/weir.
// The output belongs to the disposable fixture; no debug listener is exposed.
func profiledNode(ctx context.Context, opts ProfileNodeOptions) (result error) {
	if opts.Output == "" {
		return errors.New("profile output required")
	}
	cfg, err := app.Load(opts.Config, opts.Routes)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(opts.Output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, file.Close()) }()
	if err := pprof.StartCPUProfile(file); err != nil {
		return err
	}
	previous := runtime.SetMutexProfileFraction(10)
	defer runtime.SetMutexProfileFraction(previous)
	defer func() {
		pprof.StopCPUProfile()
		mutex, err := os.OpenFile(opts.Output+".mutex.pprof", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			result = errors.Join(result, err)
			return
		}
		result = errors.Join(result, pprof.Lookup("mutex").WriteTo(mutex, 0), mutex.Close())
	}()
	startup, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	node, err := app.Open(startup, cfg)
	if err != nil {
		return err
	}
	defer func() {
		drain, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result = errors.Join(result, node.Close(drain))
	}()
	if err := node.Start(startup); err != nil {
		return err
	}
	stop()
	_, err = fmt.Fprintf(os.Stdout, "Weir listening on %v; local execution and peer directory discovery\nDiagnostics listening on %s\n", node.Addresses(), node.DiagnosticAddress())
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return nil
	case err := <-node.Errors:
		return err
	}
}
