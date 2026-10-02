//go:build integration

package main

import (
	"context"
	"errors"
	"os"
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
	defer file.Close()
	if err := pprof.StartCPUProfile(file); err != nil {
		return err
	}
	defer pprof.StopCPUProfile()
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
	select {
	case <-ctx.Done():
		return nil
	case err := <-node.Errors:
		return err
	}
}
