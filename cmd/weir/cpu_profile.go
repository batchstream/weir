package main

import (
	"errors"
	"fmt"
	"os"
	"runtime/pprof"
	"sync"
	"time"
)

type cpuProfileOptions struct {
	path     string
	duration time.Duration
}

func (o cpuProfileOptions) validate() error {
	if o.path == "" && o.duration == 0 {
		return nil
	}
	if o.path == "" || o.duration <= 0 || o.duration > 60*time.Second {
		return errors.New("--cpu-profile and --cpu-profile-duration require a new file and a duration greater than zero and at most 60s")
	}
	return nil
}

type cpuProfile struct {
	stop chan struct{}
	done chan struct{}
	once sync.Once
	err  error
}

func startCPUProfile(options cpuProfileOptions) (*cpuProfile, error) {
	if err := options.validate(); err != nil {
		return nil, err
	}
	if options.path == "" {
		return nil, nil
	}
	file, err := os.OpenFile(options.path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, fmt.Errorf("create CPU profile: %w", err)
	}
	if err := pprof.StartCPUProfile(file); err != nil {
		return nil, errors.Join(fmt.Errorf("start CPU profile: %w", err), file.Close())
	}
	profile := &cpuProfile{stop: make(chan struct{}), done: make(chan struct{})}
	timer := time.NewTimer(options.duration)
	go func() {
		defer close(profile.done)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-profile.stop:
		}
		// Stop flushes and joins the runtime profile writer before closing its file.
		pprof.StopCPUProfile()
		profile.err = file.Close()
	}()
	return profile, nil
}

func (p *cpuProfile) close() error {
	if p == nil {
		return nil
	}
	p.once.Do(func() { close(p.stop) })
	<-p.done
	return p.err
}
