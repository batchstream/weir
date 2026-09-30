package luaworker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

type Config struct {
	Executable string
	Timeout    time.Duration
}

type Runner struct {
	executable string
	timeout    time.Duration
	workers    chan struct{}
}

func New(config Config) (*Runner, error) {
	if config.Executable == "" {
		return nil, fmt.Errorf("Lua worker executable is required")
	}
	executable, err := filepath.Abs(config.Executable)
	if err != nil {
		return nil, fmt.Errorf("resolve Lua worker executable: %w", err)
	}
	info, err := os.Stat(executable)
	if err != nil || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("Lua worker executable is unavailable")
	}
	timeout := config.Timeout
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	if timeout < time.Millisecond || timeout > MaxTimeout {
		return nil, fmt.Errorf("Lua worker timeout must be between 1ms and 1s")
	}
	runner := &Runner{executable: executable, timeout: timeout, workers: make(chan struct{}, maxConcurrent)}
	return runner, nil
}

func (r *Runner) Run(ctx context.Context, program Program) (Result, error) {
	var empty Result
	if r == nil || ctx == nil {
		return empty, fmt.Errorf("Lua worker is not configured")
	}
	if err := ValidateProgram(program); err != nil {
		return empty, err
	}
	deadline := int64(r.timeout / time.Millisecond)
	request := Request{Program: program, TimeoutMillis: deadline}
	input, err := json.Marshal(request)
	if err != nil || len(input) > MaxRequestBytes {
		return empty, fmt.Errorf("Lua worker request exceeds limit")
	}
	runCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	select {
	case r.workers <- struct{}{}:
		defer func() { <-r.workers }()
	case <-runCtx.Done():
		return empty, runCtx.Err()
	}
	command := exec.CommandContext(runCtx, r.executable, "--weir-lua-worker")
	command.WaitDelay = 100 * time.Millisecond
	stdout, err := command.StdoutPipe()
	if err != nil {
		return empty, fmt.Errorf("open Lua worker output")
	}
	stdin, err := command.StdinPipe()
	if err != nil {
		return empty, fmt.Errorf("open Lua worker input")
	}
	command.Stderr = io.Discard
	if err := command.Start(); err != nil {
		return empty, fmt.Errorf("start Lua worker")
	}
	read := make(chan stdoutResult, 1)
	go func() {
		output, readErr := io.ReadAll(io.LimitReader(stdout, MaxResponseBytes+1))
		tooLarge := len(output) > MaxResponseBytes
		if tooLarge && command.Process != nil {
			_ = command.Process.Kill()
		}
		result := stdoutResult{data: output, err: readErr, tooLarge: tooLarge}
		read <- result
	}()
	_, writeErr := io.Copy(stdin, bytes.NewReader(input))
	closeErr := stdin.Close()
	if writeErr != nil || closeErr != nil {
		_ = command.Process.Kill()
	}
	output := <-read
	waitErr := command.Wait()
	if err := runCtx.Err(); err != nil {
		return empty, err
	}
	if writeErr != nil || closeErr != nil || output.err != nil || output.tooLarge || waitErr != nil {
		return empty, fmt.Errorf("Lua worker failed")
	}
	var answer Response
	if err := DecodeEnvelope(output.data, &answer); err != nil {
		return empty, fmt.Errorf("Lua worker returned an invalid response")
	}
	if answer.Error != "" {
		return empty, fmt.Errorf("%s", answer.Error)
	}
	if err := ValidateResult(answer.Result); err != nil {
		return empty, fmt.Errorf("Lua worker returned an invalid result")
	}
	return answer.Result, nil
}

type stdoutResult struct {
	data     []byte
	err      error
	tooLarge bool
}
