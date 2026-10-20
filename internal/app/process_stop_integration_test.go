//go:build integration

package app

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// This subprocess is only a terminal-measurement probe. It starts no Weir,
// database or network fixture and never loads a configuration or secret file.
func TestProcessStopChild(t *testing.T) {
	mode := os.Getenv("WEIR_PROCESS_STOP_CHILD")
	if mode == "" {
		t.Skip("self-subprocess only")
	}
	if mode == "timeout" {
		signal.Ignore(syscall.SIGTERM)
	} else {
		termination := make(chan os.Signal, 1)
		signal.Notify(termination, syscall.SIGTERM)
		fmt.Fprintln(os.Stdout, "ready")
		<-termination
		fmt.Fprintln(os.Stderr, "terminal-stderr-"+mode)
		if mode == "nonzero" {
			os.Exit(7)
		}
		os.Exit(0)
	}
	fmt.Fprintln(os.Stdout, "ready")
	for {
		time.Sleep(time.Hour)
	}
}

func stopProbe(t *testing.T, mode string) *process {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	t.Cleanup(cancel)
	command := exec.CommandContext(ctx, binary, "-test.run=^TestProcessStopChild$")
	command.Env = []string{"WEIR_PROCESS_STOP_CHILD=" + mode}
	output, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr := new(bytes.Buffer)
	command.Stderr = stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	p := &process{command: command, stderr: stderr, done: make(chan error, 1)}
	reaped := make(chan struct{})
	go func() {
		p.done <- command.Wait()
		close(reaped)
	}()
	t.Cleanup(func() {
		select {
		case <-reaped:
		default:
			_ = command.Process.Kill()
			<-reaped
		}
	})
	ready, err := bufio.NewReader(output).ReadString('\n')
	if err != nil || ready != "ready\n" {
		t.Fatal("terminal probe did not become ready", err)
	}
	return p
}

func TestProcessStopTerminalMeasurements(t *testing.T) {
	for _, mode := range []string{"clean", "nonzero", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			p := stopProbe(t, mode)
			result := p.waitForStop()
			t.Logf("mode=%s elapsed=%s exit_valid=%t exit_code=%d timed_out=%t wait_error=%v", mode, result.Elapsed, result.ExitCodeValid, result.ExitCode, result.TimedOut, result.WaitErr)
			if result.Elapsed <= 0 {
				t.Fatal("terminal duration missing")
			}
			switch mode {
			case "clean":
				if !result.successful() || result.WaitErr != nil || result.ExitCode != 0 || !result.ExitCodeValid || result.TimedOut {
					t.Fatal("clean terminal state lost", result)
				}
			case "nonzero":
				var exit *exec.ExitError
				if result.successful() || !errors.As(result.WaitErr, &exit) || !result.ExitCodeValid || result.ExitCode != 7 || result.TimedOut || result.Elapsed > 8*time.Second {
					t.Fatal("nonzero terminal state was concealed", result)
				}
			case "timeout":
				if result.successful() || !result.TimedOut || result.WaitErr == nil || result.ExitCodeValid || result.ExitCode != -1 || result.Elapsed < 8*time.Second || result.Elapsed > 10*time.Second {
					t.Fatal("actual timeout or killed state was concealed", result)
				}
			}
			if mode != "timeout" && !strings.Contains(p.stderr.String(), "terminal-stderr-"+mode) {
				t.Fatal("Wait returned before final stderr was available")
			}
		})
	}
}

func TestProcessStopOnceConcurrent(t *testing.T) {
	p := stopProbe(t, "clean")
	results := make(chan processStopResult, 8)
	var callers sync.WaitGroup
	for range cap(results) {
		callers.Go(func() { results <- p.stop(t) })
	}
	callers.Wait()
	close(results)
	first := p.stop(t)
	for result := range results {
		if !result.successful() || result != first {
			t.Fatal("concurrent stop did not preserve one observed terminal result", result, first)
		}
	}
}
