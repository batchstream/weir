//go:build linux || darwin

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

// This child uses the actual completion owner and inherited pollable pipes. Its
// sample input is synthetic; it makes no claim about /proc or DB HTTP sampling.
func TestObservationCompletionChild(t *testing.T) {
	mode := os.Getenv("WEIR_COMPLETION_CHILD")
	if mode == "" {
		return
	}
	err := completionChild(mode)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

func completionChild(mode string) (result error) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	input, err := evidencePipe(os.Stdin)
	if err != nil {
		return err
	}
	control := newObservationControl(ctx, input)
	defer func() { result = errors.Join(result, control.close()) }()
	output, err := evidencePipe(os.Stdout)
	if err != nil {
		return err
	}
	defer output.Close()
	writer := &evidenceWriter{File: output, Context: control.Context}
	encoder := json.NewEncoder(writer)
	last := Sample{Role: "weir", Sequence: 1}
	sampler := &Sampler{Role: "weir", Sequence: 2, Previous: &last}
	seconds := 2
	if name := os.Getenv("WEIR_COMPLETION_STREAM"); name != "" {
		// Supplied by the external Python pipe regression, never a live sample.
		raw, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		lines := bytes.Split(bytes.TrimSuffix(raw, []byte("\n")), []byte("\n"))
		if len(lines) < 3 {
			return errors.New("test stream count")
		}
		for _, line := range lines {
			if _, err = writer.Write(append(line, '\n')); err != nil {
				return err
			}
		}
		if err = json.Unmarshal(lines[len(lines)-1], &last); err != nil {
			return err
		}
		sampler.Role, sampler.Sequence = last.Role, last.Sequence+1
		seconds = 2 * (len(lines) - 2)
		if mode == "external-early" {
			<-control.Context.Done()
			return context.Cause(control.Context)
		}
	} else {
		identity := map[string]string{"type": "identity", "scope": "synthetic completion-only test"}
		if err = encoder.Encode(identity); err != nil {
			return err
		}
		switch mode {
		case "early", "signal":
			<-control.Context.Done()
			return context.Cause(control.Context)
		case "blocked", "write-failure":
			_, err = writer.Write(bytes.Repeat([]byte("x"), 1<<20))
			if err != nil {
				return err
			}
		case "sample-error":
			last.Errors = []string{"sample failed"}
		case "count-error":
			sampler.Sequence++
		}
	}
	return control.finish(encoder, sampler, seconds)
}

func TestObservationCompletionProcesses(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("Unix pipe fixture")
	}
	for _, mode := range []string{
		"complete",
		"no-ack",
		"illegal",
		"early",
		"signal",
		"signal-after-end",
		"sample-error",
		"count-error",
		"blocked",
		"write-failure",
		"slow",
	} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			child := exec.CommandContext(ctx, executable, "-test.run=^TestObservationCompletionChild$")
			childMode := mode
			if mode == "slow" {
				childMode = "blocked"
			}
			child.Env = append(os.Environ(), "WEIR_COMPLETION_CHILD="+childMode)
			input, err := child.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			output, err := child.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			var stderr bytes.Buffer
			child.Stderr = &stderr
			started := time.Now()
			if err = child.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() {
				cancel()
				if child.ProcessState == nil {
					_ = child.Wait()
				}
				input.Close()
				output.Close()
			}()
			reader := bufio.NewReader(output)
			identity, err := reader.ReadBytes('\n')
			if err != nil {
				t.Fatal(err)
			}
			var raw []byte
			ack := false
			switch mode {
			case "early":
				err = input.Close()
			case "signal":
				err = child.Process.Signal(syscall.SIGTERM)
			case "blocked":
				// Wait deliberately without draining. The real write deadline must win.
			case "write-failure":
				err = output.Close()
			case "slow":
				// Slow reader backpressure, not a sleep after terminal to hide tail loss.
				ticker := time.NewTicker(20 * time.Millisecond)
				defer ticker.Stop()
				block := make([]byte, 256)
				for n := 0; n < 60; n++ {
					<-ticker.C
					count, readErr := reader.Read(block)
					raw = append(raw, block[:count]...)
					if readErr != nil {
						break
					}
				}
			default:
				raw, err = reader.ReadBytes('\n')
				if mode == "signal-after-end" && err == nil {
					err = child.Process.Signal(syscall.SIGTERM)
				}
				if mode == "complete" || mode == "illegal" {
					if err != nil || !bytes.Contains(raw, []byte("observer_end")) {
						t.Fatal(string(raw), err)
					}
					if mode == "illegal" {
						_, err = input.Write([]byte("x"))
					} else {
						err = input.Close()
						ack = true
					}
				}
			}
			if err != nil && !errors.Is(err, io.EOF) {
				t.Fatal(err)
			}
			if mode != "blocked" && mode != "write-failure" {
				tail, readErr := io.ReadAll(reader)
				raw = append(raw, tail...)
				if readErr != nil {
					t.Fatal(readErr)
				}
			}
			err = child.Wait()
			if (err == nil) != (mode == "complete") || ctx.Err() != nil {
				t.Fatalf("Wait=%v deadline=%v stderr=%s", err, ctx.Err(), stderr.String())
			}
			if mode == "no-ack" && (time.Since(started) < 4*time.Second || !strings.Contains(stderr.String(), "EOF timeout")) {
				t.Fatal(time.Since(started), stderr.String())
			}
			if mode == "sample-error" || mode == "count-error" || mode == "early" || mode == "signal" {
				if bytes.Contains(raw, []byte("observer_end")) {
					t.Fatal("failed sampling emitted normal terminal")
				}
			}
			t.Logf("pid=%d Wait=%v elapsed=%s bytes=%d ACK=%v stderr=%q", child.Process.Pid, err, time.Since(started), len(identity)+len(raw), ack, stderr.String())
		})
	}
}

func TestObservationTerminalPipeWriteFailure(t *testing.T) {
	input, ack, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer ack.Close()
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	read.Close()
	defer write.Close()
	control := newObservationControl(context.Background(), input)
	writer := &evidenceWriter{File: write, Context: control.Context}
	encoder := json.NewEncoder(writer)
	last := Sample{Role: "weir", Sequence: 1}
	sampler := &Sampler{Role: "weir", Sequence: 2, Previous: &last}
	err = control.finish(encoder, sampler, 2)
	closeErr := control.close()
	if err == nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
}

type terminalPipeBarrier struct {
	writer  *evidenceWriter
	written chan struct{}
	release chan struct{}
}

func (w *terminalPipeBarrier) Write(raw []byte) (int, error) {
	n, err := w.writer.Write(raw)
	close(w.written)
	<-w.release // Receiver can ACK the actual pipe before Encode returns.
	return n, err
}

func TestObservationTerminalWriteEOFRace(t *testing.T) {
	for n := 0; n < 30; n++ {
		input, ack, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		read, write, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		control := newObservationControl(ctx, input)
		writer := &evidenceWriter{File: write, Context: control.Context}
		barrier := &terminalPipeBarrier{writer: writer, written: make(chan struct{}), release: make(chan struct{})}
		encoder := json.NewEncoder(barrier)
		last := Sample{Role: "weir", Sequence: 1}
		sampler := &Sampler{Role: "weir", Sequence: 2, Previous: &last}
		done := make(chan error, 1)
		go func() { done <- control.finish(encoder, sampler, 2) }()
		raw, readErr := bufio.NewReader(read).ReadBytes('\n')
		<-barrier.written
		ack.Close()
		// EOF is already in the actual pipe while Encode has not returned.
		<-control.joined
		close(barrier.release)
		finishErr := <-done
		closeErr := control.close()
		write.Close()
		read.Close()
		cancel()
		if readErr != nil || finishErr != nil || closeErr != nil || !bytes.Contains(raw, []byte("observer_end")) {
			t.Fatal(readErr, finishErr, closeErr, string(raw))
		}
	}
}

func TestObservationControlReadFailureAndEarlyEOF(t *testing.T) {
	for _, mode := range []string{"EOF", "byte", "read-error"} {
		t.Run(mode, func(t *testing.T) {
			input, ack, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer ack.Close()
			control := newObservationControl(context.Background(), input)
			if mode == "read-error" {
				input.Close()
			} else if mode == "byte" {
				_, err = ack.Write([]byte("x"))
			} else {
				ack.Close()
			}
			<-control.joined
			var out bytes.Buffer
			encoder := json.NewEncoder(&out)
			last := Sample{Role: "weir", Sequence: 1}
			sampler := &Sampler{Role: "weir", Sequence: 2, Previous: &last}
			finishErr := control.finish(encoder, sampler, 2)
			_ = control.close()
			if err != nil || finishErr == nil || out.Len() != 0 {
				t.Fatal(err, finishErr, out.String())
			}
		})
	}
}

func TestObservationEarlyEOFWithoutReaderScheduling(t *testing.T) {
	for n := 0; n < 100; n++ {
		input, ack, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		control := newObservationControl(context.Background(), input)
		ack.Close() // Do not wait for the control goroutine to observe this EOF.
		var raw bytes.Buffer
		encoder := json.NewEncoder(&raw)
		last := Sample{Role: "weir", Sequence: 1}
		sampler := &Sampler{Role: "weir", Sequence: 2, Previous: &last}
		err = control.finish(encoder, sampler, 2)
		closeErr := control.close()
		if err == nil || closeErr != nil || raw.Len() != 0 {
			t.Fatal(err, closeErr, raw.String())
		}
	}
}
