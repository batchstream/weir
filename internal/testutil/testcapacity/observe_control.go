package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

// observationControl owns the pollable stdin duplicate. The sampling reader
// relinquishes ownership before finish writes the terminal and reads its EOF.
type observationControl struct {
	Context context.Context
	cancel  context.CancelCauseFunc
	input   *os.File
	joined  chan struct{}
	readErr error
}

func newObservationControl(ctx context.Context, input *os.File) *observationControl {
	ctx, cancel := context.WithCancelCause(ctx)
	c := &observationControl{Context: ctx, cancel: cancel, input: input, joined: make(chan struct{})}
	go func() {
		defer close(c.joined)
		var raw [1]byte
		n, err := input.Read(raw[:])
		if n != 0 {
			err = errors.New("observer control byte received")
		} else if errors.Is(err, io.EOF) {
			err = errors.New("observer EOF before completion")
		} else if err == nil {
			err = errors.New("observer empty control read")
		}
		c.readErr = err
		// Only finish sets a read deadline: timeout transfers ownership to it.
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			c.cancel(err)
		}
	}()
	return c
}

func (c *observationControl) close() error {
	c.cancel(context.Canceled)
	err := c.input.Close() // Interrupt the pollable Read, then join before returning.
	<-c.joined
	return err
}

func (c *observationControl) finish(encoder *json.Encoder, sampler *Sampler, seconds int) error {
	last := sampler.Previous
	if seconds < 2 || seconds > 2698 || last == nil || len(last.Errors) != 0 ||
		last.Role != sampler.Role || sampler.Sequence != uint64(seconds/2+1) || last.Sequence+1 != sampler.Sequence {
		return errors.New("observer incomplete samples")
	}
	if err := c.input.SetReadDeadline(time.Now()); err != nil {
		return err
	}
	<-c.joined
	if err := context.Cause(c.Context); err != nil {
		return err
	}
	if !errors.Is(c.readErr, os.ErrDeadlineExceeded) {
		return errors.New("observer control handoff failed")
	}
	if err := c.input.SetReadDeadline(time.Time{}); err != nil {
		return err
	}
	// An expired poller deadline may have won over an already pending EOF.
	// Check the nonblocking pipe after Join, with no competing reader.
	if err := checkObservationInput(c.input); err != nil {
		return err
	}
	// This deadline includes the terminal write, inside the existing close window.
	deadline := time.Now().Add(4 * time.Second)
	receipt := map[string]any{
		"type":     "observer_end",
		"samples":  sampler.Sequence,
		"role":     sampler.Role,
		"ended_at": time.Now().UTC(),
	}
	if err := encoder.Encode(receipt); err != nil {
		return err
	}
	if err := c.input.SetReadDeadline(deadline); err != nil {
		return err
	}
	interrupted := make(chan struct{})
	stopRead := context.AfterFunc(c.Context, func() {
		defer close(interrupted)
		_ = c.input.SetReadDeadline(time.Now())
	})
	defer func() {
		if !stopRead() {
			<-interrupted
		}
	}()
	var raw [1]byte
	n, err := c.input.Read(raw[:])
	if cause := context.Cause(c.Context); cause != nil {
		return cause
	}
	if n != 0 {
		return errors.New("observer control byte received")
	}
	if !time.Now().Before(deadline) || errors.Is(err, os.ErrDeadlineExceeded) {
		return errors.New("observer completion EOF timeout")
	}
	if !errors.Is(err, io.EOF) {
		return errors.New("observer completion control read failed: " + fmt.Sprint(err))
	}
	return nil
}
