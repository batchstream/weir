package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"google.golang.org/grpc/stats"
)

func TestHandlerTransportReadAheadIsBounded(t *testing.T) {
	source := bytes.NewBuffer(make([]byte, inputCredits+10))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	body := newCreditedBody(ctx, io.NopCloser(source), nil)
	defer body.Close()
	initial := make([]byte, inputCredits)
	if _, err := io.ReadFull(body, initial); err != nil {
		t.Fatal(err)
	}
	if source.Len() != 10 {
		t.Fatal("read beyond transport credit")
	}
	done := make(chan error, 1)
	go func() { p := make([]byte, 1); _, err := body.Read(p); done <- err }()
	select {
	case err := <-done:
		t.Fatal("read-ahead was not blocked", err)
	case <-time.After(20 * time.Millisecond):
	}
	state := &delivery{input: body}
	ctx = context.WithValue(ctx, deliveryKey, state)
	handler := deliveryStats{}
	event := &stats.InPayload{WireLength: 1}
	handler.HandleRPC(ctx, event)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("decoded message did not return credit")
	}
	if source.Len() != 9 {
		t.Fatal("returned more than consumed credit", source.Len())
	}
	go func() { p := make([]byte, 1); _, err := body.Read(p); done <- err }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel left a read-ahead goroutine")
	}
}

func TestHandlerTransportCloseInterruptsRead(t *testing.T) {
	reader, writer := io.Pipe()
	defer writer.Close()
	body := newCreditedBody(context.Background(), reader, nil)
	done := make(chan error, 1)
	go func() { p := make([]byte, 1); _, err := body.Read(p); done <- err }()
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, io.ErrClosedPipe) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("body close did not stop reader")
	}
}

func TestHandlerTransportShortReadRefundsCredit(t *testing.T) {
	source := bytes.NewBufferString("abc")
	body := newCreditedBody(context.Background(), io.NopCloser(source), nil)
	defer body.Close()
	buffer := make([]byte, 100)
	n, err := body.Read(buffer)
	if err != nil || n != 3 {
		t.Fatal(n, err)
	}
	if body.available != inputCredits-3 {
		t.Fatal("short read lost credit", body.available)
	}
	body.grant(n)
	if body.available != inputCredits {
		t.Fatal("incorrect refund", body.available)
	}
}

func TestCompletedDeliveryNeverTouchesResponseWriter(t *testing.T) {
	// HTTP may finish before gRPC's handler goroutine observes cancellation.
	// A late completion must not touch a pooled/released HTTP response writer.
	state := &delivery{}
	state.finish()
	state.beginResponse()
	state.shorten(time.Now())
	state.finishWrite(nil)
	if err := state.armWrite(); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal(err)
	}
	state.finish()
}

func TestDeliverySlotRequiresBothCompletions(t *testing.T) {
	for _, httpFirst := range []bool{true, false} {
		slots := make(chan struct{}, 1)
		slots <- struct{}{}
		state := &delivery{slots: slots}
		ctx := context.WithValue(context.Background(), deliveryKey, state)
		statistics := deliveryStats{}
		info := &stats.ConnTagInfo{}
		statistics.TagConn(ctx, info)
		end := &stats.End{}
		if httpFirst {
			state.finish()
		} else {
			statistics.HandleRPC(ctx, end)
		}
		if len(slots) != 1 {
			t.Fatal("slot released while one owner is still live")
		}
		if httpFirst {
			statistics.HandleRPC(ctx, end)
		} else {
			state.finish()
		}
		if len(slots) != 0 {
			t.Fatal("slot not released after both completions")
		}
		state.finish()
		statistics.HandleRPC(ctx, end)
	}
	// Rejection before the handler transport starts has no future stats.End.
	slots := make(chan struct{}, 1)
	slots <- struct{}{}
	rejected := &delivery{slots: slots}
	rejected.finish()
	if len(slots) != 0 {
		t.Fatal("pre-dispatch rejection leaked slot")
	}
}
