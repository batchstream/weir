package store

import (
	"context"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

func TestCanceledSingletonDoesNotRunOrPoisonPeer(t *testing.T) {
	adapter := &singleExecutionAdapter{}
	limits := DefaultLimits()
	runtime, err := New(adapter, limits, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	session := runtime.NewSession()
	defer session.Close()
	canceled, stop := context.WithCancel(context.Background())
	stop()
	first := plan(1, "first", false)
	first.Command = nativeCall()
	if _, failure, _ := runtime.Submit(canceled, first, session); failure == nil {
		t.Fatal("admitted canceled operation")
	}
	work := plan(2, "second", false)
	ticket, failure, _ := runtime.Submit(context.Background(), work, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = ticket.Wait(ctx)
	if err != nil {
		t.Fatal("peer was canceled", err)
	}
	ticket.Ack()
}

func nativeCall() *pb.Command {
	body := &pb.Document{ContentType: "application/bson", Data: []byte{5, 0, 0, 0, 0}}
	request := &pb.NativeRequest{Resource: "records", Request: body}
	variant := &pb.Command_Native{Native: request}
	command := &pb.Command{Operation: variant}
	return command
}

func TestBlockedNativeLeavesOtherExecutionCreditAvailable(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxInFlight = 2
	adapter := &singleExecutionAdapter{}
	runtime, err := New(adapter, limits, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	session := runtime.NewSession()
	defer session.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	nativePlan := plan(1, "native", false)
	nativePlan.Command = nativeCall()
	_, failure, _ := runtime.Submit(ctx, nativePlan, session)
	if failure != nil {
		t.Fatal(failure)
	}
	var blocked *Emission
	select {
	case blocked = <-session.Events:
	case <-ctx.Done():
		t.Fatal("Native output did not reach the consumer")
	}
	defer blocked.Release()
	ordinary := plan(2, "record", false)
	ticket, failure, _ := runtime.Submit(ctx, ordinary, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	result, err := ticket.Wait(ctx)
	if err != nil || result.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("one blocked Native call monopolized both execution credits", result, err)
	}
	ticket.Ack()
	if snapshot := runtime.Snapshot(); snapshot.Active != 0 || snapshot.Paused != 1 {
		t.Fatal("Native output did not return its backend execution credit", snapshot)
	}
}

func waitReleased(t *testing.T, runtime *Runtime) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		snapshot := runtime.Snapshot()
		if snapshot.Retained == 0 && snapshot.Publishers == 0 && snapshot.Active == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("reservations did not release", runtime.Snapshot())
}
