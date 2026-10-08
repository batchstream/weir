package store

import (
	"context"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

func TestCanceledSingletonDoesNotRunOrPoisonPeer(t *testing.T) {
	adapter := &scanTestAdapter{pages: 1}
	limits := DefaultLimits()
	runtime, err := New(adapter, limits)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	session := runtime.NewSession()
	defer session.Close()
	canceled, stop := context.WithCancel(context.Background())
	stop()
	first := plan(1, "first", false)
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
