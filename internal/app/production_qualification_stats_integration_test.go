//go:build integration

package app

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc/stats"
)

func TestQualificationReceiveStatsAttribution(t *testing.T) {
	start := time.Unix(100, 0)
	observer := &qualificationReceiveStats{start: start}
	ctx := context.WithValue(t.Context(), qualificationStreamKey, 7)
	info := &stats.RPCTagInfo{}
	ctx = observer.TagRPC(ctx, info)
	head := &pb.Event_Head{}
	chunk := &pb.Event_Chunk{}
	end := &pb.Event_NativeEnd{}
	events := []*pb.Event{{Value: head}, {Value: chunk}, {Value: end}}
	for i, event := range events {
		response := &pb.ExecuteResponse{Event: event}
		payload := &stats.InPayload{Payload: response, RecvTime: start.Add(time.Duration(i+1) * time.Second), Length: 101 + i, WireLength: 106 + i}
		observer.HandleRPC(ctx, payload)
	}
	observer.callback(ctx, "callback_enter", start.Add(4*time.Second))
	observer.callback(ctx, "callback_return", start.Add(5*time.Second))
	response := &pb.ExecuteResponse{Event: events[1]}
	payload := &stats.InPayload{Payload: response, RecvTime: start.Add(8 * time.Second), Length: 200, WireLength: 205}
	observer.HandleRPC(ctx, payload)
	observer.callback(ctx, "callback_enter", start.Add(9*time.Second))
	stream := &observer.streams[7]
	if stream.counts != [4]int64{1, 2, 1, 0} || stream.attributed != 1 || stream.maxGap != 4*time.Second || stream.preReceive != 3*time.Second || stream.postReceive != time.Second {
		t.Fatalf("incorrect attribution: %s", observer.summary(7))
	}
	if observer.streams[0].next != 0 {
		t.Fatal("RPC context attributed to wrong worker")
	}
	if !strings.Contains(observer.summary(7), "Length:200 WireLength:205") {
		t.Fatal("missing bounded length record")
	}
}

func TestQualificationReceiveStatsBoundedConcurrent(t *testing.T) {
	observer := &qualificationReceiveStats{start: time.Unix(100, 0)}
	var pending sync.WaitGroup
	for worker := range 64 {
		pending.Add(1)
		go func() {
			defer pending.Done()
			ctx := context.WithValue(t.Context(), qualificationStreamKey, worker%32)
			payload := &stats.InPayload{RecvTime: observer.start}
			for range 100 {
				observer.HandleRPC(ctx, payload)
				observer.summary(worker % 32)
			}
		}()
	}
	pending.Wait()
	ignored := &stats.InPayload{}
	observer.HandleRPC(t.Context(), ignored)
	invalid := context.WithValue(t.Context(), qualificationStreamKey, 32)
	observer.HandleRPC(invalid, ignored)
	for i := range observer.streams {
		stream := &observer.streams[i]
		if stream.next != 200 || stream.counts[3] != 200 || len(stream.records) != 16 {
			t.Fatalf("unbounded or incorrect worker %d", i)
		}
	}
}
