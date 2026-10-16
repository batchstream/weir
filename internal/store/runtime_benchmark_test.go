package store

import (
	"context"
	"fmt"
	"math/bits"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

type fragmentedCase struct {
	name                      string
	callers, namespaces, keys int
	documentBytes, writeEvery int
	fixed, perItem, wait      time.Duration
	batchOperations           int
}

// The adapter models a serialized database's fixed command cost plus item cost.
// It measures scheduling and invocation reduction, not database CPU or wire IO.
type fragmentedAdapter struct {
	mu                    sync.Mutex
	fixed, perItem        time.Duration
	calls                 atomic.Int64
	items                 atomic.Int64
	readEvent, writeEvent *pb.Event
}

func (*fragmentedAdapter) PrepareCommand(context.Context, uint64, *pb.Command) (*execution.Plan, *pb.Failure) {
	return nil, nil
}

func (*fragmentedAdapter) PrepareRecord(context.Context, *execution.Record) (*execution.Plan, *pb.Failure) {
	return nil, nil
}

func (*fragmentedAdapter) Close() error { return nil }

func (adapter *fragmentedAdapter) Execute(ctx context.Context, plans []*execution.Plan, emit execution.Emit) {
	adapter.mu.Lock()
	delay := adapter.fixed + time.Duration(len(plans))*adapter.perItem
	if delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			adapter.mu.Unlock()
			return
		}
	}
	adapter.calls.Add(1)
	adapter.items.Add(int64(len(plans)))
	adapter.mu.Unlock()
	for _, work := range plans {
		event := adapter.writeEvent
		if work.Command.GetRead() != nil {
			event = adapter.readEvent
		}
		if err := emit(work, event); err != nil {
			return
		}
	}
}

func fragmentedCases() []fragmentedCase {
	var cases []fragmentedCase
	for _, callers := range []int{1, 32, 256} {
		for _, kind := range []string{"read", "write", "mixed"} {
			writeEvery := 0
			if kind == "write" {
				writeEvery = 1
			}
			if kind == "mixed" {
				writeEvery = 10
			}
			item := fragmentedCase{name: fmt.Sprintf("%s/callers=%d", kind, callers), callers: callers, namespaces: 1, documentBytes: 1024, writeEvery: writeEvery, fixed: 200 * time.Microsecond, wait: 250 * time.Microsecond, batchOperations: 128}
			cases = append(cases, item)
		}
	}
	for _, namespaces := range []int{8, 64, 256} {
		item := fragmentedCase{name: fmt.Sprintf("namespace/callers=256/ns=%d", namespaces), callers: 256, namespaces: namespaces, documentBytes: 1024, writeEvery: 10, fixed: 200 * time.Microsecond, wait: 250 * time.Microsecond, batchOperations: 128}
		cases = append(cases, item)
	}
	for _, writeEvery := range []int{0, 1} {
		item := fragmentedCase{name: fmt.Sprintf("hotkey/callers=256/writeEvery=%d", writeEvery), callers: 256, namespaces: 1, keys: 1, documentBytes: 1024, writeEvery: writeEvery, fixed: 200 * time.Microsecond, wait: 250 * time.Microsecond, batchOperations: 128}
		cases = append(cases, item)
	}
	for _, size := range []int{64, 65536} {
		item := fragmentedCase{name: fmt.Sprintf("payload/callers=256/bytes=%d", size), callers: 256, namespaces: 1, documentBytes: size, writeEvery: 10, fixed: 200 * time.Microsecond, wait: 250 * time.Microsecond, batchOperations: 128}
		cases = append(cases, item)
	}
	for _, fixed := range []time.Duration{0, 2 * time.Millisecond} {
		item := fragmentedCase{name: fmt.Sprintf("cost/callers=256/fixed=%s", fixed), callers: 256, namespaces: 1, documentBytes: 1024, writeEvery: 10, fixed: fixed, perItem: 2 * time.Microsecond, wait: 250 * time.Microsecond, batchOperations: 128}
		cases = append(cases, item)
	}
	for _, wait := range []time.Duration{0, 20 * time.Millisecond} {
		item := fragmentedCase{name: fmt.Sprintf("window/callers=32/wait=%s", wait), callers: 32, namespaces: 1, documentBytes: 1024, writeEvery: 10, fixed: 200 * time.Microsecond, wait: wait, batchOperations: 128}
		cases = append(cases, item)
	}
	item := fragmentedCase{name: "unbatched/callers=256", callers: 256, namespaces: 1, documentBytes: 1024, writeEvery: 10, fixed: 200 * time.Microsecond, batchOperations: 1}
	cases = append(cases, item)
	return cases
}

func fragmentedPlan(config fragmentedCase, worker int, write bool) *execution.Plan {
	key := fmt.Sprintf("key-%d", worker)
	if config.keys > 0 {
		key = fmt.Sprintf("key-%d", worker%config.keys)
	}
	command := &pb.Command{}
	resultBytes := execution.ResultOverheadBytes
	if write {
		document := &pb.Document{ContentType: "application/json", Data: make([]byte, config.documentBytes)}
		action := &pb.MutateRequest_Put{Put: document}
		request := &pb.MutateRequest{Resource: key, Action: action}
		command.Operation = &pb.Command_Mutate{Mutate: request}
	} else {
		request := &pb.ReadRequest{Resource: key}
		command.Operation = &pb.Command_Read{Read: request}
		resultBytes += config.documentBytes
	}
	work := &execution.Plan{ID: uint64(worker), Command: command, Key: key, BatchKey: fmt.Sprintf("namespace-%d", worker%config.namespaces), Bytes: execution.EntryOverheadBytes + config.documentBytes, ResultBytes: resultBytes}
	return work
}

func BenchmarkRuntimeFragmented(b *testing.B) {
	for _, config := range fragmentedCases() {
		b.Run(config.name, func(b *testing.B) { benchmarkRuntimeFragmented(b, config) })
	}
}

func benchmarkRuntimeFragmented(b *testing.B, config fragmentedCase) {
	document := &pb.Document{ContentType: "application/json", Data: make([]byte, config.documentBytes)}
	read := protocol.ReadDocument(document)
	readValue := &pb.Event_ReadResult{ReadResult: read}
	readEvent := &pb.Event{Value: readValue}
	mutation := &pb.MutationResult{Outcome: pb.MutationOutcome_APPLIED}
	writeValue := &pb.Event_MutationResult{MutationResult: mutation}
	writeEvent := &pb.Event{Value: writeValue}
	adapter := &fragmentedAdapter{fixed: config.fixed, perItem: config.perItem, readEvent: readEvent, writeEvent: writeEvent}
	limits := DefaultLimits()
	limits.BatchOperations, limits.BatchWait = config.batchOperations, config.wait
	limits.QueueOperations, limits.QueueBytes = 4096, 512<<20
	runtime, err := New(adapter, limits, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			b.Error(err)
		}
	}()
	reads, writes := make([]*execution.Plan, config.callers), make([]*execution.Plan, config.callers)
	for worker := range config.callers {
		reads[worker], writes[worker] = fragmentedPlan(config, worker, false), fragmentedPlan(config, worker, true)
	}
	var workers sync.WaitGroup
	var failed atomic.Int64
	var latency [64]atomic.Int64
	var latencyTotal atomic.Int64
	start := make(chan struct{})
	for worker := range config.callers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for index := worker; index < b.N; index += config.callers {
				work := reads[worker]
				if config.writeEvery > 0 && index%config.writeEvery == 0 {
					work = writes[worker]
				}
				began := time.Now()
				ticket, failure, _ := runtime.Submit(b.Context(), work, nil)
				if failure != nil {
					failed.Add(1)
					continue
				}
				_, err := ticket.Wait(b.Context())
				if err != nil {
					failed.Add(1)
				}
				ticket.Ack()
				duration := time.Since(began).Nanoseconds()
				latencyTotal.Add(duration)
				latency[min(bits.Len64(uint64(duration)), 63)].Add(1)
			}
		}()
	}
	b.ReportAllocs()
	b.ResetTimer()
	close(start)
	workers.Wait()
	b.StopTimer()
	if failed.Load() != 0 {
		b.Fatalf("failed operations: %d", failed.Load())
	}
	calls, items := adapter.calls.Load(), adapter.items.Load()
	if items != int64(b.N) {
		b.Fatalf("completed %d of %d operations", items, b.N)
	}
	b.ReportMetric(float64(items)/b.Elapsed().Seconds(), "operations/s")
	b.ReportMetric(float64(calls)/float64(items), "calls/op")
	b.ReportMetric(float64(items)/float64(calls), "ops/call")
	b.ReportMetric(float64(latencyTotal.Load())/float64(items)/1000, "mean-us")
	for _, percentile := range []int64{50, 99} {
		var cumulative int64
		for bucket := range latency {
			cumulative += latency[bucket].Load()
			if cumulative*100 >= items*percentile {
				b.ReportMetric(float64(uint64(1)<<bucket)/1000, fmt.Sprintf("p%d-upper-us", percentile))
				break
			}
		}
	}
}
