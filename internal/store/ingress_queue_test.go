package store

import (
	"context"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

func TestIngressOwnedQueueWaitFollowsActualVacancy(t *testing.T) {
	memory := testExecutionMemory(192 << 20)
	limits := DefaultLimits()
	limits.QueueOperations = 1
	runtime := newRuntime(nil, limits, memory)
	defer memory.unregister(runtime.wake)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	first := plan(1, "first", false)
	ticket, failure, _ := runtime.Submit(ctx, first, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	defer ticket.stopWatch()
	defer ticket.executionLease.release()
	account, err := memory.NewIngress(ctx, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer account.Retire()
	defer account.Close()
	if err := account.ReserveMessage(128); err != nil {
		t.Fatal(err)
	}
	bytes := account.Decoded()
	input := account.Input(bytes)
	defer input.Release()
	second := plan(2, "second", false)
	input.Bind(second)
	_, failure, changed := runtime.Submit(ctx, second, nil)
	if failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED || changed == nil || input.bytes == 0 {
		t.Fatal("owned input lost bounded vacancy wait", failure, changed, memory.Snapshot())
	}
	before := memory.Snapshot()
	runtime.mu.Lock()
	runtime.dequeueLocked(ticket)
	runtime.pendingBytes -= first.Bytes
	runtime.notifyLocked()
	runtime.mu.Unlock()
	select {
	case <-changed:
	default:
		t.Fatal("actual dequeue did not wake existing waiter")
	}
	next, failure, _ := runtime.Submit(ctx, second, nil)
	if failure != nil || input.bytes != 0 {
		t.Fatal("vacancy failed same-source exchange", failure, before, memory.Snapshot())
	}
	next.stopWatch()
	next.executionLease.release()
}

func TestIngressQueueRejectsForeignAndClosedSourcesWithoutWait(t *testing.T) {
	for _, closed := range []bool{false, true} {
		memory := testExecutionMemory(192 << 20)
		foreign := testExecutionMemory(192 << 20)
		limits := DefaultLimits()
		limits.QueueOperations = 1
		runtime := newRuntime(nil, limits, memory)
		defer memory.unregister(runtime.wake)
		first := plan(1, "first", false)
		ticket, failure, _ := runtime.Submit(t.Context(), first, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		defer ticket.stopWatch()
		defer ticket.executionLease.release()
		source := foreign
		if closed {
			source = memory
		}
		account, err := source.NewIngress(t.Context(), 4096)
		if err != nil {
			t.Fatal(err)
		}
		if err := account.ReserveMessage(128); err != nil {
			t.Fatal(err)
		}
		input := account.Input(account.Decoded())
		second := plan(2, "second", false)
		input.Bind(second)
		if closed {
			input.Release()
			account.Close()
		}
		_, failure, changed := runtime.Submit(t.Context(), second, nil)
		if failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED || changed != nil {
			t.Fatal("unowned source created external waiter", closed, failure, changed)
		}
		input.Release()
		account.Close()
		account.Retire()
		if source == foreign && source.Snapshot().UsedBytes != 0 {
			t.Fatal("foreign source leaked after rejection", source.Snapshot())
		}
	}
}

func TestIngressCanceledQueueWaitRetainsUntilSourceDiscarded(t *testing.T) {
	memory := testExecutionMemory(192 << 20)
	limits := DefaultLimits()
	limits.QueueOperations = 1
	runtime := newRuntime(nil, limits, memory)
	defer memory.unregister(runtime.wake)
	ctx, cancel := context.WithCancel(t.Context())
	first := plan(1, "first", false)
	ticket, failure, _ := runtime.Submit(ctx, first, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	account, err := memory.NewIngress(ctx, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := account.ReserveMessage(128); err != nil {
		t.Fatal(err)
	}
	input := account.Input(account.Decoded())
	second := plan(2, "second", false)
	input.Bind(second)
	_, failure, changed := runtime.Submit(ctx, second, nil)
	if failure == nil || changed == nil {
		t.Fatal(failure, changed)
	}
	held := memory.Snapshot().IngressReservedBytes
	cancel()
	_, failure, _ = runtime.Submit(ctx, second, nil)
	if failure.GetCode() != pb.FailureCode_CANCELLED || memory.Snapshot().IngressReservedBytes != held {
		t.Fatal("cancel erased retained source", failure, memory.Snapshot())
	}
	input.Release()
	account.Close()
	account.Retire()
	ticket.stopWatch()
	ticket.executionLease.release()
	if snapshot := memory.Snapshot(); snapshot.UsedBytes != 0 || snapshot.IngressReservedBytes != 0 || len(memory.ingressHandoffs) != 0 {
		t.Fatal(snapshot)
	}
}
