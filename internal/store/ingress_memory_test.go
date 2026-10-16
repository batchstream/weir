package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/overload"
)

func TestIngressOwnershipSurvivesCancellationAndTrustedShrink(t *testing.T) {
	memory := testExecutionMemory(384 << 20)
	ctx, cancel := context.WithCancel(t.Context())
	account, err := memory.NewIngress(ctx, 4096)
	if err != nil {
		t.Fatal(err)
	}
	release, err := account.Buffer(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := account.ReserveMessage(12 << 20); err != nil {
		t.Fatal(err)
	}
	before := memory.Snapshot()
	if before.IngressBytes != 513 || before.IngressBuffers != 1 || before.IngressMessages != 1 || before.IngressReservedBytes != uint64((48<<20)+(16<<10)+512+8192) || before.UsedBytes != before.IngressReservedBytes {
		t.Fatal(before)
	}
	cancel()
	sample := overload.Snapshot{RecoveryAllowanceKnown: true}
	memory.ObserveMemory(sample)
	if after := memory.Snapshot(); after.UsedBytes != before.UsedBytes || after.IngressBuffers != 1 {
		t.Fatal("cancel/sample erased actual owners", before, after)
	}
	account.Retire()
	account.Close()
	held := memory.Snapshot()
	if held.IngressBytes != 513 || held.IngressReservedBytes != 513 || held.IngressBuffers != 1 || held.IngressMessages != 0 {
		t.Fatal("handler return erased surviving raw owner", held)
	}
	release()
	release()
	final := memory.Snapshot()
	if final.UsedBytes != 0 || final.ResidentBytes != 0 || final.IngressReservedBytes != 0 || final.IngressBytes != 0 || final.IngressBuffers != 0 {
		t.Fatal(final)
	}
}

func TestIngressWaitDoesNotBlockSmallMessageOrFinalFree(t *testing.T) {
	memory := testExecutionMemory(192 << 20)
	large, err := memory.NewIngress(t.Context(), 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := large.ReserveMessage(18 << 20); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	waiting, err := memory.NewIngress(ctx, 4096)
	if err != nil {
		t.Fatal(err)
	}
	release, err := waiting.Buffer(16 << 10)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- waiting.ReserveMessage(18 << 20) }()
	deadline := time.Now().Add(time.Second)
	for memory.Snapshot().IngressWaiting != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if memory.Snapshot().IngressWaiting != 1 {
		t.Fatal("large message did not wait", memory.Snapshot())
	}
	short, err := memory.NewIngress(t.Context(), 4096)
	if err != nil {
		t.Fatal("small request excluded by waiting large frame", err, memory.Snapshot())
	}
	if err := short.ReserveMessage(128); err != nil {
		t.Fatal(err)
	}
	bytes := short.Decoded()
	short.ReleaseDecoded(bytes)
	short.Retire()
	short.Close()
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal("waiting cancel", err)
	}
	waiting.Retire()
	waiting.Close()
	if memory.Snapshot().IngressBytes == 0 {
		t.Fatal("cancel released DATA before final Free")
	}
	release()
	large.Retire()
	large.Close()
	if final := memory.Snapshot(); final.UsedBytes != 0 || final.IngressWaiting != 0 {
		t.Fatal(final)
	}
}

func TestIngressActualCapacityAndConcurrentFinalOwnersBounded(t *testing.T) {
	memory := testExecutionMemory(192 << 20)
	account, err := memory.NewIngress(t.Context(), 4096)
	if err != nil {
		t.Fatal(err)
	}
	const count = 2048
	releases := make([]func(), 0, count)
	for range count {
		release, err := account.Buffer(1)
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, release)
	}
	before := memory.Snapshot()
	if before.IngressBytes != count*513 || before.IngressReservedBytes != count*513+4096 || before.IngressBuffers != count {
		t.Fatal(before)
	}
	account.Retire()
	account.Close()
	var wg sync.WaitGroup
	for _, release := range releases {
		wg.Go(func() { release(); release() })
	}
	wg.Wait()
	if final := memory.Snapshot(); final.UsedBytes != 0 || final.IngressBuffers != 0 || final.ResidentBytes != 0 {
		t.Fatal(final)
	}
}

func TestIngressSmallMessageLargeBackingDecodeHasPreReservedCapacity(t *testing.T) {
	memory := testExecutionMemory(192 << 20)
	account, err := memory.NewIngress(t.Context(), 4096)
	if err != nil {
		t.Fatal(err)
	}
	release, err := account.Buffer(16 << 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := account.ReserveMessage(55); err != nil {
		t.Fatal(err)
	}
	before := memory.Snapshot()
	memory.mu.Lock()
	memory.budget = memory.used
	memory.mu.Unlock()
	bytes := account.Decoded()
	after := memory.Snapshot()
	if bytes != 55+4096 || after.IngressMessages != 0 || after.UsedBytes > before.UsedBytes || after.IngressBytes != before.IngressBytes {
		t.Fatal("decode lacked its pre-reserved backing/field-copy peak", before, after, bytes)
	}
	account.ReleaseMessage()
	account.ReleaseDecoded(bytes)
	account.Retire()
	account.Close()
	release()
	if final := memory.Snapshot(); final.UsedBytes != 0 || final.IngressMessages != 0 {
		t.Fatal(final)
	}
}

func TestIngressImpossibleMessageFailsWithoutWaitingForDeadline(t *testing.T) {
	memory := testExecutionMemory(192 << 20)
	account, err := memory.NewIngress(t.Context(), 4096)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	err = account.ReserveMessage(64 << 20)
	if err == nil || time.Since(started) > 100*time.Millisecond || memory.Snapshot().IngressWaiting != 0 {
		t.Fatal("impossible input waited for caller timeout", err, time.Since(started), memory.Snapshot())
	}
	account.Retire()
	account.Close()
}

func TestIngressAdmissionTransfersTheSameSourceAtomically(t *testing.T) {
	memory := testExecutionMemory(192 << 20)
	account, err := memory.NewIngress(t.Context(), 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := account.ReserveMessage(12 << 20); err != nil {
		t.Fatal(err)
	}
	bytes := account.Decoded()
	input := account.Input(bytes)
	work := plan(1, "handoff", false)
	work.Bytes = 12 << 20
	input.Bind(work)
	before := memory.Snapshot()
	// The full input fits after a same-source exchange, but not if the decoded
	// ingress allocation and execution input promise are charged independently.
	memory.mu.Lock()
	memory.budget = uint64(work.Bytes+nativeQueuedWindow) + 4096
	memory.reserve = 0
	memory.mu.Unlock()
	lease, failure, _ := memory.admit(work)
	if failure != nil {
		t.Fatal("same source was charged twice", failure, before, memory.Snapshot())
	}
	after := memory.Snapshot()
	if after.UsedBytes > after.BudgetBytes || after.IngressReservedBytes != 4096 || input.bytes != 0 || after.InputUsedBytes != uint64(work.Bytes+nativeQueuedWindow) {
		t.Fatal(before, after)
	}
	input.Release()
	lease.release()
	account.Retire()
	account.Close()
	if final := memory.Snapshot(); final.UsedBytes != 0 {
		t.Fatal(final)
	}
}

func TestIngressFailedHandoffPreservesDecodedOwnership(t *testing.T) {
	memory := testExecutionMemory(192 << 20)
	account, err := memory.NewIngress(t.Context(), 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := account.ReserveMessage(12 << 20); err != nil {
		t.Fatal(err)
	}
	bytes := account.Decoded()
	input := account.Input(bytes)
	work := plan(1, "handoff", false)
	work.Bytes = 12 << 20
	input.Bind(work)
	before := memory.Snapshot()
	memory.mu.Lock()
	memory.budget = 1
	memory.mu.Unlock()
	_, failure, _ := memory.admit(work)
	after := memory.Snapshot()
	if failure == nil || after.IngressReservedBytes != before.IngressReservedBytes || after.UsedBytes != before.UsedBytes || input.bytes == 0 {
		t.Fatal("failed transfer erased original source", failure, before, after)
	}
	input.Release()
	account.Retire()
	account.Close()
	if final := memory.Snapshot(); final.UsedBytes != 0 || len(memory.ingressHandoffs) != 0 {
		t.Fatal(final)
	}
}

func TestIngressFixedRPCAccountNeedsHandlerAndNativeRetirement(t *testing.T) {
	for _, handlerFirst := range []bool{false, true} {
		memory := testExecutionMemory(192 << 20)
		account, err := memory.NewIngress(t.Context(), 4096)
		if err != nil {
			t.Fatal(err)
		}
		if handlerFirst {
			account.Close()
		} else {
			account.Retire()
		}
		if snapshot := memory.Snapshot(); snapshot.IngressReservedBytes != 4096 {
			t.Fatal("one lifecycle edge erased live RPC metadata", handlerFirst, snapshot)
		}
		if handlerFirst {
			account.Retire()
		} else {
			account.Close()
		}
		if snapshot := memory.Snapshot(); snapshot.UsedBytes != 0 || snapshot.IngressReservedBytes != 0 || len(memory.wakes) != 0 {
			t.Fatal("joined lifecycle retained RPC metadata", handlerFirst, snapshot)
		}
	}
}

func TestIngressWaitingStopsOnHandlerCloseAndTrustedShrink(t *testing.T) {
	for _, shrink := range []bool{false, true} {
		memory := testExecutionMemory(192 << 20)
		held, err := memory.NewIngress(t.Context(), 4096)
		if err != nil {
			t.Fatal(err)
		}
		if err := held.ReserveMessage(18 << 20); err != nil {
			t.Fatal(err)
		}
		account, err := memory.NewIngress(t.Context(), 4096)
		if err != nil {
			t.Fatal(err)
		}
		result := make(chan error, 1)
		go func() { result <- account.ReserveMessage(18 << 20) }()
		deadline := time.Now().Add(time.Second)
		for memory.Snapshot().IngressWaiting != 1 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if memory.Snapshot().IngressWaiting != 1 {
			t.Fatal("test did not create waiting message")
		}
		if shrink {
			sample := overload.Snapshot{RecoveryAllowance: 1, RecoveryAllowanceKnown: true}
			memory.ObserveMemory(sample)
		} else {
			account.Retire()
			account.Close()
		}
		select {
		case err := <-result:
			if err == nil {
				t.Fatal("closed/shrunken input was admitted")
			}
		case <-time.After(time.Second):
			t.Fatal("wait survived handler close/trusted impossible capacity", shrink, memory.Snapshot())
		}
		account.Retire()
		account.Close()
		held.Retire()
		held.Close()
		if snapshot := memory.Snapshot(); snapshot.UsedBytes != 0 || snapshot.IngressWaiting != 0 || len(memory.wakes) != 0 {
			t.Fatal(snapshot)
		}
	}
}
