package mongodb

import (
	"context"
	"fmt"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/luaengine"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

func memoryProgramWork(source string, owner execution.Memory) *execution.Plan {
	program := &luaengine.Program{Source: source}
	native := &plan{id: "item", program: program}
	command := &pb.Command{}
	work := &execution.Plan{Command: command, Backend: native, Memory: owner}
	return work
}

func TestMongoProgramMemoryDenialIsNotAppliedAndPreservesBatchOwners(t *testing.T) {
	probe := &recordMemoryProbe{limit: 1 << 20}
	memory := &recordMemory{owner: probe, base: 8192, held: 4096}
	ctx := context.WithValue(t.Context(), recordMemoryContextKey, memory)
	source := `return function() local s='x'; for i=1,29 do s=s..s end; return {s=s} end`
	work := memoryProgramWork(source, probe)
	write, result := transformProgram(ctx, work, nil)
	if write != nil || result.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || result.GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
		t.Fatal("Lua memory failure permitted a backend write", write, result)
	}
	if probe.peak > probe.limit || memory.base != 8192 || memory.held != 4096 || probe.working < memory.base+memory.held {
		t.Fatal("Lua replaced the batch's existing promises", probe.peak, probe.working, memory)
	}
}

func TestMongoProgramRejectTransfersToItsRecordBeforeNextLua(t *testing.T) {
	batchOwner := &recordMemoryProbe{limit: 16 << 20}
	resultOwner := &recordMemoryProbe{}
	memory := &recordMemory{owner: batchOwner, base: 8192, held: 4096}
	ctx := context.WithValue(t.Context(), recordMemoryContextKey, memory)
	first := memoryProgramWork(`return function() local s='x'; for i=1,16 do s=s..s end; return weir.reject(s) end`, resultOwner)
	write, rejected := transformProgram(ctx, first, nil)
	if write != nil || rejected.GetFailure().GetCode() != pb.FailureCode_PRECONDITION_FAILED || len(rejected.GetFailure().GetMessage()) != 1<<16 {
		t.Fatal("expected retained Lua rejection", write, rejected.GetFailure().GetCode())
	}
	retained := resultOwner.result
	if retained < len(rejected.Failure.Message)+execution.ResultOverheadBytes || batchOwner.result != 0 {
		t.Fatal("rejection used temporary scratch or the batch's first record owner", retained, batchOwner.result)
	}
	second := memoryProgramWork(`return function() return weir.keep() end`, batchOwner)
	_, kept := transformProgram(ctx, second, nil)
	if kept.GetOutcome() != pb.MutationOutcome_APPLIED || kept.GetFailure() != nil || resultOwner.result != retained || len(rejected.Failure.Message) != 1<<16 {
		t.Fatal("next Lua released the earlier record's rejection", kept, retained, resultOwner.result)
	}
}

func TestMongoProgramRejectResultDenialDoesNotPublishMessage(t *testing.T) {
	probe := &recordMemoryProbe{limit: 16 << 20, resultLimit: 1024}
	memory := &recordMemory{owner: probe, base: 4096}
	ctx := context.WithValue(t.Context(), recordMemoryContextKey, memory)
	work := memoryProgramWork(`return function() local s='x'; for i=1,12 do s=s..s end; return weir.reject(s) end`, probe)
	write, result := transformProgram(ctx, work, nil)
	if write != nil || result.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || result.GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED || probe.result != 0 {
		t.Fatal("unowned rejection escaped to output", write, result, probe.result)
	}
}

const memoryReplacementSource = `return function(current) local s='x'; for i=1,18 do s=s..s end; current.pad=s; return current end`

func memoryProgramFind(ids ...string) bson.D {
	documents := make(bson.A, 0, len(ids))
	for _, id := range ids {
		document := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int32(0)}}
		documents = append(documents, document)
	}
	cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: documents}}
	return readCursorResponse(cursor)
}

func memoryProgramWrite() bson.D {
	item := bson.D{{Key: "ok", Value: 1}, {Key: "idx", Value: 0}, {Key: "n", Value: 1}, {Key: "nModified", Value: 1}}
	items := bson.A{item}
	return programBulkResponse(items, 1)
}

func memoryPreparedProgram(t *testing.T, adapter *Adapter, id string, owner execution.Memory) *execution.Plan {
	t.Helper()
	opts := batchOperationOptions{resource: "db/records/s:" + id, action: "program", program: memoryReplacementSource}
	work, failure := prepareTestRecord(adapter, batchOperation(t, opts))
	if failure != nil {
		t.Fatal(failure)
	}
	work.Memory = owner
	work.Context = t.Context()
	return work
}

func TestMongoProgramAttemptRetryReusesMemoryWindow(t *testing.T) {
	const held = 4096
	run := func(t *testing.T, retry bool, limit int) (*recordMemoryProbe, *pb.MutationResult) {
		t.Helper()
		probe := &recordMemoryProbe{limit: limit}
		memory := &recordMemory{owner: probe, base: 8192, held: held}
		find, write := memoryProgramFind("item"), memoryProgramWrite()
		success := bson.D{{Key: "ok", Value: 1}}
		responses := []bson.D{collectionQualificationResponse("db", "records"), find, write}
		if retry {
			labels := bson.A{"TransientTransactionError"}
			responses = append(responses, programCommitError(labels), find, write)
		}
		responses = append(responses, success)
		finds, writes := 0, 0
		var work *execution.Plan
		var observations []int64
		monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
			switch e.CommandName {
			case "find":
				finds++
				if memory.held != held {
					t.Errorf("attempt inherited retired bytes: held=%d want=%d", memory.held, held)
				}
			case "bulkWrite":
				writes++
				observations = append(observations, work.Backend.(*plan).program.ObservedAt.UnixNano())
			}
		}}
		adapter := batchMockAdapter(t, responses, monitor)
		work = memoryPreparedProgram(t, adapter, "item", probe)
		ctx := context.WithValue(t.Context(), recordMemoryContextKey, memory)
		plans := []*execution.Plan{work}
		result := adapter.executePrograms(ctx, plans)[0].GetMutationResult()
		wantAttempts := 1
		if retry {
			wantAttempts = 2
		}
		if finds != wantAttempts || writes != wantAttempts || memory.held != held {
			t.Errorf("attempt ownership did not retire: reads=%d writes=%d held=%d", finds, writes, memory.held)
		}
		for _, observed := range observations {
			if work.Backend.(*plan).program.ObservedAt.IsZero() || observed != observations[0] {
				t.Error("fresh retry changed ObservedAt", observations)
			}
		}
		return probe, result
	}
	baseline, result := run(t, false, 0)
	if result.GetOutcome() != pb.MutationOutcome_APPLIED || result.GetFailure() != nil {
		t.Fatal(result)
	}
	retried, result := run(t, true, baseline.peak)
	if result.GetOutcome() != pb.MutationOutcome_APPLIED || result.GetFailure() != nil || retried.peak != baseline.peak {
		t.Fatal("fresh snapshot could not reuse the same memory window", result, baseline.peak, retried.peak)
	}
	t.Logf("single and retried peak=%d; retry succeeded at that exact limit; held baseline=%d", baseline.peak, held)
}

func TestMongoProgramAttemptPartialReadFailureReleasesHeld(t *testing.T) {
	for _, mode := range []string{"malformed", "transient", "memory denial"} {
		t.Run(mode, func(t *testing.T) {
			probe := &recordMemoryProbe{}
			memory := &recordMemory{owner: probe, base: 8192, held: 4096}
			document := bson.D{{Key: "_id", Value: "item"}, {Key: "n", Value: int32(0)}}
			cursor := bson.D{{Key: "id", Value: int64(123)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: bson.A{document}}}
			success := bson.D{{Key: "ok", Value: 1}}
			failure := bson.D{{Key: "ok", Value: 1}}
			if mode == "transient" {
				labels := bson.A{"TransientTransactionError"}
				failure = programCommitError(labels)
			}
			responses := []bson.D{collectionQualificationResponse("db", "records"), readCursorResponse(cursor), failure, success}
			if mode == "memory denial" {
				second := bson.D{{Key: "_id", Value: "peer"}, {Key: "n", Value: int32(0)}}
				cursor = bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.records"}, {Key: "firstBatch", Value: bson.A{document, second}}}
				probe.limit = memory.base + memory.held + len(expressionBSON(t, document))
				responses = []bson.D{collectionQualificationResponse("db", "records"), readCursorResponse(cursor), success}
			}
			if mode == "transient" {
				responses = append(responses, memoryProgramFind("item"), memoryProgramWrite(), success)
			}
			finds, writes, aborts := 0, 0, 0
			monitor := &event.CommandMonitor{Started: func(ctx context.Context, e *event.CommandStartedEvent) {
				if ctx.Err() != nil {
					return
				}
				switch e.CommandName {
				case "find":
					finds++
					if memory.held != 4096 {
						t.Errorf("partial read survived into retry: %d", memory.held)
					}
				case "bulkWrite":
					writes++
				case "abortTransaction":
					aborts++
				}
			}}
			adapter := batchMockAdapter(t, responses, monitor)
			work := memoryPreparedProgram(t, adapter, "item", probe)
			plans := []*execution.Plan{work}
			if mode == "memory denial" {
				plans = append(plans, memoryPreparedProgram(t, adapter, "peer", probe))
			}
			ctx := context.WithValue(t.Context(), recordMemoryContextKey, memory)
			result := adapter.executePrograms(ctx, plans)[0].GetMutationResult()
			wantOutcome, wantFinds, wantWrites := pb.MutationOutcome_NOT_APPLIED, 1, 0
			if mode == "transient" {
				wantOutcome, wantFinds, wantWrites = pb.MutationOutcome_APPLIED, 2, 1
			}
			if result.GetOutcome() != wantOutcome || finds != wantFinds || writes != wantWrites || aborts != 1 || memory.held != 4096 {
				t.Fatal("partial read ownership or rollback changed", result, finds, writes, aborts, memory.held)
			}
			if mode == "memory denial" && result.GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
				t.Fatal("memory denial was swallowed", result)
			}
		})
	}
}

func TestMongoProgramAttemptSplitRetiresParentBeforeChildren(t *testing.T) {
	probe := &recordMemoryProbe{}
	memory := &recordMemory{owner: probe, base: 8192, held: 4096}
	success := bson.D{{Key: "ok", Value: 1}}
	responses := []bson.D{
		collectionQualificationResponse("db", "records"), memoryProgramFind("a", "b"), success,
		memoryProgramFind("a"), memoryProgramWrite(), success,
		memoryProgramFind("b"), memoryProgramWrite(), success,
	}
	finds, writes, aborts, commits := 0, 0, 0, 0
	monitor := &event.CommandMonitor{Started: func(ctx context.Context, e *event.CommandStartedEvent) {
		if ctx.Err() != nil {
			return
		}
		switch e.CommandName {
		case "find":
			finds++
			if memory.held != 4096 {
				t.Errorf("split child inherited parent or sibling replacements: %d", memory.held)
			}
		case "bulkWrite":
			writes++
		case "abortTransaction":
			aborts++
			if memory.held <= 4096 {
				t.Error("test did not retain parent replacements before the split")
			}
		case "commitTransaction":
			commits++
		}
	}}
	adapter := batchMockAdapter(t, responses, monitor)
	opts := adapter.options()
	opts.ExchangeBytes = 350000
	adapter.config.Options = &opts
	plans := []*execution.Plan{memoryPreparedProgram(t, adapter, "a", probe), memoryPreparedProgram(t, adapter, "b", probe)}
	ctx := context.WithValue(t.Context(), recordMemoryContextKey, memory)
	for _, event := range adapter.executePrograms(ctx, plans) {
		result := event.GetMutationResult()
		if result.GetOutcome() != pb.MutationOutcome_APPLIED || result.GetFailure() != nil {
			t.Fatal("split child failed", result)
		}
	}
	if finds != 3 || writes != 2 || aborts != 1 || commits != 2 || memory.held != 4096 {
		t.Fatal("split lifecycle changed", finds, writes, aborts, commits, memory.held)
	}
}

func TestMongoProgramAttemptCancellationReleasesWindowBeforeWrite(t *testing.T) {
	probe := &recordMemoryProbe{limit: 7 << 20}
	memory := &recordMemory{owner: probe, base: 8192, held: 4096}
	ctx := context.WithValue(t.Context(), recordMemoryContextKey, memory)
	scope := newProgramMemoryScope(ctx)
	defer scope.close()
	items := make(bson.A, 8)
	for i := range items {
		items[i] = bson.D{{Key: "ok", Value: 1}, {Key: "idx", Value: i}, {Key: "n", Value: 1}, {Key: "nModified", Value: 1}}
	}
	responses := []bson.D{programBulkResponse(items, len(items))}
	writes := 0
	monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		if e.CommandName != "bulkWrite" {
			return
		}
		writes++
		ops, err := e.Command.Lookup("ops").Array().Values()
		if err != nil || len(ops) != 8 {
			t.Error("survivor write batch was incomplete", len(ops), err)
		}
		for i, op := range ops {
			id := op.Document().Lookup("filter").Document().Lookup("_id").StringValue()
			if id != fmt.Sprint(i+8) {
				t.Error("cancelled replacement reached the backend", id)
			}
		}
	}}
	adapter := batchMockAdapter(t, responses, monitor)
	caller, cancel := context.WithCancel(t.Context())
	defer cancel()
	plans := make([]*execution.Plan, 16)
	positions := make([]int, len(plans))
	for i := range plans {
		id := fmt.Sprint(i)
		work := memoryPreparedProgram(t, adapter, id, probe)
		if i < 8 {
			work.Context = caller
		}
		plans[i], positions[i] = work, i
		document := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int32(0)}}
		raw := expressionBSON(t, document)
		scope.documents = append(scope.documents, raw)
		memory.held += len(raw)
	}
	batch := &programBatch{plans: plans, results: make([]*pb.MutationResult, len(plans))}
	scope.writes, positions = adapter.transformPrograms(ctx, batch, positions, scope.documents)
	if len(scope.writes) != len(plans) {
		t.Fatal("fixture did not retain all Lua replacements within the limit", len(scope.writes), batch.results)
	}
	expectedHeld, framing := scope.held, 0
	for _, work := range scope.writes[8:] {
		document := work.Backend.(*plan).document
		expectedHeld += cap(document)
		framing += 2*len(document) + execution.EntryOverheadBytes
	}
	staleWorking := memory.base + memory.held + framing
	expectedWorking := memory.base + expectedHeld + framing
	if staleWorking <= probe.limit || expectedWorking > probe.limit {
		t.Fatal("fixture does not distinguish stale from survivor ownership", staleWorking, expectedWorking, probe.limit)
	}
	cancel()
	workingBeforeFilter := probe.working
	positions = scope.filterCancelled(ctx, batch, positions)
	if probe.working != workingBeforeFilter {
		t.Fatal("filter changed the working reservation instead of retiring ownership", probe.working, workingBeforeFilter)
	}
	if len(scope.writes) != 8 || len(positions) != 8 {
		t.Fatal("filter did not preserve all survivors", len(scope.writes), positions)
	}
	replies, settled, err := adapter.writePrograms(ctx, scope.writes)
	if err != nil || !settled || len(replies) != 8 || writes != 1 {
		t.Fatalf("survivor write could not reuse cancelled replacement capacity: err=%v settled=%v replies=%d writes=%d held=%d wantHeld=%d staleWorking=%d survivorWorking=%d", err, settled, len(replies), writes, memory.held, expectedHeld, staleWorking, expectedWorking)
	}
	for _, reply := range replies {
		if reply.GetOutcome() != pb.MutationOutcome_APPLIED || reply.GetFailure() != nil {
			t.Fatal("survivor write failed", reply)
		}
	}
	if memory.held != expectedHeld || probe.working != expectedWorking {
		t.Fatal("survivor write used incorrect ownership", memory.held, expectedHeld, probe.working, expectedWorking)
	}
	for i := range 8 {
		if batch.results[i].GetFailure().GetCode() != pb.FailureCode_CANCELLED {
			t.Fatal("cancelled member lost its result", i, batch.results[i])
		}
	}
	scope.close()
	if memory.held != 4096 {
		t.Fatal("retiring survivors removed parent ownership", memory.held)
	}
	t.Logf("stale request=%d exceeds limit=%d; cancelled replacements retired, survivor write succeeds at=%d", staleWorking, probe.limit, expectedWorking)
}

func TestMongoProgramAttemptCancellationClearsReplacementSlots(t *testing.T) {
	for _, action := range []string{"replace", "delete", "mixed"} {
		for _, cancelled := range []string{"a", "b", "c", "all"} {
			t.Run(action+"/"+cancelled, func(t *testing.T) {
				probe := &recordMemoryProbe{}
				memory := &recordMemory{owner: probe, base: 8192, held: 4096}
				ctx := context.WithValue(t.Context(), recordMemoryContextKey, memory)
				scope := newProgramMemoryScope(ctx)
				defer scope.close()
				config := Config{Store: "mongo"}
				adapter := &Adapter{config: config}
				initializeTestLifetime(adapter)
				caller, cancel := context.WithCancel(t.Context())
				defer cancel()
				var plans []*execution.Plan
				for _, id := range []string{"a", "b", "c"} {
					work := memoryPreparedProgram(t, adapter, id, probe)
					if action == "delete" || action == "mixed" && id == "b" {
						work.Backend.(*plan).program.Source = `return function() return weir.delete() end`
					}
					if id == cancelled || cancelled == "all" {
						work.Context = caller
					}
					plans = append(plans, work)
					document := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int32(0)}}
					raw := expressionBSON(t, document)
					scope.documents = append(scope.documents, raw)
					memory.held += len(raw)
				}
				batch := &programBatch{plans: plans, results: make([]*pb.MutationResult, len(plans))}
				positions := []int{0, 1, 2}
				scope.writes, positions = adapter.transformPrograms(ctx, batch, positions, scope.documents)
				if len(scope.writes) != 3 {
					t.Fatal("test did not produce all writes", len(scope.writes))
				}
				initialHeld, expectedHeld := scope.held, scope.held
				for _, write := range scope.writes {
					native := write.Backend.(*plan)
					if native.action == "delete" && native.document != nil {
						t.Fatal("delete fixture retained replacement bytes")
					}
					initialHeld += cap(native.document)
					if native.id != cancelled && cancelled != "all" {
						expectedHeld += cap(native.document)
					}
				}
				if memory.held != initialHeld {
					t.Fatal("fixture did not charge its actual retained writes", memory.held, initialHeld)
				}
				backing := scope.writes[:cap(scope.writes)]
				documents := scope.documents[:cap(scope.documents)]
				cancel()
				positions = scope.filterCancelled(ctx, batch, positions)
				want := 2
				if cancelled == "all" {
					want = 0
				}
				if len(scope.writes) != want || len(positions) != want || memory.held != expectedHeld {
					t.Fatal("cancel filter lost peer ownership", len(scope.writes), positions, memory.held, expectedHeld)
				}
				for i, write := range backing {
					if i >= want && write != nil {
						t.Fatal("cancelled replacement remained in backing slot", i)
					}
					if i < want && (write.Backend.(*plan).id == cancelled || cancelled == "all") {
						t.Fatal("cancelled replacement survived compaction", i)
					}
				}
				positions = scope.filterCancelled(ctx, batch, positions)
				if len(positions) != want || memory.held != expectedHeld {
					t.Fatal("repeated filtering released the same write twice", positions, memory.held, expectedHeld)
				}
				scope.close()
				for i, write := range backing {
					if write != nil {
						t.Fatal("attempt retained a write after retirement", i)
					}
				}
				for i, document := range documents {
					if document != nil {
						t.Fatal("attempt retained a source after retirement", i)
					}
				}
				if memory.held != 4096 {
					t.Fatal("cancel cleanup removed parent ownership", memory.held)
				}
			})
		}
	}
}

func TestMongoProgramAttemptRejectSurvivesAmbiguousCommit(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		t.Run(fmt.Sprint("ambiguous=", ambiguous), func(t *testing.T) {
			probe, resultOwner := &recordMemoryProbe{}, &recordMemoryProbe{}
			memory := &recordMemory{owner: probe, base: 8192, held: 4096}
			responses := []bson.D{collectionQualificationResponse("db", "records"), memoryProgramFind("reject", "write"), memoryProgramWrite()}
			if ambiguous {
				labels := bson.A{"UnknownTransactionCommitResult"}
				responses = append(responses, programCommitError(labels), programCommitError(labels))
			} else {
				success := bson.D{{Key: "ok", Value: 1}}
				responses = append(responses, success)
			}
			finds, writes, commits := 0, 0, 0
			monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
				switch e.CommandName {
				case "find":
					finds++
				case "bulkWrite":
					writes++
				case "commitTransaction":
					commits++
				}
			}}
			adapter := batchMockAdapter(t, responses, monitor)
			opts := adapter.options()
			opts.RetryAttempts = 2
			adapter.config.Options = &opts
			reject := memoryPreparedProgram(t, adapter, "reject", resultOwner)
			reject.Backend.(*plan).program.Source = `return function() local s='x'; for i=1,16 do s=s..s end; return weir.reject(s) end`
			plans := []*execution.Plan{reject, memoryPreparedProgram(t, adapter, "write", probe)}
			ctx := context.WithValue(t.Context(), recordMemoryContextKey, memory)
			results := adapter.executePrograms(ctx, plans)
			rejected, written := results[0].GetMutationResult(), results[1].GetMutationResult()
			wantOutcome, wantCommits := pb.MutationOutcome_APPLIED, 1
			if ambiguous {
				wantOutcome, wantCommits = pb.MutationOutcome_UNKNOWN, 2
			}
			if rejected.GetFailure().GetCode() != pb.FailureCode_PRECONDITION_FAILED || len(rejected.GetFailure().GetMessage()) != 1<<16 || resultOwner.result != (1<<16)+execution.ResultOverheadBytes || resultOwner.calls != 1 {
				t.Fatal("attempt retirement lost per-record reject ownership", rejected, resultOwner)
			}
			if written.GetOutcome() != wantOutcome || finds != 1 || writes != 1 || commits != wantCommits || memory.held != 4096 || probe.result != 0 {
				t.Fatal("commit ambiguity replayed Lua or retained attempt bytes", written, finds, writes, commits, memory.held)
			}
		})
	}
}

func TestMongoProgramAttemptManagedGroupsAndChunksReuseWindow(t *testing.T) {
	for _, chunk := range []bool{false, true} {
		t.Run(fmt.Sprint("chunks=", chunk), func(t *testing.T) {
			run := func(limit int) (*recordMemoryProbe, []*pb.Event, int) {
				probe := &recordMemoryProbe{limit: limit}
				firstPeak := 0
				monitor := &event.CommandMonitor{Succeeded: func(_ context.Context, e *event.CommandSucceededEvent) {
					if e.CommandName == "commitTransaction" && firstPeak == 0 {
						firstPeak = probe.peak
					}
				}}
				success := bson.D{{Key: "ok", Value: 1}}
				find, write := memoryProgramFind("item"), memoryProgramWrite()
				responses := []bson.D{collectionQualificationResponse("db", "records"), find, write, success, find, write, success}
				adapter := batchMockAdapter(t, responses, monitor)
				if chunk {
					opts := adapter.options()
					opts.ExchangeBytes = 1
					adapter.config.Options = &opts
				}
				// Duplicate identities force independent groups even within one chunk.
				plans := []*execution.Plan{memoryPreparedProgram(t, adapter, "item", probe), memoryPreparedProgram(t, adapter, "item", probe)}
				results := adapter.executeRecords(t.Context(), plans)
				return probe, results, firstPeak
			}
			baseline, results, firstPeak := run(0)
			for _, result := range results {
				if result.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED || result.GetMutationResult().GetFailure() != nil {
					t.Fatal(result)
				}
			}
			probe, results, _ := run(firstPeak)
			for _, result := range results {
				if result.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED || result.GetMutationResult().GetFailure() != nil {
					t.Fatal("managed record groups accumulated retired replacements", result, baseline.peak, probe.peak)
				}
			}
		})
	}
}
