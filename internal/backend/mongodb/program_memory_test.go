package mongodb

import (
	"context"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/luaengine"
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
