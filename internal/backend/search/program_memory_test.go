package search

import (
	"context"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/luaengine"
)

func memoryProgramWork(source string, owner execution.Memory) *execution.Plan {
	program := &luaengine.Program{Source: source}
	native := &plan{id: "item", index: "records", program: program}
	command := &pb.Command{}
	work := &execution.Plan{Command: command, Backend: native, Memory: owner}
	return work
}

func TestSearchProgramMemoryDenialIsNotAppliedAndPreservesBatchOwners(t *testing.T) {
	probe := &recordMemoryProbe{limit: 1 << 20}
	memory := &recordMemory{owner: probe, base: 8192, reply: 65537, sources: 137, frame: 16384}
	ctx := context.WithValue(t.Context(), recordMemoryContextKey, memory)
	source := `return function() local s='x'; for i=1,29 do s=s..s end; return {s=s} end`
	work := memoryProgramWork(source, probe)
	found := false
	current := &getReply{Found: &found}
	adapter := &Adapter{}
	write, result := adapter.evaluateProgram(ctx, work, current)
	if write != nil || result.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || result.GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
		t.Fatal("Lua memory failure permitted a backend write", write, result)
	}
	if probe.peak > probe.limit || memory.base != 8192 || memory.reply != 65537 || memory.sources != 137 || memory.frame != 16384 {
		t.Fatal("Lua replaced the existing input/source/frame/ACK promises", probe.peak, memory)
	}
}

func TestSearchProgramRejectTransfersToItsRecordBeforeNextLua(t *testing.T) {
	batchOwner := &recordMemoryProbe{limit: 16 << 20}
	resultOwner := &recordMemoryProbe{}
	memory := &recordMemory{owner: batchOwner, base: 8192, reply: 65537, sources: 137, frame: 16384}
	ctx := context.WithValue(t.Context(), recordMemoryContextKey, memory)
	first := memoryProgramWork(`return function() local s='x'; for i=1,16 do s=s..s end; return weir.reject(s) end`, resultOwner)
	found := false
	current := &getReply{Found: &found}
	adapter := &Adapter{}
	write, rejected := adapter.evaluateProgram(ctx, first, current)
	if write != nil || rejected.GetFailure().GetCode() != pb.FailureCode_PRECONDITION_FAILED || len(rejected.GetFailure().GetMessage()) != 1<<16 {
		t.Fatal("expected retained Lua rejection", write, rejected.GetFailure().GetCode())
	}
	retained := resultOwner.result
	if retained < len(rejected.Failure.Message)+execution.ResultOverheadBytes || batchOwner.result != 0 {
		t.Fatal("rejection used temporary scratch or the batch's first record owner", retained, batchOwner.result)
	}
	second := memoryProgramWork(`return function() return weir.keep() end`, batchOwner)
	_, kept := adapter.evaluateProgram(ctx, second, current)
	if kept.GetOutcome() != pb.MutationOutcome_APPLIED || kept.GetFailure() != nil || resultOwner.result != retained || len(rejected.Failure.Message) != 1<<16 {
		t.Fatal("next Lua released the earlier record's rejection", kept, retained, resultOwner.result)
	}
}

func TestSearchProgramRejectResultDenialDoesNotPublishMessage(t *testing.T) {
	probe := &recordMemoryProbe{limit: 16 << 20, resultLimit: 1024}
	memory := &recordMemory{owner: probe, base: 4096, reply: 65537}
	ctx := context.WithValue(t.Context(), recordMemoryContextKey, memory)
	work := memoryProgramWork(`return function() local s='x'; for i=1,12 do s=s..s end; return weir.reject(s) end`, probe)
	found := false
	current := &getReply{Found: &found}
	adapter := &Adapter{}
	write, result := adapter.evaluateProgram(ctx, work, current)
	if write != nil || result.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || result.GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED || probe.result != 0 {
		t.Fatal("unowned rejection escaped to output", write, result, probe.result)
	}
}
