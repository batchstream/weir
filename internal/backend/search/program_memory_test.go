package search

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/luaengine"
)

type initialAllocationDeadlineMemory struct {
	recordMemoryProbe
	calls    int
	waits    int
	deadline time.Time
	entered  chan context.Context
}

func (m *initialAllocationDeadlineMemory) SetWorking(ctx context.Context, bytes int) error {
	m.calls++
	if !m.deadline.IsZero() {
		actual, bounded := ctx.Deadline()
		if !bounded || !actual.Equal(m.deadline) {
			return m.recordMemoryProbe.SetWorking(ctx, bytes)
		}
	}
	m.waits++
	if m.entered != nil {
		m.entered <- ctx
	}
	<-ctx.Done()
	return ctx.Err()
}

func TestSearchInitialLuaReservationDeadlinePreservesEvidence(t *testing.T) {
	owner := &initialAllocationDeadlineMemory{}
	memory := &recordMemory{owner: owner, base: 8192, reply: 64 << 10}
	allocation, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	ctx := context.WithValue(allocation, recordMemoryContextKey, memory)
	work := memoryProgramWork(`return function() return weir.keep() end`, owner)
	found := false
	current := &getReply{Found: &found}
	adapter := &Adapter{}
	write, result := adapter.evaluateProgram(ctx, work, current)
	if write != nil || result.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || result.GetFailure().GetCode() != pb.FailureCode_DEADLINE_EXCEEDED {
		t.Fatal("initial Lua allocation deadline was mislabeled as capacity exhaustion", write, result)
	}
	if owner.calls != 1 || owner.working != 0 || owner.result != 0 || !work.Backend.(*plan).program.ObservedAt.IsZero() {
		t.Fatal("deadline permitted decoding, VM entry or write ownership", owner.calls, owner.working, owner.result)
	}
}

type evaluationContextTransport struct{ writes int }

func (transport *evaluationContextTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	raw := ""
	switch request.URL.Path {
	case "/records":
		raw = `{"records":{"settings":{"index.uuid":"test","index.number_of_shards":"1"},"mappings":{"_source":{"enabled":true}}}}`
	case "/records/_mget":
		raw = `{"docs":[{"_index":"records","_id":"item","found":true,"_seq_no":1,"_primary_term":1,"_source":{"n":1}}]}`
	default:
		transport.writes++
		return nil, errors.New("unexpected backend operation after canceled allocation")
	}
	response := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(raw)), ContentLength: int64(len(raw)), Request: request}
	return response, nil
}

// Delaying the source context's timer publication makes the independent merged
// deadline observable first, without relying on the scheduler to choose an order.
type delayedEvaluationDeadline struct {
	context.Context
	deadline time.Time
}

func (ctx delayedEvaluationDeadline) Deadline() (time.Time, bool) {
	return ctx.deadline, true
}

func TestSearchMergedEvaluationContextPreservesDeadlineAndExplicitCancel(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "deadline", true: "explicit-cancel"}[explicit], func(t *testing.T) {
			batch, stopBatch := context.WithTimeout(t.Context(), 2*time.Second)
			defer stopBatch()
			parent, cancel := context.WithCancel(batch)
			defer cancel()
			deadline := time.Now().Add(250 * time.Millisecond)
			record := delayedEvaluationDeadline{Context: parent, deadline: deadline}
			owner := &initialAllocationDeadlineMemory{deadline: deadline, entered: make(chan context.Context, 1)}
			transport := &evaluationContextTransport{}
			client := &http.Client{Transport: transport}
			config := Config{Store: "search", URL: "http://unused.invalid"}
			adapter := &Adapter{ctx: batch, config: config, client: client, dialect: ElasticsearchProduct}
			source := `return function() return weir.keep() end`
			work := memoryProgramWork(source, owner)
			native := work.Backend.(*plan)
			native.action, native.validated = "program", true
			lua := &pb.LuaTransform{Source: []byte(source)}
			form := &pb.Transform_Lua{Lua: lua}
			transform := &pb.Transform{Form: form}
			action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
			mutation := &pb.MutateRequest{Resource: "records/s:item", Action: action}
			operation := &pb.Command_Mutate{Mutate: mutation}
			command := &pb.Command{Operation: operation}
			work.Command, work.Context, work.Bytes = command, record, 1024
			results := make(chan *pb.MutationResult, 1)
			go func() {
				plans := []*execution.Plan{work}
				events := adapter.executeRecords(batch, plans)
				results <- events[0].GetMutationResult()
			}()
			var actualContext context.Context
			select {
			case actual := <-owner.entered:
				actualContext = actual
				actualDeadline, bounded := actual.Deadline()
				if !bounded || !actualDeadline.Equal(deadline) {
					t.Error("evaluation did not inherit record deadline", actualDeadline, deadline)
				}
			case result := <-results:
				t.Fatal("execution did not reach the merged evaluation reservation", result)
			case <-batch.Done():
				t.Fatal("evaluation did not enter allocation wait")
			}
			if explicit {
				cancel()
			}
			select {
			case result := <-results:
				code := pb.FailureCode_DEADLINE_EXCEEDED
				expectedErr := context.DeadlineExceeded
				if explicit {
					code = pb.FailureCode_CANCELLED
					expectedErr = context.Canceled
				}
				// The merged and record contexts have independent deadline timers;
				// one may publish its deadline before the other's Err becomes visible.
				if actualContext.Err() != expectedErr || result.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || result.GetFailure().GetCode() != code || batch.Err() != nil || transport.writes != 0 || owner.waits != 1 {
					t.Fatal("merged context changed failure evidence or sent a write", result, batch.Err(), transport.writes, owner.waits)
				}
			case <-batch.Done():
				t.Fatal("evaluation waited for the batch deadline")
			}
		})
	}
}

type reservationNoSendTransport struct {
	calls int
	err   error
}

func (transport *reservationNoSendTransport) RoundTrip(*http.Request) (*http.Response, error) {
	transport.calls++
	if transport.err != nil {
		return nil, transport.err
	}
	return nil, errors.New("unexpected request after failed reservation")
}

func TestSearchBackendContextErrorsDoNotAcquireReservationProvenance(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		transport := &reservationNoSendTransport{err: cause}
		client := &http.Client{Transport: transport}
		config := Config{Store: "search", URL: "http://unused.invalid"}
		adapter := &Adapter{ctx: t.Context(), config: config, client: client}
		work := memoryProgramWork(`return function() return weir.keep() end`, nil)
		plans := []*execution.Plan{work}
		result := adapter.mgetUnique(t.Context(), plans)[0]
		if result.failure.GetCode() != pb.FailureCode_UNAVAILABLE || transport.calls != 1 || errors.Is(cause, errRecordReservation) {
			t.Fatal("backend context-shaped error became an allocation cancellation", result.failure, transport.calls)
		}
	}
}

func TestSearchPreSendReservationDeadlineRetainsCauseAndNoAttempt(t *testing.T) {
	owner := &initialAllocationDeadlineMemory{}
	memory := &recordMemory{owner: owner, base: 8192, reply: 64 << 10}
	allocation, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	ctx := context.WithValue(allocation, recordMemoryContextKey, memory)
	transport := &reservationNoSendTransport{}
	client := &http.Client{Transport: transport}
	config := Config{Store: "search", URL: "http://unused.invalid"}
	adapter := &Adapter{ctx: t.Context(), config: config, client: client}
	call := exchange{path: "/_bulk", mutation: true, operation: "bulk"}
	_, _, err := adapter.request(ctx, call)
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, errWriteMemoryNotSent) || !errors.Is(err, errRecordReservation) || transport.calls != 0 {
		t.Fatal("pre-send reservation lost deadline or attempted a request", err, transport.calls)
	}
	work := memoryProgramWork(`return function() return weir.keep() end`, owner)
	response := bulkResponse{works: []*execution.Plan{work}, err: err}
	// The request's allocation context can be narrower than the caller doing
	// result normalization. Its wrapped cause must survive even a live context.
	result := adapter.bulkResults(t.Context(), response)[0].GetMutationResult()
	if result.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || result.GetFailure().GetCode() != pb.FailureCode_DEADLINE_EXCEEDED {
		t.Fatal("pre-send deadline became capacity rejection or uncertain effect", result)
	}
}

type postSendReservationBody struct {
	io.Reader
	eof    bool
	closed bool
	cancel context.CancelFunc
}

func (body *postSendReservationBody) Read(data []byte) (int, error) {
	count, err := body.Reader.Read(data)
	body.eof = body.eof || err == io.EOF
	return count, err
}

func (body *postSendReservationBody) Close() error {
	body.closed = true
	if body.cancel != nil {
		body.cancel()
	}
	return nil
}

type postSendReservationTransport struct {
	body  *postSendReservationBody
	bytes int
	calls int
}

func (transport *postSendReservationTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	transport.calls++
	response := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: transport.body, ContentLength: int64(transport.bytes), Request: request}
	return response, nil
}

type postSendReservationMemory struct {
	recordMemoryProbe
	body  *postSendReservationBody
	cause error
	calls int
}

func (memory *postSendReservationMemory) SetWorking(ctx context.Context, bytes int) error {
	if !memory.body.closed {
		return memory.recordMemoryProbe.SetWorking(ctx, bytes)
	}
	memory.calls++
	if bytes <= memory.working {
		return errors.New("post-EOF reservation did not request additional workspace")
	}
	if memory.cause == execution.ErrExecutionMemory {
		return memory.cause
	}
	<-ctx.Done()
	return ctx.Err()
}

func TestSearchPostSendValidationReservationPreservesUnknownAndCause(t *testing.T) {
	for _, test := range []struct {
		name  string
		cause error
		code  pb.FailureCode
	}{
		{name: "capacity", cause: execution.ErrExecutionMemory, code: pb.FailureCode_RESOURCE_EXHAUSTED},
		{name: "cancel", cause: context.Canceled, code: pb.FailureCode_CANCELLED},
		{name: "deadline", cause: context.DeadlineExceeded, code: pb.FailureCode_DEADLINE_EXCEEDED},
	} {
		t.Run(test.name, func(t *testing.T) {
			allocation, cancel := context.WithTimeout(t.Context(), time.Second)
			if test.cause == context.DeadlineExceeded {
				cancel()
				allocation, cancel = context.WithTimeout(t.Context(), 100*time.Millisecond)
			}
			defer cancel()
			raw := `{"errors":false,"took":1,"items":[` + strings.Repeat(`{},`, 255) + `{}]}`
			body := &postSendReservationBody{Reader: strings.NewReader(raw)}
			if test.cause == context.Canceled {
				body.cancel = cancel
			}
			owner := &postSendReservationMemory{body: body, cause: test.cause}
			memory := &recordMemory{owner: owner, base: 8192, reply: 64 << 10}
			ctx := context.WithValue(allocation, recordMemoryContextKey, memory)
			transport := &postSendReservationTransport{body: body, bytes: len(raw)}
			client := &http.Client{Transport: transport}
			config := Config{Store: "search", URL: "http://unused.invalid"}
			adapter := &Adapter{ctx: t.Context(), config: config, client: client}
			call := exchange{path: "/_bulk", mutation: true, operation: "bulk"}
			status, reply, err := adapter.request(ctx, call)
			if status != http.StatusOK || reply != nil || !body.eof || !body.closed || owner.calls != 1 || transport.calls != 1 || !errors.Is(err, errRecordReservation) || !errors.Is(err, test.cause) || errors.Is(err, errWriteMemoryNotSent) {
				t.Fatal("failure did not originate in post-send EOF validation reservation", status, err, body.eof, body.closed, owner.calls, transport.calls)
			}
			work := memoryProgramWork(`return function() return weir.keep() end`, owner)
			response := bulkResponse{works: []*execution.Plan{work}, status: status, err: err}
			result := adapter.bulkResults(t.Context(), response)[0].GetMutationResult()
			if result.GetOutcome() != pb.MutationOutcome_UNKNOWN || result.GetFailure().GetCode() != test.code || transport.calls != 1 {
				t.Fatal("post-send reservation lost its cause, changed effect or replayed", result, transport.calls)
			}
		})
	}
}

func TestSearchBulkReservationMatchingExcludesUnrelatedContextErrors(t *testing.T) {
	for _, test := range []struct {
		cause error
		code  pb.FailureCode
	}{
		{cause: context.Canceled, code: pb.FailureCode_UNAVAILABLE},
		{cause: context.DeadlineExceeded, code: pb.FailureCode_UNAVAILABLE},
		{cause: execution.ErrExecutionMemory, code: pb.FailureCode_RESOURCE_EXHAUSTED},
	} {
		wrapped := errors.Join(errors.New("backend-shaped error"), test.cause)
		work := memoryProgramWork(`return function() return weir.keep() end`, nil)
		response := bulkResponse{works: []*execution.Plan{work}, err: wrapped}
		adapter := &Adapter{}
		result := adapter.bulkResults(t.Context(), response)[0].GetMutationResult()
		if errors.Is(wrapped, errRecordReservation) || result.GetOutcome() != pb.MutationOutcome_UNKNOWN || result.GetFailure().GetCode() != test.code {
			t.Fatal("unrelated backend context error acquired reservation semantics", wrapped, result)
		}
	}
}

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
