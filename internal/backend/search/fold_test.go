package search

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

type storedSource struct {
	source string
	seq    int64
}

// memorySearch serves one index from memory with real-time reads and the
// conditional bulk writes Weir sends.
type memorySearch struct {
	mu        sync.Mutex
	docs      map[string]storedSource
	seq       int64
	reads     int
	bulks     []string
	refreshes []string
	barriers  int
	// changeBeforeWrite rewrites a stored record before the next bulk applies,
	// as a concurrent writer would.
	changeBeforeWrite []string
	reject            func(source string) bool
}

func (m *memorySearch) handle(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch r.URL.Path {
	case "/records":
		_, _ = io.WriteString(w, testIndexReply)
	case "/records/_mget":
		m.reads++
		var request struct{ IDs []string }
		_ = json.NewDecoder(r.Body).Decode(&request)
		docs := make([]string, len(request.IDs))
		for i, id := range request.IDs {
			doc, found := m.docs[id]
			if !found {
				docs[i] = fmt.Sprintf(`{"_index":"records","_id":%q,"found":false}`, id)
				continue
			}
			docs[i] = fmt.Sprintf(`{"_index":"records","_id":%q,"found":true,"_seq_no":%d,"_primary_term":1,"_source":%s}`, id, doc.seq, doc.source)
		}
		_, _ = io.WriteString(w, `{"docs":[`+strings.Join(docs, ",")+`]}`)
	case "/records/_refresh":
		m.barriers++
		_, _ = io.WriteString(w, `{"_shards":{"total":1,"successful":1,"failed":0}}`)
	case "/_bulk":
		raw, _ := io.ReadAll(r.Body)
		m.bulks = append(m.bulks, string(raw))
		m.refreshes = append(m.refreshes, r.URL.Query().Get("refresh"))
		for _, id := range m.changeBeforeWrite {
			m.seq++
			m.docs[id] = storedSource{source: `{"n":100}`, seq: m.seq}
		}
		m.changeBeforeWrite = nil
		items, failed := m.apply(string(raw))
		_, _ = fmt.Fprintf(w, `{"errors":%t,"took":1,"items":[%s]}`, failed, strings.Join(items, ","))
	default:
		http.Error(w, "unexpected path", http.StatusNotFound)
	}
}

func (m *memorySearch) apply(body string) ([]string, bool) {
	var items []string
	failed := false
	scanner := bufio.NewScanner(strings.NewReader(body))
	for scanner.Scan() {
		var line map[string]struct {
			ID   string `json:"_id"`
			Seq  *int64 `json:"if_seq_no"`
			Term *int64 `json:"if_primary_term"`
		}
		_ = json.Unmarshal(scanner.Bytes(), &line)
		for op, meta := range line {
			source := ""
			if op != "delete" && scanner.Scan() {
				source = scanner.Text()
			}
			doc, found := m.docs[meta.ID]
			reply := func(status int, fields string) string {
				return fmt.Sprintf(`{%q:{"_index":"records","_id":%q,"status":%d,%s}}`, op, meta.ID, status, fields)
			}
			switch {
			case meta.Seq != nil && (!found || doc.seq != *meta.Seq), op == "create" && found:
				items, failed = append(items, reply(409, `"error":{"type":"version_conflict_engine_exception"}`)), true
				continue
			case m.reject != nil && op != "delete" && m.reject(source):
				items, failed = append(items, reply(400, `"error":{"type":"mapper_parsing_exception"}`)), true
				continue
			}
			m.seq++
			status, result := 200, "updated"
			switch {
			case op == "delete" && found:
				result = "deleted"
				delete(m.docs, meta.ID)
			case op == "delete":
				status, result = 404, "not_found"
			case !found:
				status, result = 201, "created"
				m.docs[meta.ID] = storedSource{source: source, seq: m.seq}
			default:
				m.docs[meta.ID] = storedSource{source: source, seq: m.seq}
			}
			fields := fmt.Sprintf(`"result":%q,"_version":2,"_seq_no":%d,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}`, result, m.seq)
			items = append(items, reply(status, fields))
		}
	}
	return items, failed
}

func (m *memorySearch) source(id string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.docs[id].source
}

func newMemorySearch(t *testing.T, docs map[string]string) (*memorySearch, *Adapter) {
	t.Helper()
	memory := &memorySearch{docs: make(map[string]storedSource)}
	for id, source := range docs {
		memory.seq++
		memory.docs[id] = storedSource{source: source, seq: memory.seq}
	}
	adapter := testHTTPAdapter(t, memory.handle)
	return memory, adapter
}

const addProgram = `return function(current, incoming)
  local n = 0
  if current ~= nil then n = current.n end
  return {n = n + incoming.by}
end`

// recordMutations prepares mutations of one resource.
type recordMutations struct {
	t        *testing.T
	adapter  *Adapter
	resource string
	visible  bool
}

// prepare prepares one mutation. data is the document of a put, create or
// replace, or the input of a Lua program.
func (r recordMutations) prepare(action, data string) *execution.Plan {
	r.t.Helper()
	request := &pb.MutateRequest{Resource: r.resource}
	document := &pb.Document{ContentType: "application/json", Data: []byte(data)}
	switch action {
	case "put":
		request.Action = &pb.MutateRequest_Put{Put: document}
	case "create":
		request.Action = &pb.MutateRequest_Create{Create: document}
	case "replace":
		request.Action = &pb.MutateRequest_Replace{Replace: document}
	case "delete":
		empty := &pb.Empty{}
		request.Action = &pb.MutateRequest_Delete{Delete: empty}
	case "add", "keep":
		source := addProgram
		if action == "keep" {
			source = `return function(current, incoming) return weir.keep() end`
		}
		lua := &pb.LuaTransform{Source: []byte(source), Input: document}
		form := &pb.Transform_Lua{Lua: lua}
		transform := &pb.Transform{Form: form}
		request.Action = &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	default:
		r.t.Fatal(action)
	}
	if r.visible {
		request.Options = &pb.MutationOptions{Visibility: pb.MutationVisibility_QUERY_VISIBLE}
	}
	variant := &pb.Command_Mutate{Mutate: request}
	command := &pb.Command{Operation: variant}
	operation := &pb.ExecuteRequest{Index: 1, Command: command}
	work, failure := prepareTestRecord(r.adapter, operation)
	if failure != nil {
		r.t.Fatal(failure)
	}
	return work
}

type expected struct {
	outcome pb.MutationOutcome
	code    pb.FailureCode
}

var appliedResult = expected{outcome: pb.MutationOutcome_APPLIED}

func checkResults(t *testing.T, results []*pb.Event, want []expected) {
	t.Helper()
	if len(results) != len(want) {
		t.Fatal("results", len(results), "want", len(want))
	}
	for i, result := range results {
		mutation := result.GetMutationResult()
		if mutation.GetOutcome() != want[i].outcome || mutation.GetFailure().GetCode() != want[i].code {
			t.Fatalf("result %d: %v, want %v", i, mutation, want[i])
		}
	}
}

func TestOneRecordsMutationsFoldIntoOneReadAndOneWrite(t *testing.T) {
	memory, adapter := newMemorySearch(t, map[string]string{"seller": `{"n":1}`})
	seller := recordMutations{t: t, adapter: adapter, resource: "records/s:seller", visible: true}
	other := recordMutations{t: t, adapter: adapter, resource: "records/s:other", visible: true}
	works := []*execution.Plan{
		seller.prepare("add", `{"by":1}`),
		other.prepare("put", `{"n":7}`),
		seller.prepare("add", `{"by":10}`),
		seller.prepare("add", `{"by":100}`),
	}
	results := adapter.ExecuteRecords(t.Context(), works)
	want := []expected{appliedResult, appliedResult, appliedResult, appliedResult}
	checkResults(t, results, want)
	if memory.reads != 1 || len(memory.bulks) != 1 || memory.refreshes[0] != "wait_for" {
		t.Fatal("reads", memory.reads, "bulks", len(memory.bulks), memory.refreshes)
	}
	if lines := strings.Count(memory.bulks[0], "\n"); lines != 4 {
		t.Fatal("one write per record expected", memory.bulks[0])
	}
	if !strings.Contains(memory.bulks[0], `"if_seq_no":1`) {
		t.Fatal("the folded write was not conditional on its read", memory.bulks[0])
	}
	if got := memory.source("seller"); got != `{"n":112}` {
		t.Fatal("folded transforms did not apply in order", got)
	}
}

func TestFoldedActionsApplyInQueueOrder(t *testing.T) {
	memory, adapter := newMemorySearch(t, map[string]string{"x": `{"n":1}`})
	x := recordMutations{t: t, adapter: adapter, resource: "records/s:x"}
	works := []*execution.Plan{
		x.prepare("put", `{"a":1}`),
		x.prepare("create", `{"b":1}`),
		x.prepare("replace", `{"n":5}`),
		x.prepare("add", `{"by":1}`),
		x.prepare("delete", ``),
		x.prepare("replace", `{"d":1}`),
		x.prepare("create", `{"n":40}`),
		x.prepare("add", `{"by":2}`),
	}
	results := adapter.ExecuteRecords(t.Context(), works)
	precondition := expected{outcome: pb.MutationOutcome_NOT_APPLIED, code: pb.FailureCode_PRECONDITION_FAILED}
	want := []expected{appliedResult, precondition, appliedResult, appliedResult, appliedResult, precondition, appliedResult, appliedResult}
	checkResults(t, results, want)
	// A put that replaces whatever the record holds needs no read.
	if memory.reads != 0 || len(memory.bulks) != 1 || strings.Contains(memory.bulks[0], "if_seq_no") {
		t.Fatal("reads", memory.reads, memory.bulks)
	}
	if got := memory.source("x"); got != `{"n":42}` {
		t.Fatal("folded actions", got)
	}
}

func TestAFoldedWriteThatConflictsRunsAgainFromAFreshRead(t *testing.T) {
	memory, adapter := newMemorySearch(t, map[string]string{"x": `{"n":1}`})
	memory.changeBeforeWrite = []string{"x"}
	x := recordMutations{t: t, adapter: adapter, resource: "records/s:x"}
	works := []*execution.Plan{
		x.prepare("add", `{"by":1}`),
		x.prepare("add", `{"by":2}`),
	}
	results := adapter.ExecuteRecords(t.Context(), works)
	want := []expected{appliedResult, appliedResult}
	checkResults(t, results, want)
	if memory.reads != 2 || len(memory.bulks) != 2 {
		t.Fatal("reads", memory.reads, "bulks", len(memory.bulks))
	}
	if got := memory.source("x"); got != `{"n":103}` {
		t.Fatal("the chain did not run again on the concurrent change", got)
	}
}

func TestARejectedFoldedWriteRunsEachMutationAlone(t *testing.T) {
	memory, adapter := newMemorySearch(t, nil)
	memory.reject = func(source string) bool { return strings.Contains(source, "bad") }
	x := recordMutations{t: t, adapter: adapter, resource: "records/s:x"}
	works := []*execution.Plan{
		x.prepare("put", `{"n":1}`),
		x.prepare("put", `{"bad":1}`),
		x.prepare("add", `{"by":1}`),
	}
	results := adapter.ExecuteRecords(t.Context(), works)
	rejected := expected{outcome: pb.MutationOutcome_NOT_APPLIED, code: pb.FailureCode_PRECONDITION_FAILED}
	// Alone, the transform reads the rejected put's predecessor, whose n it
	// increments; folded, it would have failed with the rejected document.
	want := []expected{appliedResult, rejected, appliedResult}
	checkResults(t, results, want)
	if got := memory.source("x"); got != `{"n":2}` {
		t.Fatal("mutations did not run alone after the rejection", got)
	}
	if len(memory.bulks) != 4 {
		t.Fatal("one folded write and three single writes expected", len(memory.bulks))
	}
}

func TestAFoldedChainWithoutChangeWritesNothing(t *testing.T) {
	memory, adapter := newMemorySearch(t, map[string]string{"x": `{"n":1}`})
	x := recordMutations{t: t, adapter: adapter, resource: "records/s:x", visible: true}
	works := []*execution.Plan{
		x.prepare("keep", `{}`),
		x.prepare("keep", `{}`),
	}
	results := adapter.ExecuteRecords(t.Context(), works)
	want := []expected{appliedResult, appliedResult}
	checkResults(t, results, want)
	if len(memory.bulks) != 0 || memory.barriers != 1 {
		t.Fatal("bulks", len(memory.bulks), "refresh barriers", memory.barriers)
	}
}

func TestAChainMemberWhoseCallerLeftDoesNotStart(t *testing.T) {
	memory, adapter := newMemorySearch(t, map[string]string{"x": `{"n":1}`})
	x := recordMutations{t: t, adapter: adapter, resource: "records/s:x"}
	works := []*execution.Plan{
		x.prepare("add", `{"by":1}`),
		x.prepare("add", `{"by":10}`),
		x.prepare("add", `{"by":100}`),
	}
	ended, cancel := context.WithCancel(t.Context())
	cancel()
	works[0].Context = ended
	results := adapter.ExecuteRecords(t.Context(), works)
	canceled := expected{outcome: pb.MutationOutcome_NOT_STARTED, code: pb.FailureCode_CANCELLED}
	want := []expected{canceled, appliedResult, appliedResult}
	checkResults(t, results, want)
	if got := memory.source("x"); got != `{"n":111}` {
		t.Fatal("the remaining chain did not apply", got)
	}
}

func TestBackendExpressionsDoNotFold(t *testing.T) {
	adapter := &Adapter{dialect: ElasticsearchProduct}
	expression := batchTestPlan(t, adapter, "expression", "records/s:x")
	if expression.Folds {
		t.Fatal("a backend expression cannot be applied to a known source")
	}
	for _, action := range []string{"put", "create", "replace", "delete", "program"} {
		if work := batchTestPlan(t, adapter, action, "records/s:x"); !work.Folds {
			t.Fatal(action, "does not fold")
		}
	}
	if read := batchTestPlan(t, adapter, "read", "records/s:x"); read.Folds || !slices.Contains([]string{"records"}, read.BatchKey) {
		t.Fatal("a read folds")
	}
}
