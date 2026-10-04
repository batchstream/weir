//go:build integration

package search

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

func TestSearchEveryRecordActionSharesNativeBatch(t *testing.T) {
	base, backend := setupSearch(t)
	for _, id := range []string{"read", "replace", "program", "expression", "noop", "delete"} {
		status, raw := backend.Do(t, "PUT", "/"+backend.Index+"/_doc/"+id, `{"n":1}`)
		if status != 201 {
			t.Fatal(status, string(raw))
		}
	}
	var inspections, reads, writes atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/" + backend.Index:
			inspections.Add(1)
		case "/" + backend.Index + "/_mget":
			reads.Add(1)
		case "/_bulk":
			writes.Add(1)
		}
		request, err := http.NewRequestWithContext(r.Context(), r.Method, backend.URL+r.URL.RequestURI(), r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		request.Header = r.Header.Clone()
		request.GetBody = nil
		response, err := backend.Client.Do(request)
		if err != nil {
			t.Error(err)
			return
		}
		defer response.Body.Close()
		raw, err := io.ReadAll(io.LimitReader(response.Body, batchBodyLimit+1))
		if err != nil {
			t.Error(err)
			return
		}
		w.WriteHeader(response.StatusCode)
		_, _ = w.Write(raw)
	})
	proxy := httptest.NewServer(handler)
	defer proxy.Close()
	cfg := base.config
	cfg.URL = proxy.URL
	a, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	inspections.Store(0)
	program := &pb.ProgramTransform{Runtime: "lua.v1", Source: []byte(`return weir.replace(weir.object("n", weir.add(weir.to64(weir.get(current, "n")), weir.i64("1"))))`)}
	form := &pb.Transform_Program{Program: program}
	transform := &pb.Transform{Form: form}
	action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	mutation := &pb.MutateRequest{Resource: backend.Index + "/s:program", Action: action}
	operation := &execution.Operation{Index: 1, Mutate: mutation}
	programWork, failure := prepareTestRecord(a, operation)
	if failure != nil {
		t.Fatal(failure)
	}
	works := []*execution.Plan{
		searchPlan(t, a, "read", searchResource(backend.Index, "read")), searchPlan(t, a, "read", searchResource(backend.Index, "missing-read")),
		searchPlan(t, a, "replace", searchResource(backend.Index, "replace")), programWork,
		searchPlan(t, a, "put", searchResource(backend.Index, "put")), searchPlan(t, a, "create", searchResource(backend.Index, "create")), searchPlan(t, a, "delete", searchResource(backend.Index, "delete")),
	}
	for _, id := range []string{"expression", "noop", "missing-expression"} {
		body := `{"doc":{"n":2}}`
		if id == "noop" {
			body = `{"doc":{}}`
		}
		op := expressionOperation(backend.Index+"/s:"+id, body)
		work, failure := prepareTestRecord(a, op)
		if failure != nil {
			t.Fatal(failure)
		}
		works = append(works, work)
	}
	for i, work := range works {
		work.Operation.Index = uint64(100 + i)
	}
	results, feedback := a.executeRecords(context.Background(), works)
	if inspections.Load() != 1 || reads.Load() != 1 || writes.Load() != 1 || len(results) != len(works) || feedback != execution.Neutral {
		t.Fatal("aggregate backend call counts", inspections.Load(), reads.Load(), writes.Load(), len(results), feedback)
	}
	if results[0].Read.GetDocument() == nil || results[1].Read.GetMissing() == nil {
		t.Fatal("batched read evidence", results[:2])
	}
	for i, result := range results {
		if result.Index != uint64(100+i) {
			t.Fatal("caller result index", result)
		}
		if i >= 2 {
			if i == len(results)-1 {
				assertOutcome(t, result, pb.MutationOutcome_NOT_APPLIED, pb.FailureCode_PRECONDITION_FAILED)
			} else {
				assertOutcome(t, result, pb.MutationOutcome_APPLIED, 0)
			}
		}
	}
	for _, id := range []string{"program", "expression", "put", "create", "replace", "noop"} {
		status, raw := backend.Do(t, "GET", "/"+backend.Index+"/_doc/"+id, "")
		want := `"n":2`
		if id == "noop" {
			want = `"n":1`
		} else if id == "put" || id == "create" || id == "replace" {
			want = `"n":9223372036854775807`
		}
		if status != 200 || !strings.Contains(string(raw), want) {
			t.Fatal("persisted batch action", id, status, string(raw))
		}
	}
	for _, id := range []string{"delete", "missing-expression"} {
		status, raw := backend.Do(t, "GET", "/"+backend.Index+"/_doc/"+id, "")
		if status != 404 {
			t.Fatal(id, status, string(raw))
		}
	}
	t.Log(fmt.Sprintf("%s ten mixed operations -> one qualification, one realtime _mget, one mixed _bulk; persisted Lua/Replace/update/noop and isolated missing update verified", backend.Product))
}

func TestSearchMixedLuaConflictRereadsOnlyConditionalItem(t *testing.T) {
	base, backend := setupSearch(t)
	backend.Do(t, "PUT", "/"+backend.Index+"/_doc/program", `{"n":1}`)
	var reads, writes atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request, err := http.NewRequestWithContext(r.Context(), r.Method, backend.URL+r.URL.RequestURI(), r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		request.Header = r.Header.Clone()
		request.GetBody = nil
		response, err := backend.Client.Do(request)
		if err != nil {
			t.Error(err)
			return
		}
		defer response.Body.Close()
		raw, err := io.ReadAll(io.LimitReader(response.Body, batchBodyLimit+1))
		if err != nil {
			t.Error(err)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/_mget") && reads.Add(1) == 1 {
			backend.Do(t, "PUT", "/"+backend.Index+"/_doc/program", `{"n":10}`)
		}
		if r.URL.Path == "/_bulk" {
			writes.Add(1)
		}
		w.WriteHeader(response.StatusCode)
		_, _ = w.Write(raw)
	})
	proxy := httptest.NewServer(handler)
	defer proxy.Close()
	cfg := base.config
	cfg.URL = proxy.URL
	a, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	program := &pb.ProgramTransform{Runtime: "lua.v1", Source: []byte(`return weir.replace(weir.object("n", weir.add(weir.to64(weir.get(current, "n")), weir.i64("1"))))`)}
	form := &pb.Transform_Program{Program: program}
	transform := &pb.Transform{Form: form}
	action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	mutation := &pb.MutateRequest{Resource: backend.Index + "/s:program", Action: action}
	operation := &execution.Operation{Index: 1, Mutate: mutation}
	programWork, failure := prepareTestRecord(a, operation)
	if failure != nil {
		t.Fatal(failure)
	}
	put := searchPlan(t, a, "put", searchResource(backend.Index, "put"))
	works := []*execution.Plan{programWork, put}
	results, feedback := a.executeRecords(context.Background(), works)
	if len(results) != 2 || reads.Load() != 2 || writes.Load() != 2 || feedback != execution.Neutral {
		t.Fatal("conditional subset retry counts", len(results), reads.Load(), writes.Load(), feedback)
	}
	for _, result := range results {
		assertOutcome(t, result, pb.MutationOutcome_APPLIED, 0)
	}
	status, raw := backend.Do(t, "GET", "/"+backend.Index+"/_doc/program", "")
	if status != 200 || !strings.Contains(string(raw), `"n":11`) {
		t.Fatal("Lua did not reevaluate the concurrently changed source", status, string(raw))
	}
	status, raw = backend.Do(t, "GET", "/"+backend.Index+"/_doc/put", "")
	if status != 200 || !strings.Contains(string(raw), `"_version":1`) {
		t.Fatal("independent acknowledged write replayed", status, string(raw))
	}
	t.Logf("%s Lua OCC conflict -> only Lua reread and rewritten, peer Put remained version 1", backend.Product)
}
