package search

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

const visibleRefreshReply = `{"_shards":{"total":1,"successful":1,"failed":0}}`

type visibilityFixture struct {
	mu            sync.Mutex
	refreshModes  []string
	refreshes     int
	refreshStatus int
	refreshReply  string
	bulkStall     bool
	refreshStall  bool
}

func (f *visibilityFixture) serve(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/records":
		_, _ = io.WriteString(w, testIndexReply)
	case "/records/_mget":
		var request struct{ IDs []string }
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		docs := make([]string, len(request.IDs))
		for i, id := range request.IDs {
			if id == "lua_missing" {
				docs[i] = fmt.Sprintf(`{"_index":"records","_id":%q,"found":false}`, id)
			} else {
				docs[i] = fmt.Sprintf(`{"_index":"records","_id":%q,"found":true,"_seq_no":1,"_primary_term":1,"_source":{"n":1}}`, id)
			}
		}
		_, _ = fmt.Fprintf(w, `{"docs":[%s]}`, strings.Join(docs, ","))
	case "/_bulk":
		f.mu.Lock()
		f.refreshModes = append(f.refreshModes, r.URL.Query().Get("refresh"))
		f.mu.Unlock()
		if f.bulkStall {
			_, _ = io.ReadAll(r.Body)
			<-r.Context().Done()
			return
		}
		raw, _ := io.ReadAll(r.Body)
		lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
		items := make([]string, 0)
		for i := 0; i < len(lines); i++ {
			var metadata map[string]struct {
				ID string `json:"_id"`
			}
			if json.Unmarshal([]byte(lines[i]), &metadata) != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			for action, record := range metadata {
				result, status := "updated", 200
				if action == "create" {
					result, status = "created", 201
				}
				if action == "delete" {
					result = "deleted"
					if record.ID == "delete_missing" {
						result, status = "not_found", 404
					}
				} else {
					i++
				}
				if action == "update" && record.ID == "expression_noop" {
					result = "noop"
				}
				items = append(items, fmt.Sprintf(`{%q:{"_index":"records","_id":%q,"status":%d,"result":%q,"_version":2,"_seq_no":2,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}}`, action, record.ID, status, result))
			}
		}
		_, _ = fmt.Fprintf(w, `{"errors":false,"took":1,"items":[%s]}`, strings.Join(items, ","))
	case "/records/_refresh":
		f.mu.Lock()
		f.refreshes++
		f.mu.Unlock()
		if f.refreshStall {
			<-r.Context().Done()
			return
		}
		if f.refreshStatus != 0 {
			w.WriteHeader(f.refreshStatus)
		}
		reply := f.refreshReply
		if reply == "" {
			reply = visibleRefreshReply
		}
		_, _ = io.WriteString(w, reply)
	default:
		http.NotFound(w, r)
	}
}

func visibilityAdapter(t *testing.T, fixture *visibilityFixture) *Adapter {
	t.Helper()
	handler := http.HandlerFunc(fixture.serve)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	cfg := Config{Store: "search", URL: server.URL}
	a := &Adapter{dialect: ElasticsearchProduct, config: cfg, client: server.Client(), ctx: context.Background()}
	return a
}

type visibilityPlanOptions struct {
	action, id, resource, program string
	wait                          bool
}

func visibilityPlan(t *testing.T, adapter *Adapter, opts visibilityPlanOptions) *execution.Plan {
	t.Helper()
	resource := opts.resource
	if resource == "" {
		resource = "records/s:" + opts.id
	}
	work := batchTestPlan(t, adapter, opts.action, resource)
	if opts.wait {
		options := &pb.MutationOptions{Visibility: pb.MutationVisibility_SEARCH_VISIBLE}
		work.Command.GetMutate().Options = options
	}
	if opts.program != "" {
		work.Command.GetMutate().GetAtomicTransform().GetLua().Source = []byte(opts.program)
	}
	request := &pb.ExecuteRequest{Index: work.ID, Command: work.Command}
	prepared, failure := prepareTestRecord(adapter, request)
	if failure != nil {
		t.Fatal(failure)
	}
	return prepared
}

func TestMutationVisibilityFlagsGroupWritesAndKeepDefaultBehavior(t *testing.T) {
	fixture := &visibilityFixture{}
	adapter := visibilityAdapter(t, fixture)
	options := []visibilityPlanOptions{
		{action: "put", id: "default"},
		{action: "put", id: "put", wait: true},
		{action: "create", id: "create", wait: true},
		{action: "replace", id: "replace", wait: true},
		{action: "delete", id: "delete", wait: true},
		{action: "expression", id: "expression", wait: true},
		{action: "program", id: "program", wait: true},
		{action: "put", id: "default_after"},
	}
	works := make([]*execution.Plan, len(options))
	for i, opts := range options {
		works[i] = visibilityPlan(t, adapter, opts)
	}
	if works[0].BatchKey == works[1].BatchKey || works[1].BatchKey != works[6].BatchKey {
		t.Fatal("scheduler must separate visibility waits and batch every opted-in mutation")
	}
	results := adapter.executeRecords(t.Context(), works)
	for _, result := range results {
		mutation := result.GetMutationResult()
		if mutation.GetOutcome() != pb.MutationOutcome_APPLIED || mutation.GetFailure() != nil {
			t.Fatal("visibility acknowledgement", mutation)
		}
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if strings.Join(fixture.refreshModes, ",") != "false,wait_for,false" || fixture.refreshes != 0 {
		t.Fatal("default writes gained refresh waits or changing writes gained needless refresh barriers", fixture.refreshModes, fixture.refreshes)
	}
}

func TestMutationOptionsDefaultVisibilityKeepsApplicationAcknowledgement(t *testing.T) {
	empty := &pb.MutationOptions{}
	explicit := &pb.MutationOptions{Visibility: pb.MutationVisibility_APPLICATION_ACKNOWLEDGED}
	cases := []struct {
		name    string
		options *pb.MutationOptions
	}{
		{name: "omitted"},
		{name: "empty", options: empty},
		{name: "explicit", options: explicit},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			fixture := &visibilityFixture{}
			adapter := visibilityAdapter(t, fixture)
			work := batchTestPlan(t, adapter, "put", "records/s:"+test.name)
			work.Command.GetMutate().Options = test.options
			request := &pb.ExecuteRequest{Index: work.ID, Command: work.Command}
			prepared, failure := prepareTestRecord(adapter, request)
			if failure != nil {
				t.Fatal(failure)
			}
			works := []*execution.Plan{prepared}
			mutation := adapter.executeRecords(t.Context(), works)[0].GetMutationResult()
			if mutation.GetOutcome() != pb.MutationOutcome_APPLIED || mutation.GetFailure() != nil || prepared.BatchKey != "records" {
				t.Fatal("default visibility changed application acknowledgement", mutation, prepared.BatchKey)
			}
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			if strings.Join(fixture.refreshModes, ",") != "false" || fixture.refreshes != 0 {
				t.Fatal("default mutation options introduced search refresh", fixture.refreshModes, fixture.refreshes)
			}
		})
	}
}

func TestNoopVisibilityUsesOneConfirmedBarrierPerIndex(t *testing.T) {
	for _, wait := range []bool{false, true} {
		t.Run(fmt.Sprint(wait), func(t *testing.T) {
			fixture := &visibilityFixture{}
			adapter := visibilityAdapter(t, fixture)
			options := []visibilityPlanOptions{
				{action: "program", id: "lua_keep", wait: wait, program: `return function(current, incoming) return weir.keep() end`},
				{action: "program", id: "lua_missing", wait: wait, program: `return function(current, incoming) return weir.delete() end`},
				{action: "delete", id: "delete_missing", wait: wait},
				{action: "expression", id: "expression_noop", wait: wait},
			}
			works := make([]*execution.Plan, len(options))
			for i, opts := range options {
				works[i] = visibilityPlan(t, adapter, opts)
			}
			for _, result := range adapter.executeRecords(t.Context(), works) {
				mutation := result.GetMutationResult()
				if mutation.GetOutcome() != pb.MutationOutcome_APPLIED || mutation.GetFailure() != nil {
					t.Fatal("noop visibility acknowledgement", mutation)
				}
			}
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			want := 0
			if wait {
				want = 1
			}
			if fixture.refreshes != want || len(fixture.refreshModes) != 1 {
				t.Fatal("noop barriers modified business state or were not grouped", fixture.refreshes, fixture.refreshModes)
			}
		})
	}
}

func TestVisibilityBarrierFailurePreservesAppliedOutcome(t *testing.T) {
	cases := []struct {
		name   string
		status int
		reply  string
		stall  bool
		code   pb.FailureCode
	}{
		{name: "missing_shards", reply: `{}`, code: pb.FailureCode_UNAVAILABLE},
		{name: "missing_count", reply: `{"_shards":{"total":1,"successful":1}}`, code: pb.FailureCode_UNAVAILABLE},
		{name: "partial_refresh", reply: `{"_shards":{"total":2,"successful":1,"failed":1}}`, code: pb.FailureCode_UNAVAILABLE},
		{name: "empty_refresh", reply: `{"_shards":{"total":0,"successful":0,"failed":0}}`, code: pb.FailureCode_UNAVAILABLE},
		{name: "permission", status: 403, reply: `{"error":{"type":"security_exception"},"status":403}`, code: pb.FailureCode_PERMISSION_DENIED},
		{name: "deadline", stall: true, code: pb.FailureCode_DEADLINE_EXCEEDED},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			fixture := &visibilityFixture{refreshStatus: test.status, refreshReply: test.reply, refreshStall: test.stall}
			adapter := visibilityAdapter(t, fixture)
			options := visibilityPlanOptions{action: "program", id: "lua_keep", wait: true, program: `return function(current, incoming) return weir.keep() end`}
			work := visibilityPlan(t, adapter, options)
			ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
			defer cancel()
			works := []*execution.Plan{work}
			mutation := adapter.executeRecords(ctx, works)[0].GetMutationResult()
			if mutation.GetOutcome() != pb.MutationOutcome_APPLIED || mutation.GetFailure().GetCode() != test.code {
				t.Fatal("refresh failure lost prior applied evidence", mutation)
			}
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			if fixture.refreshes != 1 || len(fixture.refreshModes) != 0 {
				t.Fatal("refresh failure replayed the noop operation", fixture.refreshes, fixture.refreshModes)
			}
		})
	}
}

func TestVisibilityWriteDeadlineIsUnknownAndNeverReplayed(t *testing.T) {
	fixture := &visibilityFixture{bulkStall: true}
	adapter := visibilityAdapter(t, fixture)
	options := visibilityPlanOptions{action: "program", id: "lua_write", wait: true}
	work := visibilityPlan(t, adapter, options)
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()
	works := []*execution.Plan{work}
	mutation := adapter.executeRecords(ctx, works)[0].GetMutationResult()
	if mutation.GetOutcome() != pb.MutationOutcome_UNKNOWN || mutation.GetFailure().GetCode() != pb.FailureCode_DEADLINE_EXCEEDED {
		t.Fatal("unacknowledged visible write lost uncertainty", mutation)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if strings.Join(fixture.refreshModes, ",") != "wait_for" || fixture.refreshes != 0 {
		t.Fatal("possibly-applied visible write was replayed", fixture.refreshModes, fixture.refreshes)
	}
}
