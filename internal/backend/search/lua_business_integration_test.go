//go:build integration

package search

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

type searchBusinessProgramOptions struct {
	resource, source, input string
	observedAt              time.Time
}

func prepareSearchBusinessProgram(t *testing.T, adapter *Adapter, opts searchBusinessProgramOptions) *execution.Plan {
	t.Helper()
	input := &pb.Document{ContentType: "application/json", Data: []byte(opts.input)}
	program := &pb.LuaTransform{Source: []byte(opts.source), Input: input}
	form := &pb.Transform_Lua{Lua: program}
	transform := &pb.Transform{Form: form}
	action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	mutation := &pb.MutateRequest{Resource: opts.resource, Action: action}
	operation := &pb.Command_Mutate{Mutate: mutation}
	command := &pb.Command{Operation: operation}
	request := &pb.ExecuteRequest{Index: 1, Command: command}
	work, failure := prepareTestRecord(adapter, request)
	if failure != nil {
		t.Fatal(failure)
	}
	work.Backend.(*plan).program.ObservedAt = opts.observedAt
	return work
}

func TestSearchLuaProductFunctionCreatesAndPreservesJSON(t *testing.T) {
	source, err := os.ReadFile("../../../examples/lua/product.lua")
	if err != nil {
		t.Fatal(err)
	}
	adapter, backend := setupSearch(t)
	current := `{"title":"old","tags":["a","b"],"big":9007199254740993,"decimal":1.2500,"exponent":1e+3,"negative_zero":-0,"null":null,"empty_array":[],"empty_object":{}}`
	status, raw := backend.Do(t, "PUT", "/"+backend.Index+"/_doc/existing", current)
	if status != http.StatusCreated {
		t.Fatal("product fixture write", status, string(raw))
	}
	var before map[string]json.RawMessage
	if err := json.Unmarshal([]byte(current), &before); err != nil {
		t.Fatal(err)
	}
	observedAt := time.Date(2026, time.October, 7, 1, 2, 3, 456789000, time.UTC)
	cases := []struct {
		id, title, input, tags string
	}{
		{id: "new", title: `"new"`, input: `{"title":"new","tags":["a","a","b"]}`, tags: `["a","b"]`},
		{id: "existing", title: `"old"`, input: `{"title":"","tags":["b","c"]}`, tags: `["a","b","c"]`},
	}
	for _, testCase := range cases {
		t.Run(testCase.id, func(t *testing.T) {
			opts := searchBusinessProgramOptions{resource: searchResource(backend.Index, testCase.id), source: string(source), input: testCase.input, observedAt: observedAt}
			work := prepareSearchBusinessProgram(t, adapter, opts)
			result := runSearch(t, adapter, work)
			assertOutcome(t, result, pb.MutationOutcome_APPLIED, 0)
			status, raw := backend.Do(t, "GET", "/"+backend.Index+"/_doc/"+testCase.id, "")
			var stored struct {
				ID     string                     `json:"_id"`
				Source map[string]json.RawMessage `json:"_source"`
			}
			if status != http.StatusOK || json.Unmarshal(raw, &stored) != nil || stored.ID != testCase.id || string(stored.Source["title"]) != testCase.title || string(stored.Source["tags"]) != testCase.tags || string(stored.Source["updated_at"]) != `"`+observedAt.Format(time.RFC3339Nano)+`"` {
				t.Fatal("product title/tags/fixed time changed", status, string(raw))
			}
			if testCase.id == "existing" {
				for field, want := range before {
					if field != "title" && field != "tags" && string(stored.Source[field]) != string(want) {
						t.Errorf("product merge changed untouched JSON %s: got=%s want=%s", field, stored.Source[field], want)
					}
				}
			}
			for _, field := range []string{"_id", "_index", "_routing", "_version", "_seq_no", "_primary_term"} {
				if _, exists := stored.Source[field]; exists {
					t.Errorf("product merge leaked backend identity/version %s", field)
				}
			}
		})
	}
}

func TestSearchLuaComplexProductKeepsBusinessRules(t *testing.T) {
	source, err := os.ReadFile("../../luaengine/testdata/product_merge.lua")
	if err != nil {
		t.Fatal(err)
	}
	adapter, backend := setupSearch(t)
	comments := make([]string, 0, 23)
	for i := 0; i < 23; i++ {
		comments = append(comments, fmt.Sprintf(`{"id":%d}`, i))
	}
	offers := make([]string, 0, 51)
	for i := 0; i < 51; i++ {
		offers = append(offers, fmt.Sprintf(`{"uid":"new%02d"}`, i))
	}
	cases := []struct {
		name, current, incoming string
	}{
		{name: "content", current: `{"title":"old","brand":"OLD","uid":"old","uids":["legacy"],"gallery":["old"],"allowed_countries":["US"],"first_found_at":"first","comment_count":4,"stocks":[{"stock":3,"variables":{"b":[1,2],"a":{"x":true}}}],"comments":[{"b":[{"z":0},{"z":1}],"a":{"x":"y"}}],"solds":[{"sold":1,"period_hours":24,"record_at":"2026-10-07T01:00:00Z"}],"offers":[{"uid":"same","amount":1},{"uid":"old","amount":0}]}`, incoming: `{"title":"","brand":"new brand","uid":"new","uids":["legacy","","new"],"gallery":[],"allowed_countries":["US","JP"],"first_found_at":"later","last_found_at":"last","comment_count":0,"rating":0,"available":true,"stocks":[{"variables":{"a":{"x":true},"b":[1,2]},"stock":3}],"comments":[{"a":{"x":"y"},"b":[{"z":0},{"z":1}]}],"solds":[{"sold":1,"period_hours":24,"record_at":"2026-10-07T20:00:00Z"}],"offers":[{"uid":"same","amount":2},{"uid":"new","amount":3}]}`},
		{name: "history20", current: `{"comments":[` + strings.Join(comments, ",") + `]}`, incoming: `{"comments":[{"id":22},{"id":23}]}`},
		{name: "offers50", current: `{"offers":[{"uid":"old"}]}`, incoming: `{"offers":[` + strings.Join(offers, ",") + `]}`},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			status, raw := backend.Do(t, "PUT", "/"+backend.Index+"/_doc/"+testCase.name, testCase.current)
			if status != http.StatusCreated {
				t.Fatal("product fixture write", status, string(raw))
			}
			opts := searchBusinessProgramOptions{resource: searchResource(backend.Index, testCase.name), source: string(source), input: testCase.incoming}
			work := prepareSearchBusinessProgram(t, adapter, opts)
			result := runSearch(t, adapter, work)
			assertOutcome(t, result, pb.MutationOutcome_APPLIED, 0)
			status, raw = backend.Do(t, "GET", "/"+backend.Index+"/_doc/"+testCase.name, "")
			var stored struct {
				ID     string                     `json:"_id"`
				Source map[string]json.RawMessage `json:"_source"`
			}
			if status != http.StatusOK || json.Unmarshal(raw, &stored) != nil || stored.ID != testCase.name {
				t.Fatal("product identity/readback changed", status, string(raw))
			}
			switch testCase.name {
			case "content":
				var brand string
				if json.Unmarshal(stored.Source["brand"], &brand) != nil || brand != "NEW BRAND" {
					t.Fatal("brand uppercase changed", string(raw))
				}
				for field, want := range map[string]string{"title": `"old"`, "uid": `"new"`, "first_found_at": `"first"`, "last_found_at": `"last"`, "comment_count": "4", "rating": "0", "available": "true"} {
					if string(stored.Source[field]) != want {
						t.Errorf("scalar product rule %s changed: got=%s want=%s", field, stored.Source[field], want)
					}
				}
				for field, want := range map[string]int{"uids": 3, "gallery": 1, "allowed_countries": 2, "stocks": 1, "comments": 1, "solds": 1, "offers": 3} {
					var items []json.RawMessage
					if json.Unmarshal(stored.Source[field], &items) != nil || len(items) != want {
						t.Errorf("%s deduplication/empty-array policy changed: %s", field, stored.Source[field])
					}
				}
				var offers []struct{ Amount int }
				if json.Unmarshal(stored.Source["offers"], &offers) != nil || len(offers) != 3 || offers[0].Amount != 2 {
					t.Fatal("incoming offer did not win duplicate uid", string(raw))
				}
			case "history20":
				var items []struct{ ID int }
				if json.Unmarshal(stored.Source["comments"], &items) != nil || len(items) != 20 || items[0].ID != 4 || items[19].ID != 23 {
					t.Fatal("history did not deduplicate and retain last 20", string(raw))
				}
			case "offers50":
				var items []struct{ UID string }
				if json.Unmarshal(stored.Source["offers"], &items) != nil || len(items) != 50 || items[0].UID != "new02" || items[49].UID != "old" {
					t.Fatal("original incoming-first/keep-tail offer policy changed", string(raw))
				}
			}
		})
	}
}
