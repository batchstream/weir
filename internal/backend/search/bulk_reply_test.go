package search

import (
	"context"
	"fmt"
	"strings"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

// bulkReplyFixture is a 200 _bulk reply that indexes count documents.
func bulkReplyFixture(count int) bulkResponse {
	works := make([]*execution.Plan, count)
	items := make([]string, count)
	for i := range count {
		mutation := &pb.MutateRequest{Resource: fmt.Sprintf("records/s:%d", i)}
		operation := &pb.Command_Mutate{Mutate: mutation}
		command := &pb.Command{Operation: operation}
		native := &plan{index: "records", id: fmt.Sprint(i), action: "index"}
		works[i] = &execution.Plan{Command: command, Backend: native}
		items[i] = fmt.Sprintf(`{"index":{"_index":"records","_id":"%d","_version":3,"result":"updated","forced_refresh":false,"_shards":{"total":2,"successful":1,"failed":0},"_seq_no":%d,"_primary_term":1,"status":200}}`, i, 100+i)
	}
	raw := []byte(`{"errors":false,"took":12,"items":[` + strings.Join(items, ",") + `]}`)
	response := bulkResponse{works: works, status: 200, raw: raw}
	return response
}

func TestBulkReplyDecodingMatchesItsActions(t *testing.T) {
	works := bulkReplyFixture(2).works
	item := func(id, extra string) string {
		return `{"index":{"_index":"records","_id":"` + id + `","result":"updated","_version":2,"_seq_no":5,"_primary_term":1,"_shards":{"total":2,"successful":1,"failed":0},"status":200` + extra + `}}`
	}
	reply := func(errors string, items ...string) string {
		return `{"took":3,"errors":` + errors + `,"items":[` + strings.Join(items, ",") + `]}`
	}
	conflict := `{"index":{"_index":"records","_id":"1","status":409,"error":{"type":"version_conflict_engine_exception","reason":"x","caused_by":{"type":"y"}}}}`
	cases := []struct {
		name string
		raw  string
		ok   bool
	}{
		{name: "acknowledged", raw: reply("false", item("0", ""), item("1", "")), ok: true},
		{name: "unknown fields", raw: `{"ingest_took":0,"took":3,"errors":false,"items":[` + item("0", `,"forced_refresh":false,"get":{"found":true}`) + `,` + item("1", "") + `]}`, ok: true},
		{name: "escaped id", raw: reply("false", item("0", ""), item(`\u0031`, "")), ok: true},
		{name: "null reads as absent", raw: reply("false", strings.Replace(item("0", ""), `"_version":2`, `"_version":null`, 1), item("1", "")), ok: true},
		{name: "item error", raw: reply("true", item("0", ""), conflict), ok: true},
		{name: "errors flag disagrees", raw: reply("false", item("0", ""), conflict), ok: false},
		{name: "null took", raw: `{"took":null,"errors":false,"items":[` + item("0", "") + `,` + item("1", "") + `]}`, ok: false},
		{name: "missing errors", raw: `{"took":3,"items":[` + item("0", "") + `,` + item("1", "") + `]}`, ok: false},
		{name: "names match exactly", raw: `{"took":3,"Errors":false,"items":[` + item("0", "") + `,` + item("1", "") + `]}`, ok: false},
		{name: "case variant beside a reply name", raw: `{"took":3,"errors":false,"Errors":true,"items":[` + item("0", "") + `,` + item("1", "") + `]}`, ok: false},
		{name: "case variant beside an item name", raw: reply("false", item("0", `,"Status":500`), item("1", "")), ok: false},
		{name: "case variant of an absent item name", raw: reply("false", item("0", `,"Error":{"type":"x"}`), item("1", "")), ok: false},
		{name: "case variant beside a shard name", raw: reply("false", strings.Replace(item("0", ""), `"failed":0`, `"failed":0,"Successful":0`, 1), item("1", "")), ok: false},
		{name: "case variant beside an error name", raw: reply("true", item("0", ""), strings.Replace(conflict, `"reason"`, `"Type":"other","reason"`, 1)), ok: false},
		{name: "string version", raw: reply("false", strings.Replace(item("0", ""), `"_version":2`, `"_version":"2"`, 1), item("1", "")), ok: false},
		{name: "fractional sequence", raw: reply("false", strings.Replace(item("0", ""), `"_seq_no":5`, `"_seq_no":1.5`, 1), item("1", "")), ok: false},
		{name: "string error", raw: reply("false", item("0", `,"error":"x"`), item("1", "")), ok: false},
		{name: "second name in an entry", raw: reply("false", item("0", ""), strings.TrimSuffix(item("1", ""), "}")+`,"index2":{}}`), ok: false},
		{name: "another action", raw: reply("false", item("0", ""), strings.Replace(item("1", ""), `"index"`, `"create"`, 1)), ok: false},
		{name: "another id", raw: reply("false", item("0", ""), item("2", "")), ok: false},
		{name: "extra item", raw: reply("false", item("0", ""), item("1", ""), item("2", "")), ok: false},
		{name: "missing item", raw: reply("false", item("0", "")), ok: false},
		{name: "null items", raw: `{"took":3,"errors":false,"items":null}`, ok: false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if validateJSON(context.Background(), []byte(c.raw)) != nil {
				t.Fatal("fixture is not valid JSON", c.raw)
			}
			decoded, ok := decodeBulkReply([]byte(c.raw), works)
			if ok != c.ok {
				t.Fatal("unexpected decoding", ok, c.raw)
			}
			if !ok {
				return
			}
			first := decoded.items[0]
			if first.result != "updated" || first.seq != (optional{value: 5, set: true}) || !first.validShards() || first.status != 200 {
				t.Fatal("item fields lost", first)
			}
			if first.version.set == strings.Contains(c.raw, `"_version":null`) {
				t.Fatal("null version was not read as absent", first.version)
			}
			if c.name == "item error" && (!decoded.items[1].hasError || decoded.items[1].errorType != "version_conflict_engine_exception" || decoded.items[1].status != 409) {
				t.Fatal("item error lost", decoded.items[1])
			}
		})
	}
}

func BenchmarkBulkResults(b *testing.B) {
	a := &Adapter{dialect: ElasticsearchProduct}
	response := bulkReplyFixture(128)
	b.ReportAllocs()
	b.SetBytes(int64(len(response.raw)))
	for b.Loop() {
		results := a.bulkResults(context.Background(), response)
		if results[127].GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
			b.Fatal("fixture was not acknowledged")
		}
	}
}
