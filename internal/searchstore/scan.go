package searchstore

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"strconv"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protocol"
	"google.golang.org/protobuf/proto"
)

const scanPageBudget = 16 << 20
const pitKeepAlive = "60s"
const maxPITBytes = 16 << 10

type scanPlan struct {
	query                    json.RawMessage
	items                    int
	pit                      string
	after                    int64
	hasAfter, opened, closed bool
}

func (a *Adapter) PrepareScan(req *pb.ScanRequest) (*execution.Plan, *pb.Failure) {
	if f := protocol.ValidateScan(req, a.config.Store); f != nil {
		return nil, f
	}
	_, parts, _ := protocol.ParseResource(req.Resource)
	if len(parts) != 1 || parts[0] != a.config.Index {
		return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "only the configured concrete index supports Scan")
	}
	if req.ReadMediaType != "" && req.ReadMediaType != "application/json" {
		return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "Scan outputs native JSON hits")
	}
	native := &scanPlan{items: protocol.FetchItems(req.FetchItemsHint), query: json.RawMessage(`{"match_all":{}}`)}
	if d := req.Selector; d != nil {
		if d.MediaType != "application/json" {
			return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "search selector requires JSON")
		}
		if !object(d.Data) || validateJSON(d.Data, 4096) != nil {
			return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid or excessive JSON selector")
		}
		var fields map[string]json.RawMessage
		if json.Unmarshal(d.Data, &fields) != nil {
			return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid selector")
		}
		for name, value := range fields {
			if name != "query" || !object(value) {
				return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "only native query is allowed in the selector")
			}
			native.query = value
		}
	}
	p := &execution.Plan{Scan: true, Key: req.Resource, Token: "scan", Bytes: proto.Size(req) + protocol.EntryOverhead + 4096, ResultBytes: protocol.MaxDocument + protocol.ResultOverhead, PageBytes: scanPageBudget, Backend: native}
	return p, nil
}

func (a *Adapter) FetchScan(ctx context.Context, p *execution.Plan) (*execution.ScanPage, execution.Feedback) {
	n := p.Backend.(*scanPlan)
	page := &execution.ScanPage{}
	if n.closed || ctx.Err() != nil {
		page.Failure = protocol.ContextFailure(ctx)
		return page, execution.Neutral
	}
	if !n.opened {
		caps, f, fb := a.inspect(ctx, false)
		if f != nil {
			page.Failure = f
			return page, fb
		}
		if !caps.source {
			page.Failure = protocol.Fail(pb.FailureCode_UNSUPPORTED, "Scan requires stored full source")
			return page, execution.Neutral
		}
		call := exchange{path: "/" + a.config.Index + "/_pit?keep_alive=" + pitKeepAlive + "&allow_partial_search_results=false", body: []byte("{}"), limit: metadataLimit}
		if a.config.Profile == "opensearch-2.19.0" {
			call.path = "/" + a.config.Index + "/_search/point_in_time?keep_alive=" + pitKeepAlive + "&allow_partial_pit_creation=false"
		}
		n.opened = true
		status, raw, err := a.request(ctx, call)
		f, fb = a.scanExchangeFailure(ctx, status, err)
		if f != nil {
			page.Failure = f
			return page, fb
		}
		page.Failure = a.openPITReply(raw, n)
		if page.Failure != nil {
			return page, execution.Neutral
		}
		// Opening is a scheduler step of its own. Return an empty, nonterminal page
		// so the same continuation yields before the first search fetch.
		return page, execution.Healthy
	}
	if n.pit == "" {
		page.Failure = protocol.Fail(pb.FailureCode_INTERNAL, "PIT unavailable")
		return page, execution.Neutral
	}
	sort := "_shard_doc"
	if a.config.Profile == "opensearch-2.19.0" {
		sort = "_doc"
	} // Unique in the qualified single-shard PIT reader.
	pit := map[string]any{"id": n.pit, "keep_alive": pitKeepAlive}
	body := map[string]any{"pit": pit, "query": n.query, "size": n.items, "sort": []string{sort}, "track_total_hits": false, "timeout": "1s", "_source": true}
	if n.hasAfter {
		body["search_after"] = []int64{n.after}
	}
	encoded, _ := json.Marshal(body)
	call := exchange{path: "/_search?allow_partial_search_results=false", body: encoded, limit: responseLimit}
	status, raw, err := a.request(ctx, call)
	f, fb := a.scanExchangeFailure(ctx, status, err)
	if f != nil {
		page.Failure = f
		return page, fb
	}
	page = a.scanReply(raw, n)
	if page.Failure != nil {
		return page, execution.Neutral
	}
	return page, execution.Healthy
}

func (a *Adapter) scanExchangeFailure(ctx context.Context, status int, err error) (*pb.Failure, execution.Feedback) {
	if ctx.Err() != nil {
		return protocol.ContextFailure(ctx), execution.Neutral
	}
	if err == errTimeout {
		return protocol.Fail(pb.FailureCode_DEADLINE_EXCEEDED, "Scan backend call timed out"), execution.Congested
	}
	if err == errResponseLimit {
		return protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "Scan native response exceeds byte bound"), execution.Neutral
	}
	if err == errTransport || status == 429 || status == 503 {
		return protocol.Fail(pb.FailureCode_UNAVAILABLE, "Scan backend unavailable"), execution.Congested
	}
	if err != nil {
		return protocol.Fail(pb.FailureCode_INTERNAL, "invalid, truncated or excessive Scan response"), execution.Neutral
	}
	if status != 200 {
		return protocol.Fail(pb.FailureCode_UNAVAILABLE, "PIT or search request failed"), execution.Neutral
	}
	return nil, execution.Neutral
}

func (a *Adapter) openPITReply(raw []byte, n *scanPlan) *pb.Failure {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return protocol.Fail(pb.FailureCode_INTERNAL, "invalid PIT response")
	}
	key := "id"
	if a.config.Profile == "opensearch-2.19.0" {
		key = "pit_id"
	}
	var id string
	if json.Unmarshal(fields[key], &id) != nil || len(id) == 0 || len(id) > maxPITBytes {
		return protocol.Fail(pb.FailureCode_INTERNAL, "missing or excessive PIT ID")
	}
	n.pit = id // Keep latest known state for cleanup even on a failed envelope.
	for name := range fields {
		if name != key && name != "_shards" && (name != "creation_time" || a.config.Profile != "opensearch-2.19.0") {
			return protocol.Fail(pb.FailureCode_INTERNAL, "unexpected PIT envelope field")
		}
	}
	if a.config.Profile == "opensearch-2.19.0" {
		var created int64
		if json.Unmarshal(fields["creation_time"], &created) != nil || created <= 0 {
			return protocol.Fail(pb.FailureCode_INTERNAL, "missing PIT creation evidence")
		}
	}
	return scanShards(fields["_shards"])
}

func scanShards(raw []byte) *pb.Failure {
	var shards struct {
		Total, Successful, Skipped, Failed *int
		Failures                           json.RawMessage
	}
	if json.Unmarshal(raw, &shards) != nil || shards.Total == nil || shards.Successful == nil || shards.Skipped == nil || shards.Failed == nil {
		return protocol.Fail(pb.FailureCode_INTERNAL, "missing shard participation evidence")
	}
	if *shards.Failed > 0 {
		return protocol.Fail(pb.FailureCode_UNAVAILABLE, "Scan shard failure")
	}
	// Native successful includes normally skipped shards; do not add skipped a
	// second time, or misclassify a healthy match_none query as partial results.
	if *shards.Total != 1 || *shards.Successful != 1 || *shards.Failed != 0 || *shards.Skipped < 0 || *shards.Skipped > *shards.Successful {
		return protocol.Fail(pb.FailureCode_INTERNAL, "inconsistent shard participation")
	}
	if shards.Failures != nil && string(shards.Failures) != "[]" {
		return protocol.Fail(pb.FailureCode_INTERNAL, "contradictory shard failures")
	}
	return nil
}

func (a *Adapter) scanReply(raw []byte, n *scanPlan) *execution.ScanPage {
	page := &execution.ScanPage{Failure: protocol.Fail(pb.FailureCode_INTERNAL, "incomplete or malformed search envelope")}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return page
	}
	if idRaw, exists := fields["pit_id"]; exists {
		var id string
		if json.Unmarshal(idRaw, &id) != nil || id == "" || len(id) > maxPITBytes {
			return page
		}
		n.pit = id
	} else if a.config.Profile == "elasticsearch-8.17.0" {
		return page
	}
	for _, key := range []string{"error", "aggregations", "suggest", "_clusters"} {
		if _, exists := fields[key]; exists {
			return page
		}
	}
	var timedOut *bool
	var took *int64
	if json.Unmarshal(fields["timed_out"], &timedOut) != nil || timedOut == nil || json.Unmarshal(fields["took"], &took) != nil || took == nil || *took < 0 {
		return page
	}
	if *timedOut {
		page.Failure = protocol.Fail(pb.FailureCode_DEADLINE_EXCEEDED, "search timed out")
		return page
	}
	if early, exists := fields["terminated_early"]; exists {
		var terminated *bool
		if json.Unmarshal(early, &terminated) != nil || terminated == nil || *terminated {
			return page
		}
	}
	if f := scanShards(fields["_shards"]); f != nil {
		page.Failure = f
		return page
	}
	var hits struct {
		Hits     json.RawMessage
		MaxScore json.RawMessage `json:"max_score"`
	}
	if json.Unmarshal(fields["hits"], &hits) != nil || len(hits.Hits) == 0 || hits.Hits[0] != '[' || !scanScore(hits.MaxScore) {
		return page
	}
	var rows []json.RawMessage
	if json.Unmarshal(hits.Hits, &rows) != nil {
		return page
	}
	if len(rows) > n.items {
		page.Failure = protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "search page exceeds item bound")
		return page
	}
	docs := make([]*pb.Document, 0, len(rows))
	last := n.after
	hasLast := n.hasAfter
	for _, row := range rows {
		if len(row) > protocol.MaxDocument {
			page.Failure = protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "native hit exceeds output bound")
			return page
		}
		var hit struct {
			Index  string            `json:"_index"`
			ID     string            `json:"_id"`
			Source json.RawMessage   `json:"_source"`
			Score  json.RawMessage   `json:"_score"`
			Sort   []json.RawMessage `json:"sort"`
		}
		if json.Unmarshal(row, &hit) != nil || hit.Index != a.config.Index || hit.ID == "" || len(hit.ID) > 512 || !object(hit.Source) || !scanScore(hit.Score) || len(hit.Sort) != 1 {
			return page
		}
		position, err := strconv.ParseInt(string(hit.Sort[0]), 10, 64)
		if err != nil || position < 0 || hasLast && position <= last {
			return page
		}
		last, hasLast = position, true
		doc := &pb.Document{MediaType: "application/json", Data: row}
		docs = append(docs, doc)
	}
	n.after, n.hasAfter = last, hasLast
	page.Documents = docs
	// Only a complete, validated search envelope on this same PIT and sort can
	// establish exhaustion. total hits is deliberately neither requested nor used.
	page.Exhausted = len(rows) == 0
	page.Failure = nil
	return page
}

func scanScore(raw json.RawMessage) bool {
	if string(raw) == "null" {
		return true
	}
	if len(raw) == 0 || raw[0] != '-' && (raw[0] < '0' || raw[0] > '9') {
		return false
	}
	number, err := strconv.ParseFloat(string(raw), 64)
	return err == nil && !math.IsNaN(number) && !math.IsInf(number, 0)
}

func (a *Adapter) CloseScan(ctx context.Context, p *execution.Plan) *pb.Failure {
	n := p.Backend.(*scanPlan)
	if n.closed {
		return nil
	}
	n.closed = true
	defer func() { n.pit = ""; n.query = nil }()
	if n.pit == "" {
		if n.opened {
			return protocol.Fail(pb.FailureCode_UNAVAILABLE, "PIT allocation reply lost; remote cleanup unconfirmed")
		}
		return nil
	}
	body := map[string]any{"id": n.pit}
	endpoint := "/_pit"
	if a.config.Profile == "opensearch-2.19.0" {
		body = map[string]any{"pit_id": []string{n.pit}}
		endpoint = "/_search/point_in_time"
	}
	raw, _ := json.Marshal(body)
	call := exchange{path: endpoint, body: raw, limit: metadataLimit, method: http.MethodDelete}
	status, raw, err := a.request(ctx, call)
	if err != nil || status != 200 {
		return protocol.Fail(pb.FailureCode_UNAVAILABLE, "remote PIT cleanup unconfirmed")
	}
	if a.config.Profile == "opensearch-2.19.0" {
		var reply struct {
			PITs []struct {
				ID         string `json:"pit_id"`
				Successful bool
			}
		}
		if json.Unmarshal(raw, &reply) == nil && len(reply.PITs) == 1 && reply.PITs[0].ID == n.pit && reply.PITs[0].Successful {
			return nil
		}
	} else {
		var reply struct {
			Succeeded bool
			Freed     *int `json:"num_freed"`
		}
		if json.Unmarshal(raw, &reply) == nil && reply.Succeeded && reply.Freed != nil && *reply.Freed >= 0 && *reply.Freed <= 1 {
			return nil
		}
	}
	return protocol.Fail(pb.FailureCode_UNAVAILABLE, "remote PIT cleanup unconfirmed")
}
