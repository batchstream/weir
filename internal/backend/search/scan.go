package search

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"strconv"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"google.golang.org/protobuf/proto"
)

const scanPageBudget = 24 << 20
const scanWorkingBytes = 64 << 20
const pitKeepAlive = "60s"

const maxPITBytes = 16 << 10

type scanPlan struct {
	count                    uint64
	index                    string
	query                    json.RawMessage
	items                    int
	batchSize                int
	pit                      string
	after                    int64
	hasAfter, opened, closed bool
	pageSize                 uint64
	fingerprint              string
	resumed, transferred     bool
}

type scanCheckpoint struct {
	PIT       string `json:"pit"`
	After     int64  `json:"after"`
	BatchSize int    `json:"batch_size"`
}

func (a *Adapter) prepareScan(req *pb.ScanRequest) (*execution.Plan, *pb.Failure) {
	if f := protocol.ValidateScan(req); f != nil {
		return nil, f
	}
	parts, _ := protocol.ParseRelativeResource(req.Resource)
	if len(parts) != 1 || !validIndex(parts[0]) {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "Scan requires one concrete Search index")
	}
	native := &scanPlan{
		index:       parts[0],
		items:       execution.ScanBatchDocuments,
		batchSize:   execution.ScanBatchDocuments,
		query:       json.RawMessage(`{"match_all":{}}`),
		pageSize:    protocol.ScanPageSize(req),
		fingerprint: protocol.ScanFingerprint(req, a.config.Store, "search:"+a.dialect),
	}
	if d := req.Selector; d != nil {
		if d.ContentType != "application/json" {
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
	if len(req.ContinuationToken) != 0 {
		raw, err := protocol.DecodeScanToken(req.ContinuationToken, "search:"+a.dialect, native.fingerprint)
		var checkpoint scanCheckpoint
		decodeErr := json.Unmarshal(raw, &checkpoint)
		canonical, _ := json.Marshal(checkpoint)
		if err != nil || decodeErr != nil || !bytes.Equal(raw, canonical) || checkpoint.PIT == "" || len(checkpoint.PIT) > maxPITBytes || checkpoint.After < 0 || checkpoint.BatchSize < 1 || checkpoint.BatchSize > execution.ScanBatchDocuments {
			return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid or mismatched Search Scan continuation")
		}
		native.pit = checkpoint.PIT
		native.after = checkpoint.After
		native.hasAfter = true
		native.opened = true
		native.resumed = true
		native.batchSize = checkpoint.BatchSize
	}
	p := &execution.Plan{
		Singleton:    true,
		Key:          req.Resource,
		Bytes:        proto.Size(req) + execution.EntryOverheadBytes + 4096,
		ResultBytes:  execution.ScanResultBytes,
		WorkingBytes: scanWorkingBytes,
		Backend:      native,
	}
	return p, nil
}

func (a *Adapter) fetchScan(ctx context.Context, p *execution.Plan) (*execution.ScanPage, execution.Feedback) {
	n := p.Backend.(*scanPlan)
	page := &execution.ScanPage{}
	if n.closed || ctx.Err() != nil {
		page.Failure = protocol.ContextFailure(ctx)
		return page, execution.Neutral
	}
	if !n.opened {
		caps, f, fb := a.inspect(ctx, n.index, false)
		if f != nil {
			page.Failure = f
			return page, fb
		}
		if !caps.source {
			page.Failure = protocol.Fail(pb.FailureCode_UNSUPPORTED, "Scan requires stored full source")
			return page, execution.Neutral
		}
		call := exchange{
			path:  "/" + n.index + "/_pit?keep_alive=" + pitKeepAlive + "&allow_partial_search_results=false",
			body:  []byte("{}"),
			limit: metadataLimit,
		}
		if a.dialect == OpenSearchProduct {
			call.path = "/" + n.index + "/_search/point_in_time?keep_alive=" + pitKeepAlive + "&allow_partial_pit_creation=false"
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
	if n.count >= n.pageSize {
		page.Failure = protocol.Fail(pb.FailureCode_INTERNAL, "Scan fetched beyond its logical page")
		return page, execution.Neutral
	}
	remaining := n.pageSize - n.count
	n.items = min(n.batchSize, execution.ScanBatchDocuments, int(remaining))
	sort := "_shard_doc"
	if a.dialect == OpenSearchProduct {
		sort = "_doc"
	} // Unique in the qualified single-shard PIT reader.
	pit := map[string]any{"id": n.pit, "keep_alive": pitKeepAlive}
	body := map[string]any{
		"pit":              pit,
		"query":            n.query,
		"size":             n.items,
		"sort":             []string{sort},
		"track_total_hits": false,
		"timeout":          "1s",
		"_source":          true,
	}
	if n.hasAfter {
		body["search_after"] = []int64{n.after}
	}
	var raw []byte
	for {
		body["size"] = n.items
		encoded, _ := json.Marshal(body)
		call := exchange{
			path:      "/_search?allow_partial_search_results=false",
			body:      encoded,
			limit:     responseLimit,
			jsonNodes: n.items*(16384+32) + 64,
		}
		status, reply, err := a.request(ctx, call)
		// The read-only search has not advanced its checkpoint. Retry an excessive
		// response at a smaller size within the same absolute fetch deadline.
		if err == errResponseLimit && n.items > 1 && ctx.Err() == nil {
			n.items = max(1, n.items/2)
			n.batchSize = n.items
			continue
		}
		f, fb := a.scanExchangeFailure(ctx, status, err)
		if f != nil {
			page.Failure = f
			return page, fb
		}
		raw = reply
		break
	}
	page = a.scanReply(raw, n)
	if page.Failure != nil {
		return page, execution.Neutral
	}
	if !page.Exhausted && n.count+uint64(len(page.Documents)) >= n.pageSize {
		checkpoint := scanCheckpoint{PIT: n.pit, After: n.after, BatchSize: n.batchSize}
		state, _ := json.Marshal(checkpoint)
		token, err := protocol.EncodeScanToken("search:"+a.dialect, n.fingerprint, state)
		if err != nil {
			page.Documents = nil
			page.Failure = protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "Scan continuation exceeds bound")
			return page, execution.Neutral
		}
		page.Complete = true
		page.NextContinuationToken = token
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
	if a.dialect == OpenSearchProduct {
		key = "pit_id"
	}
	var id string
	if json.Unmarshal(fields[key], &id) != nil || len(id) == 0 || len(id) > maxPITBytes {
		return protocol.Fail(pb.FailureCode_INTERNAL, "missing or excessive PIT ID")
	}
	n.pit = id // Keep latest known state for cleanup even on a failed envelope.
	for name := range fields {
		if name != key && name != "_shards" && (name != "creation_time" || a.dialect != OpenSearchProduct) {
			return protocol.Fail(pb.FailureCode_INTERNAL, "unexpected PIT envelope field")
		}
	}
	if a.dialect == OpenSearchProduct {
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
	} else if a.dialect == ElasticsearchProduct {
		return page
	}
	for _, key := range []string{"error", "aggregations", "suggest", "_clusters"} {
		if _, exists := fields[key]; exists {
			return page
		}
	}
	var timedOut *bool
	var took *int64
	if json.Unmarshal(fields["timed_out"], &timedOut) != nil ||
		timedOut == nil ||
		json.Unmarshal(fields["took"], &took) != nil ||
		took == nil ||
		*took < 0 {
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
	acceptedLast := last
	acceptedBytes := 0
	retaining := true
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
		if json.Unmarshal(row, &hit) != nil ||
			hit.Index != n.index ||
			hit.ID == "" ||
			len(hit.ID) > 512 ||
			!object(hit.Source) ||
			validateJSON(hit.Source, 16384) != nil ||
			!scanScore(hit.Score) ||
			len(hit.Sort) != 1 {
			return page
		}
		position, err := strconv.ParseInt(string(hit.Sort[0]), 10, 64)
		if err != nil || position < 0 || hasLast && position <= last {
			return page
		}
		last, hasLast = position, true
		if retaining && acceptedBytes+len(row) <= execution.ScanBatchBytes {
			doc := &pb.Document{ContentType: "application/json", Data: row}
			docs = append(docs, doc)
			acceptedBytes += len(row)
			acceptedLast = position
		} else {
			// Validate the complete envelope before publishing this ordered prefix.
			// Its tail is refetched from the last accepted position on the same PIT.
			retaining = false
		}
	}
	if len(docs) != 0 {
		n.after, n.hasAfter = acceptedLast, true
	}
	if len(docs) < len(rows) {
		n.batchSize = max(1, len(docs))
	}
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

func (a *Adapter) closeScan(ctx context.Context, p *execution.Plan) *pb.Failure {
	n := p.Backend.(*scanPlan)
	if n.closed {
		return nil
	}
	n.closed = true
	defer func() {
		n.pit = ""
		n.query = nil
	}()
	// An input checkpoint remains retryable until the backend's keep-alive
	// expires, including when the last page or its transport acknowledgement is
	// lost. The caller cannot acknowledge receipt through a completed Execute RPC.
	if n.resumed || n.transferred {
		return nil
	}
	if n.pit == "" {
		if n.opened {
			return protocol.Fail(pb.FailureCode_UNAVAILABLE, "PIT allocation reply lost; remote cleanup unconfirmed")
		}
		return nil
	}
	body := map[string]any{"id": n.pit}
	endpoint := "/_pit"
	if a.dialect == OpenSearchProduct {
		body = map[string]any{"pit_id": []string{n.pit}}
		endpoint = "/_search/point_in_time"
	}
	raw, _ := json.Marshal(body)
	call := exchange{path: endpoint, body: raw, limit: metadataLimit, method: http.MethodDelete}
	status, raw, err := a.request(ctx, call)
	if err != nil || status != 200 {
		return protocol.Fail(pb.FailureCode_UNAVAILABLE, "remote PIT cleanup unconfirmed")
	}
	if a.dialect == OpenSearchProduct {
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
