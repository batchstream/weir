// Package searchstore implements the qualified direct-record Search profiles.
package searchstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protocol"
	"google.golang.org/protobuf/proto"
)

type Config struct {
	Store, URL, Index, Profile string
	Pool                       int
}
type Adapter struct {
	config    Config
	client    *http.Client
	transport *http.Transport
	ctx       context.Context
	cancel    context.CancelFunc
	once      sync.Once
}
type plan struct {
	id, action string
	source     []byte
}
type capabilities struct{ source, write bool }

var indexPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,62}$`)

func Open(ctx context.Context, cfg Config) (*Adapter, error) {
	name, segments, err := protocol.ParseResource("weir://" + cfg.Store)
	if err != nil || name != cfg.Store || len(segments) != 0 || !indexPattern.MatchString(cfg.Index) || cfg.Pool < 1 || cfg.Pool > 32 {
		return nil, fmt.Errorf("invalid search configuration")
	}
	if cfg.Profile != "elasticsearch-8.17.0" && cfg.Profile != "opensearch-2.19.0" {
		return nil, fmt.Errorf("unsupported search profile")
	}
	endpoint, err := url.Parse(cfg.URL)
	if err != nil || endpoint.Scheme != "http" || endpoint.User != nil || endpoint.Path != "" || endpoint.RawQuery != "" || endpoint.Fragment != "" || endpoint.Port() == "" {
		return nil, fmt.Errorf("search requires a credential-free explicit loopback HTTP endpoint")
	}
	ip := net.ParseIP(endpoint.Hostname())
	if ip == nil || !ip.IsLoopback() {
		return nil, fmt.Errorf("search milestone is loopback only")
	}
	transport := newTransport(cfg.Pool)
	client := &http.Client{Transport: transport, CheckRedirect: noRedirect}
	lifetime, cancel := context.WithCancel(context.Background())
	a := &Adapter{config: cfg, client: client, transport: transport, ctx: lifetime, cancel: cancel}
	if err := a.qualify(ctx); err != nil {
		_ = a.Close()
		return nil, err
	}
	return a, nil
}
func (a *Adapter) Close() error {
	a.once.Do(func() { a.cancel(); a.transport.CloseIdleConnections() })
	return nil
}
func (a *Adapter) qualify(ctx context.Context) error {
	call := exchange{path: "/", limit: metadataLimit}
	status, raw, err := a.request(ctx, call)
	var info struct {
		Version struct {
			Number, Distribution string
			BuildFlavor          string `json:"build_flavor"`
		}
	}
	if err != nil || status != 200 || json.Unmarshal(raw, &info) != nil {
		return fmt.Errorf("search version qualification failed")
	}
	switch a.config.Profile {
	case "elasticsearch-8.17.0":
		if info.Version.Number != "8.17.0" || info.Version.BuildFlavor != "default" || info.Version.Distribution != "" {
			return fmt.Errorf("expected Elasticsearch 8.17.0 default distribution")
		}
	case "opensearch-2.19.0":
		if info.Version.Number != "2.19.0" || info.Version.Distribution != "opensearch" {
			return fmt.Errorf("expected OpenSearch 2.19.0 distribution")
		}
	}
	call.path = "/_cluster/settings?include_defaults=true&flat_settings=true"
	status, raw, err = a.request(ctx, call)
	var settings struct{ Defaults, Persistent, Transient map[string]json.RawMessage }
	if err != nil || status != 200 || json.Unmarshal(raw, &settings) != nil {
		return fmt.Errorf("cannot verify search automatic index creation policy")
	}
	auto := settings.Defaults["action.auto_create_index"]
	if value, ok := settings.Persistent["action.auto_create_index"]; ok {
		auto = value
	}
	if value, ok := settings.Transient["action.auto_create_index"]; ok {
		auto = value
	}
	if string(auto) != `"false"` && string(auto) != "false" {
		return fmt.Errorf("search profile requires action.auto_create_index=false; Weir never modifies settings")
	}
	_, failure, _ := a.inspect(ctx)
	if failure != nil {
		return fmt.Errorf("search index qualification failed: %s", failure.Message)
	}
	return nil
}
func (a *Adapter) inspect(ctx context.Context) (capabilities, *pb.Failure, execution.Feedback) {
	caps := capabilities{}
	call := exchange{path: "/" + a.config.Index + "?flat_settings=true", limit: metadataLimit}
	status, raw, err := a.request(ctx, call)
	if err == errTransport && ctx.Err() == nil {
		return caps, protocol.Fail(pb.FailureCode_UNAVAILABLE, "index qualification transport failed"), execution.Congested
	}
	if err != nil {
		return caps, protocol.Fail(pb.FailureCode_UNAVAILABLE, "index qualification response unavailable"), execution.Neutral
	}
	if status == 429 || status == 503 {
		return caps, protocol.Fail(pb.FailureCode_UNAVAILABLE, "backend capacity unavailable"), execution.Congested
	}
	if status != 200 {
		return caps, protocol.Fail(pb.FailureCode_UNSUPPORTED, "configured concrete index unavailable"), execution.Neutral
	}
	type indexInfo struct {
		DataStream string `json:"data_stream"`
		Settings   map[string]string
		Mappings   struct {
			Source struct {
				Enabled            *bool
				Mode               string
				Includes, Excludes []string
			} `json:"_source"`
			Routing struct{ Required bool } `json:"_routing"`
		}
	}
	var indexes map[string]indexInfo
	if json.Unmarshal(raw, &indexes) != nil || len(indexes) != 1 {
		return caps, protocol.Fail(pb.FailureCode_UNSUPPORTED, "index qualification invalid"), execution.Neutral
	}
	index, ok := indexes[a.config.Index]
	if !ok || index.DataStream != "" || index.Settings["index.uuid"] == "" || index.Settings["index.number_of_shards"] != "1" || index.Mappings.Routing.Required || index.Settings["index.routing_partition_size"] != "" && index.Settings["index.routing_partition_size"] != "1" || index.Settings["index.mode"] != "" && index.Settings["index.mode"] != "standard" {
		return caps, protocol.Fail(pb.FailureCode_UNSUPPORTED, "single-primary concrete standard index with default routing required"), execution.Neutral
	}
	source := index.Mappings.Source
	caps.source = (source.Enabled == nil || *source.Enabled) && (source.Mode == "" || source.Mode == "stored") && len(source.Includes) == 0 && len(source.Excludes) == 0 && (index.Settings["index.mapping.source.mode"] == "" || index.Settings["index.mapping.source.mode"] == "stored")
	final := index.Settings["index.final_pipeline"]
	caps.write = caps.source && (final == "" || final == "_none")
	return caps, nil, execution.Neutral
}
func (a *Adapter) Prepare(op *pb.BulkOperation) (*execution.Plan, *pb.Failure) {
	if failure := protocol.Validate(op, a.config.Store); failure != nil {
		return nil, failure
	}
	resource := protocol.Resource(op)
	_, segments, err := protocol.ParseResource(resource)
	if err != nil || len(segments) != 2 || segments[0] != a.config.Index {
		return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "only the configured concrete index is supported")
	}
	key := segments[1]
	if !strings.HasPrefix(key, "s:") || len(key) <= 2 || len(key[2:]) > 512 {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "exact string ID of 1-512 bytes required")
	}
	native := &plan{id: key[2:]}
	work := &execution.Plan{Operation: op, Key: resource, Backend: native, Bytes: proto.Size(op) + len(resource)*2 + 1024, ResultBytes: protocol.ResultOverhead, Token: "write", Batchable: true}
	if read := op.GetRead(); read != nil {
		if read.AdapterOptions != nil || read.ReadMediaType != "" && read.ReadMediaType != "application/json" {
			return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "read representation/options unsupported")
		}
		native.action = "read"
		work.Batchable = false
		work.Token = "read"
		work.ResultBytes += protocol.MaxDocument
	} else {
		mutation := op.GetMutate()
		if mutation.AdapterOptions != nil {
			return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "adapter options unsupported")
		}
		var document *pb.Document
		switch action := mutation.Action.(type) {
		case *pb.MutateRequest_Put:
			native.action = "index"
			document = action.Put
		case *pb.MutateRequest_Create:
			native.action = "create"
			document = action.Create
		case *pb.MutateRequest_Replace:
			native.action = "replace"
			document = action.Replace
			work.Batchable = false
			work.Token = "replace"
		case *pb.MutateRequest_Delete:
			native.action = "delete"
		default:
			return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "mutation unsupported")
		}
		if document != nil {
			if document.MediaType != "application/json" {
				return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "only JSON source is supported")
			}
			if !object(document.Data) || validateJSON(document.Data, 4096) != nil {
				return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "JSON source validation/limits failed")
			}
			native.source = document.Data
		}
	}
	return work, nil
}

type getReply struct {
	Index  string          `json:"_index"`
	ID     string          `json:"_id"`
	Found  *bool           `json:"found"`
	Source json.RawMessage `json:"_source"`
	Seq    *int64          `json:"_seq_no"`
	Term   *int64          `json:"_primary_term"`
}

func (a *Adapter) get(ctx context.Context, p *plan) (*getReply, *pb.Failure, execution.Feedback) {
	call := exchange{path: "/" + a.config.Index + "/_doc/" + url.PathEscape(p.id) + "?realtime=true", limit: protocol.MaxDocument + 32<<10}
	status, raw, err := a.request(ctx, call)
	if err == errTransport && ctx.Err() == nil {
		return nil, protocol.Fail(pb.FailureCode_UNAVAILABLE, "record transport failed"), execution.Congested
	}
	if err != nil {
		return nil, protocol.Fail(pb.FailureCode_UNAVAILABLE, "record response unavailable or exceeds limits"), execution.Neutral
	}
	if status == 429 || status == 503 {
		return nil, protocol.Fail(pb.FailureCode_UNAVAILABLE, "backend capacity unavailable"), execution.Congested
	}
	var reply getReply
	if json.Unmarshal(raw, &reply) != nil || reply.Index != a.config.Index || reply.ID != p.id || reply.Found == nil {
		return nil, protocol.Fail(pb.FailureCode_UNAVAILABLE, "incomplete record response"), execution.Neutral
	}
	if !*reply.Found && status == 404 {
		return &reply, nil, execution.Healthy
	}
	if status != 200 || !*reply.Found || reply.Seq == nil || reply.Term == nil || *reply.Seq < 0 || *reply.Term < 1 || !object(reply.Source) {
		return nil, protocol.Fail(pb.FailureCode_UNAVAILABLE, "incomplete record response"), execution.Neutral
	}
	if len(reply.Source) > protocol.MaxDocument {
		return nil, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "stored record exceeds read limit"), execution.Neutral
	}
	return &reply, nil, execution.Healthy
}
func readResult(work *execution.Plan, reply *getReply, failure *pb.Failure) *pb.BulkResult {
	var read *pb.ReadResult
	switch {
	case failure != nil:
		read = protocol.ReadFailure(failure)
	case !*reply.Found:
		read = protocol.Missing()
	default:
		document := &pb.Document{MediaType: "application/json", Data: reply.Source}
		read = protocol.ReadDocument(document)
	}
	variant := &pb.BulkResult_Read{Read: read}
	result := &pb.BulkResult{Index: work.Operation.Index, Result: variant}
	return result
}

func (a *Adapter) Execute(ctx context.Context, works []*execution.Plan) ([]*pb.BulkResult, execution.Feedback) {
	if len(works) == 0 {
		return nil, execution.Neutral
	}
	totalBytes := 0
	for _, work := range works {
		totalBytes += work.Bytes
	}
	if len(works) > 128 || totalBytes > 8<<20 {
		results := make([]*pb.BulkResult, len(works))
		for i, work := range works {
			results[i] = protocol.ResultError(work.Operation, pb.MutationOutcome_NOT_STARTED, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "batch exceeds execution bounds"))
		}
		return results, execution.Neutral
	}
	caps, failure, sample := a.inspect(ctx)
	results := make([]*pb.BulkResult, len(works))
	pending := make([]*execution.Plan, 0, len(works))
	positions := make([]int, 0, len(works))
	var request bytes.Buffer
	for i, work := range works {
		native := work.Backend.(*plan)
		denied := failure
		if denied == nil && (native.action == "read" && !caps.source || native.action != "read" && native.action != "delete" && !caps.write) {
			denied = protocol.Fail(pb.FailureCode_UNSUPPORTED, "stored full source and no final pipeline required for this operation")
		}
		if denied != nil {
			results[i] = protocol.ResultError(work.Operation, pb.MutationOutcome_NOT_APPLIED, denied)
			continue
		}
		var observed *getReply
		if native.action == "read" || native.action == "replace" {
			observed, denied, sample = a.get(ctx, native)
			if native.action == "read" {
				results[i] = readResult(work, observed, denied)
				continue
			}
			if denied == nil && !*observed.Found {
				denied = protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, "record missing")
				sample = execution.Neutral
			}
			if denied != nil {
				results[i] = protocol.ResultError(work.Operation, pb.MutationOutcome_NOT_APPLIED, denied)
				continue
			}
		}
		metadata := map[string]any{"_index": a.config.Index, "_id": native.id}
		action := native.action
		if action != "delete" {
			metadata["pipeline"] = "_none"
		}
		if action == "replace" {
			action = "index"
			metadata["if_seq_no"] = *observed.Seq
			metadata["if_primary_term"] = *observed.Term
		}
		header := map[string]any{action: metadata}
		encoded, _ := json.Marshal(header)
		request.Write(encoded)
		request.WriteByte('\n')
		if native.source != nil {
			// NDJSON framing requires compact source, without interpreting any number.
			if err := json.Compact(&request, native.source); err != nil {
				panic("validated source changed")
			}
			request.WriteByte('\n')
		}
		pending = append(pending, work)
		positions = append(positions, i)
	}
	if len(pending) == 0 {
		return results, sample
	}
	if request.Len() > 8<<20 {
		for _, i := range positions {
			results[i] = protocol.ResultError(works[i].Operation, pb.MutationOutcome_NOT_APPLIED, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "native request exceeds bound"))
		}
		return results, execution.Neutral
	}
	call := exchange{path: "/_bulk?pipeline=_none&refresh=false&wait_for_active_shards=1&timeout=1s", body: request.Bytes(), limit: responseLimit}
	status, raw, err := a.request(ctx, call)
	replies, feedback := a.bulkResults(pending, status, raw, err)
	for i, position := range positions {
		results[position] = replies[i]
	}
	return results, feedback
}
