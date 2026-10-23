// Package search implements direct-record operations for Elasticsearch and OpenSearch.
package search

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/luaengine"
)

const ElasticsearchProduct = "elasticsearch"
const OpenSearchProduct = "opensearch"

type Config struct {
	Options *backend.Options
	Store   string
	URL     string

	Connection *Connection
	// Resolver optionally supplies a standard DNS I/O dependency; app uses system configuration.
	Resolver *net.Resolver
}

type Adapter struct {
	dialer          *connectionDialer
	config          Config
	dialect         string
	client          *http.Client
	transport       *http.Transport
	nativeTransport *http.Transport
	nativeClient    *http.Client
	ctx             context.Context
	cancel          context.CancelFunc
	once            sync.Once
	metrics         searchMetrics
}

type plan struct {
	index            string
	id               string
	action           string
	source           []byte
	program          *luaengine.Program
	input            *pb.Document
	waitUntilVisible bool
	validated        bool
}

type capabilities struct {
	index                                string
	source, write, nativeWrite, writable bool
}

func validIndex(name string) bool {
	if name == "" || len(name) > 255 || name == "." || name == ".." || strings.ToLower(name) != name || strings.ContainsAny(name, "\\/*?\"<>| ,#:+") || strings.ContainsRune("-_+", rune(name[0])) || !utf8.ValidString(name) {
		return false
	}
	for _, character := range name {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func (a *Adapter) options() backend.Options {
	if a.config.Options != nil {
		return *a.config.Options
	}
	return backend.DefaultOptions()
}

func Open(ctx context.Context, cfg Config) (*Adapter, error) {
	if err := ValidateConfig(cfg); err != nil {
		return nil, err
	}
	settings := backend.DefaultOptions()
	if cfg.Options != nil {
		settings = *cfg.Options
	}
	if err := settings.Validate(); err != nil {
		return nil, err
	}
	cfg.Options = &settings

	cfg.URL, _ = canonicalURL(cfg.URL)
	if cfg.Connection != nil {
		connection := *cfg.Connection
		cfg.Connection = &connection
	}
	tlsConfig, err := connectionTLS(cfg)
	if err != nil {
		return nil, err
	}
	lifetime, cancel := context.WithCancel(context.Background())
	dialer := &connectionDialer{
		ctx:       lifetime,
		timeout:   settings.ConnectTimeout,
		resolver:  cfg.Resolver,
		tlsConfig: tlsConfig,
		conns:     make(map[*searchConn]struct{}),
	}
	transport := newTransport(settings.ConnectTimeout, settings.IdleTimeout)
	transport.DialContext = dialer.dial
	transport.DialTLSContext = dialer.dial
	transport.TLSClientConfig = tlsConfig
	client := &http.Client{Transport: transport, CheckRedirect: noRedirect}
	nativeTransport := newTransport(settings.ConnectTimeout, settings.IdleTimeout)
	nativeTransport.DialContext = dialer.dial
	nativeTransport.DialTLSContext = dialer.dial
	nativeTransport.TLSClientConfig = tlsConfig
	nativeTransport.DisableKeepAlives = true
	nativeClient := &http.Client{Transport: nativeTransport, CheckRedirect: noRedirect}
	a := &Adapter{
		dialer:          dialer,
		config:          cfg,
		client:          client,
		transport:       transport,
		nativeTransport: nativeTransport,
		nativeClient:    nativeClient,
		ctx:             lifetime,
		cancel:          cancel,
	}
	if err := a.qualify(ctx); err != nil {
		_ = a.Close(ctx)
		return nil, err
	}
	return a, nil
}

func (a *Adapter) Close(context.Context) error {
	a.once.Do(func() {
		a.cancel()
		if a.dialer != nil {
			a.dialer.close()
			a.logConnections()
		}
		a.transport.CloseIdleConnections()
		if a.nativeTransport != nil {
			a.nativeTransport.CloseIdleConnections()
		}
	})
	return nil
}

func (a *Adapter) qualify(ctx context.Context) error {
	call := exchange{path: "/"}
	status, raw, err := a.request(ctx, call)
	var info struct {
		Version struct {
			Distribution string
			BuildFlavor  string `json:"build_flavor"`
		}
	}
	if err != nil || status != 200 || json.Unmarshal(raw, &info) != nil {
		return errors.New("search product identification failed")
	}
	switch {
	case info.Version.BuildFlavor == "default" && info.Version.Distribution == "":
		a.dialect = ElasticsearchProduct
	case info.Version.Distribution == "opensearch":
		a.dialect = OpenSearchProduct
	default:
		return errors.New("unsupported Search server product")
	}
	return nil
}

func (a *Adapter) inspect(ctx context.Context, target string, native bool) (capabilities, *pb.Failure) {
	caps := capabilities{}
	if ctx.Err() != nil {
		return caps, protocol.ContextFailure(ctx)
	}
	query := url.Values{
		"flat_settings": {"true"},
		"filter_path":   {"error,status,*.aliases,*.settings,*.mappings._source,*.mappings._routing,*.data_stream"},
	}
	call := exchange{path: "/" + url.PathEscape(target) + "?" + query.Encode(), native: native, operation: "metadata"}
	status, raw, err := a.request(ctx, call)
	if ctx.Err() != nil {
		return caps, protocol.ContextFailure(ctx)
	}
	if err == errTransport && ctx.Err() == nil {
		return caps, protocol.Fail(pb.FailureCode_UNAVAILABLE, "index qualification transport failed")
	}
	if err != nil {
		return caps, protocol.Fail(pb.FailureCode_UNAVAILABLE, "index qualification response unavailable")
	}
	if status == 429 || status == 503 {
		return caps, protocol.Fail(pb.FailureCode_UNAVAILABLE, "backend capacity unavailable")
	}
	if status != 200 {
		failure := a.nativeResponseFailure(ctx, status, raw)
		return caps, failure
	}
	type indexInfo struct {
		DataStream string `json:"data_stream"`
		Aliases    map[string]struct {
			Filter        json.RawMessage `json:"filter"`
			Routing       string          `json:"routing"`
			IndexRouting  string          `json:"index_routing"`
			SearchRouting string          `json:"search_routing"`
			IsWriteIndex  *bool           `json:"is_write_index"`
		} `json:"aliases"`
		Settings map[string]string
		Mappings struct {
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
		return caps, protocol.Fail(pb.FailureCode_UNSUPPORTED, "index qualification invalid")
	}
	var index indexInfo
	for name, info := range indexes {
		caps.index, index = name, info
	}
	caps.writable = true
	if caps.index != target {
		alias, ok := index.Aliases[target]
		if !ok || len(alias.Filter) != 0 || alias.Routing != "" || alias.IndexRouting != "" || alias.SearchRouting != "" {
			return caps, protocol.Fail(pb.FailureCode_UNSUPPORTED, "single-index alias without filter or routing required")
		}
		caps.writable = alias.IsWriteIndex == nil || *alias.IsWriteIndex
	}
	shards, err := strconv.Atoi(index.Settings["index.number_of_shards"])
	if !validIndex(caps.index) || shards < 1 || err != nil ||
		index.DataStream != "" ||
		index.Settings["index.uuid"] == "" ||
		index.Mappings.Routing.Required ||
		index.Settings["index.routing_partition_size"] != "" && index.Settings["index.routing_partition_size"] != "1" ||
		index.Settings["index.mode"] != "" && index.Settings["index.mode"] != "standard" {
		return caps, protocol.Fail(pb.FailureCode_UNSUPPORTED, "standard index with default routing required")
	}
	source := index.Mappings.Source
	caps.source = (source.Enabled == nil || *source.Enabled) &&
		(source.Mode == "" || source.Mode == "stored") &&
		len(source.Includes) == 0 &&
		len(source.Excludes) == 0 &&
		(index.Settings["index.mapping.source.mode"] == "" || index.Settings["index.mapping.source.mode"] == "stored")
	final := index.Settings["index.final_pipeline"]
	defaultPipeline := index.Settings["index.default_pipeline"]
	caps.nativeWrite = (defaultPipeline == "" || defaultPipeline == "_none") && (final == "" || final == "_none")
	caps.write = caps.source && (final == "" || final == "_none")
	return caps, nil
}

func (a *Adapter) prepareRecord(ctx context.Context, record *execution.Record) (*execution.Plan, *pb.Failure) {
	op := record.Command()
	segments := record.Segments()
	if len(segments) != 2 || !validIndex(segments[0]) {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "one Search index or single-index alias and string ID required")
	}
	key := segments[1]
	if !strings.HasPrefix(key, "s:") || len(key) <= 2 || len(key[2:]) > 512 {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "exact string ID of 1-512 bytes required")
	}
	native := &plan{index: segments[0], id: key[2:]}
	work := &execution.Plan{
		Command: op,
		Key:     record.Key(),
		Backend: native,
		Bytes:   record.Bytes(),
	}
	if read := op.GetRead(); read != nil {
		native.action = "read"
	} else {
		mutation := op.GetMutate()
		native.waitUntilVisible = mutation.GetOptions().GetVisibility() == pb.MutationVisibility_QUERY_VISIBLE
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
		case *pb.MutateRequest_Delete:
			native.action = "delete"
		case *pb.MutateRequest_AtomicTransform:
			if program := action.AtomicTransform.GetLua(); program != nil {
				if program.Input != nil && program.Input.ContentType != "application/json" {
					return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "Search Lua input must use JSON")
				}

				native.action = "program"
				settings := a.options().Lua
				luaProgram := &luaengine.Program{Limits: &settings}
				native.program = luaProgram
				native.input = program.Input
			} else {
				expression := action.AtomicTransform.GetBackendExpression()
				if expression.ContentType != ExpressionContentType {
					return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "unsupported Search expression profile")
				}
				native.action = "expression"
				native.source = expression.Data
			}
		default:
			return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "mutation unsupported")
		}
		if document != nil {
			if document.ContentType != "application/json" {
				return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "only JSON source is supported")
			}

			native.source = document.Data
		}
	}
	if ctx.Err() != nil {
		return nil, protocol.ContextFailure(ctx)
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
	Error  *nativeError    `json:"error"`
	Status int             `json:"status"`
}

func readResult(reply *getReply, failure *pb.Failure) *pb.Event {
	var read *pb.ReadResult
	switch {
	case failure != nil:
		read = protocol.ReadFailure(failure)
	case !*reply.Found:
		read = protocol.Missing()
	default:
		document := &pb.Document{ContentType: "application/json", Data: reply.Source}
		read = protocol.ReadDocument(document)
	}
	value := &pb.Event_ReadResult{ReadResult: read}
	event := &pb.Event{Value: value}
	return event
}
