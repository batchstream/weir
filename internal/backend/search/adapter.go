// Package search implements direct-record operations for Elasticsearch and OpenSearch.
package search

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"regexp"
	"strings"
	"sync"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/luaengine"
	"github.com/batchstream/weir/internal/protocol"
	"github.com/batchstream/weir/internal/value"
	"google.golang.org/protobuf/proto"
)

const ElasticsearchProduct = "elasticsearch"
const OpenSearchProduct = "opensearch"

type Config struct {
	Store string
	URL   string
	Pool  int
	// MaxReadSize bounds ordinary Record Read only; zero uses the 2 MiB protocol limit.
	MaxReadSize int
	Connection  *Connection
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
}

type plan struct {
	index          string
	id             string
	action         string
	source         []byte
	program        *luaengine.Program
	expectedResult string
}

type capabilities struct{ source, write, nativeWrite bool }

var indexPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,62}$`)

func Open(ctx context.Context, cfg Config) (*Adapter, error) {
	if err := ValidateConfig(cfg); err != nil {
		return nil, err
	}
	if cfg.MaxReadSize == 0 {
		cfg.MaxReadSize = protocol.MaxDocument
	}
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
		resolver:  cfg.Resolver,
		tlsConfig: tlsConfig,
		slots:     make(chan struct{}, cfg.Pool+1),
		conns:     make(map[*searchConn]struct{}),
	}
	transport := newTransport(cfg.Pool)
	transport.DialContext = dialer.dial
	transport.DialTLSContext = dialer.dial
	transport.TLSClientConfig = tlsConfig
	client := &http.Client{Transport: transport, CheckRedirect: noRedirect}
	nativeTransport := newTransport(1)
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
		_ = a.Close()
		return nil, err
	}
	return a, nil
}

func (a *Adapter) Close() error {
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
	call := exchange{path: "/", limit: metadataLimit}
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
	call.path = "/_cluster/settings?include_defaults=true&flat_settings=true"
	status, raw, err = a.request(ctx, call)
	var settings struct{ Defaults, Persistent, Transient map[string]json.RawMessage }
	if err != nil || status != 200 || json.Unmarshal(raw, &settings) != nil {
		return errors.New("cannot verify search automatic index creation policy")
	}
	auto := settings.Defaults["action.auto_create_index"]
	if value, ok := settings.Persistent["action.auto_create_index"]; ok {
		auto = value
	}
	if value, ok := settings.Transient["action.auto_create_index"]; ok {
		auto = value
	}
	if string(auto) != `"false"` && string(auto) != "false" {
		return errors.New("search requires action.auto_create_index=false; Weir never modifies settings")
	}
	return nil
}

func (a *Adapter) inspect(ctx context.Context, target string, native bool) (capabilities, *pb.Failure, execution.Feedback) {
	caps := capabilities{}
	call := exchange{path: "/" + target + "?flat_settings=true", limit: metadataLimit, native: native}
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
		return caps, protocol.Fail(pb.FailureCode_UNSUPPORTED, "requested concrete index unavailable"), execution.Neutral
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
	index, ok := indexes[target]
	if !ok ||
		index.DataStream != "" ||
		index.Settings["index.uuid"] == "" ||
		index.Settings["index.number_of_shards"] != "1" ||
		index.Mappings.Routing.Required ||
		index.Settings["index.routing_partition_size"] != "" && index.Settings["index.routing_partition_size"] != "1" ||
		index.Settings["index.mode"] != "" && index.Settings["index.mode"] != "standard" {
		return caps, protocol.Fail(pb.FailureCode_UNSUPPORTED, "single-primary concrete standard index with default routing required"), execution.Neutral
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
	return caps, nil, execution.Neutral
}

func (a *Adapter) prepareRecord(op *pb.Operation) (*execution.Plan, *pb.Failure) {
	if failure := protocol.Validate(op, a.config.Store); failure != nil {
		return nil, failure
	}
	resource := protocol.Resource(op)
	_, segments, err := protocol.ParseResource(resource)
	if err != nil || len(segments) != 2 || !indexPattern.MatchString(segments[0]) {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "one concrete Search index and string ID required")
	}
	key := segments[1]
	if !strings.HasPrefix(key, "s:") || len(key) <= 2 || len(key[2:]) > 512 {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "exact string ID of 1-512 bytes required")
	}
	native := &plan{index: segments[0], id: key[2:]}
	work := &execution.Plan{
		Operation:   op,
		Key:         resource,
		Backend:     native,
		Bytes:       proto.Size(op) + len(resource)*2 + 1024,
		ResultBytes: protocol.ResultOverhead,
	}
	if read := op.GetRead(); read != nil {
		if read.AdapterOptions != nil || read.ReadMediaType != "" && read.ReadMediaType != "application/json" {
			return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "read representation/options unsupported")
		}
		native.action = "read"
		// Reserve bounded source scratch for the batched pre-read.
		work.ResultBytes += a.maxReadSize()
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
			// Reserve bounded source scratch for the batched pre-read.
			work.ResultBytes += protocol.MaxDocument
		case *pb.MutateRequest_Delete:
			native.action = "delete"
		case *pb.MutateRequest_AtomicTransform:
			if program := action.AtomicTransform.GetProgram(); program != nil {
				if program.Input != nil && program.Input.MediaType != "application/json" {
					return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "Search Lua input must use JSON")
				}
				input := value.Value{Kind: value.Missing}
				if program.Input != nil {
					if validateJSON(program.Input.Data, 4096) != nil {
						return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid JSON transform input")
					}
					input, err = value.DecodeJSON(program.Input.Data)
					if err != nil {
						return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid JSON transform input")
					}
				}
				native.action = "program"
				luaProgram := &luaengine.Program{Source: string(program.Source), Input: input}
				native.program = luaProgram
				// Reserve bounded source scratch for the batched pre-read.
				work.ResultBytes += protocol.MaxDocument
			} else {
				expression := action.AtomicTransform.GetBackendExpression()
				if f := prepareExpression(expression); f != nil {
					return nil, f
				}
				native.action = "expression"
				native.source = expression.Data
			}
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
	Error  *nativeError    `json:"error"`
	Status int             `json:"status"`
}

func readResult(work *execution.Plan, reply *getReply, failure *pb.Failure) *pb.Result {
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
	variant := &pb.Result_Read{Read: read}
	result := &pb.Result{Index: work.Operation.Index, Result: variant}
	return result
}
