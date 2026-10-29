// Package mongodb owns MongoDB encoding, clients, sessions and execution evidence.
package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/luaengine"
	"github.com/batchstream/weir/internal/value"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"
)

// DefaultMaxConnecting matches the native driver connection creation default.
const DefaultMaxConnecting uint64 = 2

type Config struct {
	Options       *backend.Options
	MaxConnecting uint64
	URI           string
	Store         string
	Username      string
	CAFile        string
	Password      string
}

type Adapter struct {
	maxDocumentBytes int
	client           *mongo.Client
	deployment       driver.Deployment
	commandMonitor   *event.CommandMonitor
	config           Config
	once             sync.Once
	closeErr         error
	lifetime         context.Context
	cancelLifetime   context.CancelFunc
	targets          targetCache
}

type plan struct {
	target    namespace
	id        any
	document  bson.Raw
	action    string
	program   *luaengine.Program
	validated bool
}

func (a *Adapter) options() backend.Options {
	if a.config.Options != nil {
		return *a.config.Options
	}
	return backend.DefaultOptions()
}

func Open(ctx context.Context, cfg Config) (*Adapter, error) {
	profile, err := parseConfig(cfg)
	if err != nil {
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
	if cfg.MaxConnecting == 0 {
		cfg.MaxConnecting = DefaultMaxConnecting
	}

	opts, err := connectionOptions(cfg, profile)
	if err != nil {
		return nil, err
	}
	opts.SetAppName("weir:" + cfg.Store).
		SetMaxPoolSize(0).
		SetMaxConnecting(cfg.MaxConnecting).
		SetMinPoolSize(0).
		SetRetryWrites(false).
		SetRetryReads(false).
		SetMaxAdaptiveRetries(0).
		SetEnableOverloadRetargeting(false).
		SetCompressors(nil).
		SetServerMonitoringMode(options.ServerMonitoringModePoll).
		SetServerSelectionTimeout(0).
		SetConnectTimeout(settings.ConnectTimeout).
		SetReadPreference(readpref.Primary()).
		SetWriteConcern(writeconcern.Majority())
	if opts.Timeout != nil {
		return nil, fmt.Errorf("client timeoutMS is unsupported; caller context owns execution deadlines")
	}
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("invalid MongoDB connection profile")
	}
	// Typed operations and Native share the same configured topology and pool.
	// Native uses the pinned driver's Operation API to avoid implicit sessions
	// and command options added by Database.RunCommand.
	topologyConfig, err := topology.NewConfig(opts, nil)
	if err != nil {
		return nil, fmt.Errorf("MongoDB topology configuration rejected")
	}
	deployment, err := topology.New(topologyConfig)
	if err != nil {
		return nil, fmt.Errorf("MongoDB topology configuration rejected")
	}
	opts.Deployment = deployment
	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, fmt.Errorf("MongoDB client configuration rejected")
	}
	lifetime, cancelLifetime := context.WithCancel(context.Background())
	a := &Adapter{
		lifetime:       lifetime,
		cancelLifetime: cancelLifetime,
		client:         client,
		deployment:     deployment,
		commandMonitor: opts.Monitor,
		config:         cfg,
	}
	a.targets.entries = make(map[namespace]*targetCheck)
	a.targets.capacity = settings.MetadataCacheEntries
	if err = a.qualify(ctx, profile.replicaSet); err != nil {
		cleanup, cancel := context.WithTimeout(context.Background(), settings.CleanupTimeout)
		_ = a.Close(cleanup)
		cancel()
		return nil, err
	}
	return a, nil
}

func (a *Adapter) qualify(ctx context.Context, replicaSet string) error {
	ctx, release := a.bindLifetime(ctx)
	defer release()
	cmd := bson.D{{Key: "hello", Value: 1}}
	var hello struct {
		SetName     string `bson:"setName"`
		Msg         string `bson:"msg"`
		MaxDocument int    `bson:"maxBsonObjectSize"`
	}
	if err := a.client.Database("admin").RunCommand(ctx, cmd).Decode(&hello); err != nil {
		return mongoQualificationFailure("MongoDB replica-set qualification failed", err)
	}
	if hello.SetName == "" || hello.Msg == "isdbgrid" || replicaSet != "" && hello.SetName != replicaSet {
		return fmt.Errorf("MongoDB requires a replica set and no mongos")
	}
	a.maxDocumentBytes = hello.MaxDocument
	return nil
}

// The advertised BSON size estimates record output and chooses native command batches.
// A single operation may exceed the exchange target and is sent without splitting.
func (a *Adapter) documentBytes() int {
	if a.maxDocumentBytes > 0 {
		return a.maxDocumentBytes
	}
	return 16 << 20
}

func (a *Adapter) commandBytes() int {
	return min(a.documentBytes(), a.options().ExchangeBytes)
}

func mongoQualificationFailure(message string, err error) error {
	var commandError mongo.CommandError
	if errors.As(err, &commandError) {
		return fmt.Errorf("%s (MongoDB code %d)", message, commandError.Code)
	}
	return fmt.Errorf("%s (%T)", message, err)
}

func (a *Adapter) Close(ctx context.Context) error {
	a.once.Do(func() {
		// Direct callers may have no deadline. Cap their context without
		// extending the absolute retirement deadline supplied by Runtime.
		ctx, cancel := context.WithTimeout(ctx, a.options().CleanupTimeout)
		defer cancel()
		// Canceling the lifetime aborts in-flight commands, whose connections
		// the driver closes. Disconnect then ends idle sessions and the pools.
		a.cancelLifetime()
		if a.client != nil {
			a.closeErr = a.client.Disconnect(ctx)
		}
	})
	return a.closeErr
}

func (a *Adapter) prepareRecord(ctx context.Context, record *execution.Record) (*execution.Plan, *pb.Failure) {
	op := record.Command()
	s := record.Segments()
	if len(s) != 3 || !validNamespace(s) {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid MongoDB record target")
	}
	id, err := parseID(s[2])
	if err != nil {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid record identity")
	}
	target := namespace{database: s[0], collection: s[1]}
	native := &plan{target: target, id: id}
	p := &execution.Plan{Command: op, Key: record.Key(), Backend: native, Bytes: record.Bytes()}
	if r := op.GetRead(); r != nil {
		native.action = "read"
	} else {
		// Both visibility choices use primary reads and majority-acknowledged
		// writes. MongoDB has no separate search refresh stage to wait for.
		m := op.GetMutate()
		var d *pb.Document
		switch v := m.Action.(type) {
		case *pb.MutateRequest_Put:
			native.action = "put"
			d = v.Put
		case *pb.MutateRequest_Create:
			native.action = "create"
			d = v.Create
		case *pb.MutateRequest_Replace:
			native.action = "replace"
			d = v.Replace
		case *pb.MutateRequest_Delete:
			native.action = "delete"
		case *pb.MutateRequest_AtomicTransform:
			if program := v.AtomicTransform.GetLua(); program != nil {
				if program.Input != nil && program.Input.ContentType != "application/bson" {
					return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "MongoDB Lua input must use BSON")
				}

				native.action = "program"
				native.program = &luaengine.Program{}
			} else {
				expression := v.AtomicTransform.GetBackendExpression()
				if expression.ContentType != ExpressionContentType {
					return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "unsupported MongoDB expression profile")
				}
				native.action = "expression"
				native.document = expression.Data
			}
		}
		if d != nil {
			if d.ContentType != "application/bson" {
				return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "only raw BSON is supported")
			}
			if !validBSONDocument(d.Data, 0) {
				return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid BSON document")
			}
			native.document = d.Data
		}
	}
	// BSON filters, model envelopes and write-command overhead fit this conservative charge.
	p.Bytes = record.Bytes()
	return p, nil
}

func parseID(s string) (any, error) {
	if strings.HasPrefix(s, "s:") {
		return s[2:], nil
	}
	if strings.HasPrefix(s, "oid:") {
		id, err := bson.ObjectIDFromHex(s[4:])
		if err == nil && id.Hex() == s[4:] {
			return id, nil
		}
	}
	if strings.HasPrefix(s, "i:") {
		n, err := strconv.ParseInt(s[2:], 10, 64)
		if err == nil && strconv.FormatInt(n, 10) == s[2:] {
			return n, nil
		}
	}
	return nil, fmt.Errorf("invalid ID")
}

func equalID(v value.Value, id any) bool {
	switch i := id.(type) {
	case string:
		return v.Kind == value.String && v.Text == i
	case int64:
		return (v.Kind == value.Int32 || v.Kind == value.Int64) && v.Integer == i
	case bson.ObjectID:
		return v.Kind == value.Extended && v.Type == "mongodb.bson.objectid.v1" && string(v.Data) == string(i[:])
	}
	return false
}

func backendFailure(ctx context.Context, err error) *pb.Failure {
	if ctx.Err() != nil {
		return protocol.ContextFailure(ctx)
	}
	var command mongo.CommandError
	if errors.As(err, &command) {
		code := pb.FailureCode_UNAVAILABLE
		switch command.Code {
		case 18:
			code = pb.FailureCode_UNAUTHENTICATED
		case 13:
			code = pb.FailureCode_PERMISSION_DENIED
		case 26:
			code = pb.FailureCode_TARGET_NOT_FOUND
		}
		if code != pb.FailureCode_UNAVAILABLE {
			return protocol.Fail(code, "backend operation rejected")
		}
	}
	if mongo.IsDuplicateKeyError(err) {
		return protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, "duplicate key")
	}
	// Backend strings may contain user data; never copy them into wire errors.
	return protocol.Fail(pb.FailureCode_UNAVAILABLE, "backend operation failed")
}
