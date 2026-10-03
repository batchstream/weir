// Package mongodb owns MongoDB encoding, clients, sessions and execution evidence.
package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend/targetcache"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/luaengine"
	"github.com/batchstream/weir/internal/value"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

type Config struct {
	URI      string
	Store    string
	Username string
	Password string
	Pool     uint64
	// MaxReadSize bounds ordinary Record Read only; zero uses the 16 KiB default.
	MaxReadSize int
}

type Adapter struct {
	dialer   *boundedDialer
	client   *mongo.Client
	config   Config
	once     sync.Once
	closeErr error
	targets  targetcache.Cache[namespace, struct{}]
}

type plan struct {
	target   namespace
	id       any
	document bson.Raw
	action   string
	program  *luaengine.Program
}

func Open(ctx context.Context, cfg Config) (*Adapter, error) {
	if err := ValidateConfig(cfg); err != nil {
		return nil, err
	}
	if cfg.MaxReadSize == 0 {
		cfg.MaxReadSize = execution.DefaultMaxReadSize
	}
	dialer := newBoundedDialer(int(cfg.Pool)+1, mongoMaxConnecting+1)
	complete := false
	defer func() {
		if !complete {
			dialer.close()
		}
	}()
	opts, err := connectionOptions(cfg, dialer)
	if err != nil {
		return nil, err
	}
	opts.SetDirect(true).
		SetAppName("weir:" + cfg.Store).
		SetMaxPoolSize(cfg.Pool).
		SetMinPoolSize(0).
		SetMaxConnecting(mongoMaxConnecting).
		SetRetryWrites(false).
		SetRetryReads(false).
		SetMaxAdaptiveRetries(0).
		SetEnableOverloadRetargeting(false).
		SetCompressors(nil).
		SetServerMonitoringMode(options.ServerMonitoringModePoll).
		SetServerSelectionTimeout(2 * time.Second).
		SetConnectTimeout(2 * time.Second).
		SetReadPreference(readpref.Primary()).
		SetWriteConcern(writeconcern.Majority())
	if opts.Timeout != nil {
		return nil, fmt.Errorf("client timeoutMS is unsupported; runtime owns execution deadlines")
	}
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("invalid MongoDB connection profile")
	}
	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, fmt.Errorf("MongoDB client configuration rejected")
	}
	a := &Adapter{
		dialer: dialer,
		client: client,
		config: cfg,
	}
	if err = a.qualify(ctx); err != nil {
		_ = a.Close()
		return nil, err
	}
	complete = true
	return a, nil
}

func (a *Adapter) qualify(ctx context.Context) error {
	cmd := bson.D{{Key: "hello", Value: 1}}
	var hello struct {
		SetName    string `bson:"setName"`
		Msg        string `bson:"msg"`
		MaxMessage int    `bson:"maxMessageSizeBytes"`
	}
	if err := a.client.Database("admin").RunCommand(ctx, cmd).Decode(&hello); err != nil {
		return mongoQualificationFailure("MongoDB replica-set qualification failed", err)
	}
	if hello.SetName == "" || hello.Msg == "isdbgrid" || hello.MaxMessage > 48<<20 {
		return fmt.Errorf("MongoDB requires a replica set, no mongos, and bounded native messages")
	}

	return nil
}

func mongoQualificationFailure(message string, err error) error {
	var commandError mongo.CommandError
	if errors.As(err, &commandError) {
		return fmt.Errorf("%s (MongoDB code %d)", message, commandError.Code)
	}
	return fmt.Errorf("%s (%T)", message, err)
}

func (a *Adapter) Close() error {
	a.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if a.dialer != nil {
			a.dialer.stop()
		}
		a.closeErr = a.client.Disconnect(ctx)
		if a.dialer != nil {
			a.dialer.close()
			a.logConnections()
		}
	})
	return a.closeErr
}

func (a *Adapter) prepareRecord(record *execution.Record) (*execution.Plan, *pb.Failure) {
	op := record.Operation()
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
	p := &execution.Plan{Operation: op, Key: record.Key(), Backend: native, ResultBytes: protocol.ResultOverhead}
	if r := op.GetRead(); r != nil {
		if r.AdapterOptions != nil || r.ReadMediaType != "" && r.ReadMediaType != "application/bson" {
			return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "read representation/options unsupported")
		}
		native.action = "read"
		p.ResultBytes += a.maxReadSize()
	} else {
		m := op.GetMutate()
		if m.AdapterOptions != nil {
			return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "adapter options unsupported")
		}
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
			if program := v.AtomicTransform.GetProgram(); program != nil {
				if program.Input != nil && program.Input.MediaType != "application/bson" {
					return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "MongoDB Lua input must use BSON")
				}
				input := value.Value{Kind: value.Missing}
				if program.Input != nil {
					input, err = Decode(program.Input.Data)
					if err != nil {
						return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid BSON transform input")
					}
				}
				native.action = "program"
				luaProgram := &luaengine.Program{Source: string(program.Source), Input: input}
				native.program = luaProgram
			} else {
				expression := v.AtomicTransform.GetBackendExpression()
				if f := a.prepareExpression(expression); f != nil {
					return nil, f
				}
				native.action = "expression"
				native.document = expression.Data
			}
		}
		if d != nil {
			if d.MediaType != "application/bson" {
				return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "only raw BSON is supported")
			}
			doc, err := Decode(d.Data)
			if err != nil {
				return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "BSON validation/limits failed")
			}
			got, err := doc.Lookup("_id")
			if err != nil || len(doc.Fields) == 0 || doc.Fields[0].Name != "_id" || !equalID(got, id) {
				return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "explicit first _id must match resource")
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
	if mongo.IsDuplicateKeyError(err) {
		return protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, "duplicate key")
	}
	// Backend strings may contain user data; never copy them into wire errors.
	return protocol.Fail(pb.FailureCode_UNAVAILABLE, "backend operation failed")
}

func feedback(ctx context.Context, err error) execution.Feedback {
	if err == nil || errors.Is(err, mongo.ErrNoDocuments) {
		return execution.Healthy
	}
	if ctx.Err() != nil {
		return execution.Neutral
	}
	var ce mongo.CommandError
	if errors.As(err, &ce) && (ce.Code == 91 || ce.Code == 189 || ce.Code == 16500) {
		return execution.Congested
	}
	// Only Runtime knows whether a deadline belongs to its backend cap or a caller.
	if mongo.IsTimeout(err) {
		return execution.Neutral
	}
	if mongo.IsNetworkError(err) {
		return execution.Congested
	}
	return execution.Neutral
}
