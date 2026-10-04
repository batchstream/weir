package mongodb

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/drivertest"
)

type nativeCapture struct {
	head              *pb.NativeHead
	body              bytes.Buffer
	chunks            int
	headErr, chunkErr error
}

func (c *nativeCapture) Head(h *pb.NativeHead) error {
	if c.headErr != nil {
		return c.headErr
	}
	c.head = h
	return nil
}
func (c *nativeCapture) Chunk(b []byte) error {
	if c.chunkErr != nil {
		return c.chunkErr
	}
	c.chunks++
	_, err := c.body.Write(b)
	return err
}
func (c *nativeCapture) Emit(_ *execution.Plan, event *pb.Event) error {
	if head := event.GetHead(); head != nil {
		return c.Head(head)
	}
	return c.Chunk(event.GetChunk())
}

func TestMongoNativeExplicitCongestion(t *testing.T) {
	for _, test := range []struct {
		name             string
		code             int32
		failHead         bool
		failChunk        bool
		oversized        bool
		cancelAfterReply bool
		invalidOK        bool
		completion       pb.NativeCompletion
		feedback         execution.Feedback
	}{
		{name: "shutdown", code: 91, completion: pb.NativeCompletion_RESPONSE_COMPLETE, feedback: execution.Congested},
		{name: "stepdown", code: 189, completion: pb.NativeCompletion_RESPONSE_COMPLETE, feedback: execution.Congested},
		{name: "capacity", code: 16500, completion: pb.NativeCompletion_RESPONSE_COMPLETE, feedback: execution.Congested},
		{name: "deterministic_error", code: 2, completion: pb.NativeCompletion_RESPONSE_COMPLETE, feedback: execution.Neutral},
		{name: "success", completion: pb.NativeCompletion_RESPONSE_COMPLETE, feedback: execution.Completed},
		{name: "canceled_after_reply", cancelAfterReply: true, completion: pb.NativeCompletion_RESPONSE_COMPLETE, feedback: execution.Neutral},
		{name: "invalid_ok", invalidOK: true, completion: pb.NativeCompletion_RESPONSE_INCOMPLETE, feedback: execution.Neutral},
		{name: "success_head_failure", failHead: true, completion: pb.NativeCompletion_RESPONSE_INCOMPLETE, feedback: execution.Neutral},
		{name: "success_chunk_failure", failChunk: true, completion: pb.NativeCompletion_RESPONSE_INCOMPLETE, feedback: execution.Neutral},
		{name: "head_failure", code: 16500, failHead: true, completion: pb.NativeCompletion_RESPONSE_INCOMPLETE, feedback: execution.Neutral},
		{name: "chunk_failure", code: 16500, failChunk: true, completion: pb.NativeCompletion_RESPONSE_INCOMPLETE, feedback: execution.Neutral},
		{name: "oversized", code: 16500, oversized: true, completion: pb.NativeCompletion_RESPONSE_INCOMPLETE, feedback: execution.Neutral},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 0}}
			if test.invalidOK {
				response[0].Value = int32(2)
			}
			if test.code != 0 {
				message := "native error"
				if test.oversized {
					message = strings.Repeat("x", NativeResponseLimit)
				}
				response = bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: test.code}, {Key: "errmsg", Value: message}}
			}
			var calls int
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			monitor := &event.CommandMonitor{
				Started: func(_ context.Context, _ *event.CommandStartedEvent) { calls++ },
				Succeeded: func(_ context.Context, e *event.CommandSucceededEvent) {
					if test.cancelAfterReply && e.CommandName == "count" {
						cancel()
					}
				},
			}
			qualification := collectionQualificationResponse("db", "records")
			deployment := drivertest.NewMockDeployment(qualification, response)
			opts := options.Client().SetRetryWrites(false).SetRetryReads(false).SetMaxAdaptiveRetries(0).SetMonitor(monitor)
			opts.Deployment = deployment
			client, err := mongo.Connect(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Disconnect(context.Background())
			cfg := Config{Store: "mongo"}
			a := &Adapter{client: client, config: cfg}
			nativeBody := &pb.Document{ContentType: "application/bson", Data: []byte{5, 0, 0, 0, 0}}
			open := &pb.NativeRequest{Resource: "db/records", Request: nativeBody}
			plan, failure := a.prepareNative(open)
			if failure != nil {
				t.Fatal(failure)
			}
			capture := &nativeCapture{}
			if test.failHead {
				capture.headErr = errors.New("output unavailable")
			}
			if test.failChunk {
				capture.chunkErr = errors.New("output unavailable")
			}
			command := bson.D{{Key: "count", Value: "records"}}
			raw, err := bson.Marshal(command)
			if err != nil {
				t.Fatal(err)
			}
			open.Request.Data = raw
			plan.Command = testutil.NativeCommand(open)
			end, feedback := a.executeNative(ctx, plan, capture.Emit)
			if end.Completion != test.completion || feedback != test.feedback || calls != 2 {
				t.Fatal(end, feedback, calls)
			}
			if test.completion == pb.NativeCompletion_RESPONSE_COMPLETE {
				want, err := bson.Marshal(response)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(capture.body.Bytes(), want) || capture.head.BodyContentType != "application/bson" || end.Failure != nil {
					t.Fatal("native reply changed", bson.Raw(capture.body.Bytes()), end)
				}
			}
		})
	}
}
func TestMongoNativeCommandScope(t *testing.T) {
	cfg := Config{Store: "mongo"}
	a := &Adapter{config: cfg}
	target := namespace{database: "db", collection: "records"}
	for _, name := range []string{"find", "aggregate", "getMore", "killCursors", "drop", "insert", "eval", "startSession"} {
		command := bson.D{{Key: name, Value: "records"}}
		raw, _ := bson.Marshal(command)
		if a.nativeCommand(raw, target) == nil {
			t.Fatal(name)
		}
	}
	for _, key := range []string{"$db", "lsid", "txnNumber", "autocommit", "startTransaction", "writeConcern", "readConcern", "pipeline", "let"} {
		command := bson.D{{Key: "findAndModify", Value: "records"}, {Key: key, Value: 1}}
		raw, _ := bson.Marshal(command)
		if a.nativeCommand(raw, target) == nil {
			t.Fatal(key)
		}
	}
	for _, command := range []bson.D{
		{{Key: "count", Value: "other"}},
		{{Key: "findAndModify", Value: "records"}, {Key: "update", Value: bson.A{}}},
		{{Key: "count", Value: "records"}, {Key: "count", Value: "other"}},
	} {
		raw, _ := bson.Marshal(command)
		if a.nativeCommand(raw, target) == nil {
			t.Fatal(command)
		}
	}
}

func TestMongoNativeRejectsCode(t *testing.T) {
	cfg := Config{}
	a := &Adapter{config: cfg}
	target := namespace{database: "db", collection: "records"}
	for _, key := range []string{"$where", "$function", "$accumulator"} {
		query := bson.D{{Key: key, Value: "code"}}
		command := bson.D{{Key: "count", Value: "records"}, {Key: "query", Value: query}}
		raw, _ := bson.Marshal(command)
		if a.nativeCommand(raw, target) == nil {
			t.Fatal(key)
		}
	}
}

func TestMongoNativeExactCommandBound(t *testing.T) {
	cfg := Config{}
	a := &Adapter{config: cfg}
	target := namespace{database: "db", collection: "records"}
	query := bson.D{{Key: "pad", Value: ""}}
	command := bson.D{{Key: "count", Value: "records"}, {Key: "query", Value: query}}
	base, _ := bson.Marshal(command)
	for _, extra := range []int{0, 1} {
		query[0].Value = strings.Repeat("x", NativeCommandLimit-len(base)+extra)
		raw, _ := bson.Marshal(command)
		f := a.nativeCommand(raw, target)
		if extra == 0 && f != nil || extra == 1 && f == nil {
			t.Fatal(extra, len(raw), f)
		}
	}
}

func TestMongoNativeAdapterOwnsRequestFormat(t *testing.T) {
	adapter := &Adapter{}
	for _, contentType := range []string{"application/http", "application/vnd.future.store"} {
		document := &pb.Document{ContentType: contentType, Data: []byte("GET /_doc/x HTTP/1.1\r\n\r\n")}
		request := &pb.NativeRequest{Resource: "db/records", Request: document}
		if _, failure := adapter.prepareNative(request); failure.GetCode() != pb.FailureCode_UNSUPPORTED {
			t.Fatal("shared envelope interpreted adapter payload", contentType, failure)
		}
	}
	// An empty opaque document is a valid envelope, but an invalid BSON command.
	document := &pb.Document{ContentType: "application/bson"}
	request := &pb.NativeRequest{Resource: "db/records", Request: document}
	work, failure := adapter.prepareNative(request)
	if failure != nil {
		t.Fatal("generic envelope unexpectedly requires a body", failure)
	}
	work.Command = testutil.NativeCommand(request)
	capture := &nativeCapture{}
	end, feedback := adapter.executeNative(context.Background(), work, capture.Emit)
	if end.Completion != pb.NativeCompletion_NATIVE_NOT_STARTED || end.GetFailure().GetCode() != pb.FailureCode_INVALID_ARGUMENT || feedback != execution.Neutral || capture.head != nil || capture.body.Len() != 0 {
		t.Fatal("invalid BSON reached backend", end, feedback)
	}
}
