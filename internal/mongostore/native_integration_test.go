//go:build integration

package mongostore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestMongoNativeRealErrorsBoundsAndReplyLoss(t *testing.T) {
	for _, mode := range []string{"query", "write", "native_error", "multiframe", "oversized", "drop", "reauth", "overload", "response_limit"} {
		t.Run(mode, func(t *testing.T) {
			native, db := testmongo.Open(t)
			proxy := testmongo.StartProxy(t)
			cfg := Config{URI: proxy.URI(), Store: "mongo", Database: db, Collection: "records", Pool: 1}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			a, err := Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			padding := 150 << 10
			if mode == "response_limit" {
				padding = NativeResponseLimit
			}
			doc := bson.D{{Key: "_id", Value: "x"}, {Key: "n", Value: 1}, {Key: "pad", Value: strings.Repeat("x", padding)}}
			if _, err := native.Database(db).Collection("records").InsertOne(ctx, doc); err != nil {
				t.Fatal(err)
			}
			command := bson.D{{Key: "count", Value: "records"}}
			if mode != "query" {
				query := bson.D{{Key: "_id", Value: "x"}}
				increment := bson.D{{Key: "n", Value: 1}}
				update := bson.D{{Key: "$inc", Value: increment}}
				command = bson.D{{Key: "findAndModify", Value: "records"}, {Key: "query", Value: query}, {Key: "update", Value: update}, {Key: "new", Value: true}}
			}
			if mode == "native_error" {
				update := bson.D{{Key: "$notAnOperator", Value: 1}}
				command[2].Value = update
			}
			if mode == "oversized" {
				query := bson.D{{Key: "pad", Value: strings.Repeat("x", NativeCommandLimit)}}
				command[1].Value = query
			}
			if mode == "drop" {
				proxy.DropCommand = "findAndModify"
				proxy.DropRemaining.Store(1)
			}
			if mode == "reauth" || mode == "overload" {
				code := int32(391)
				if mode == "overload" {
					code = 16500
				}
				data := bson.D{{Key: "failCommands", Value: bson.A{"findAndModify"}}, {Key: "errorCode", Value: code}, {Key: "appName", Value: "weir:" + db}}
				testmongo.FailCommand(t, native, data, 1)
			}
			raw, _ := bson.Marshal(command)
			descriptor := &pb.Document{MediaType: NativeDescriptor}
			open := &pb.NativeOpen{Resource: "weir://mongo/" + db + "/records", Descriptor_: descriptor, BodyMediaType: "application/bson"}
			plan, f := a.PrepareNative(open)
			if f != nil {
				t.Fatal(f)
			}
			capture := &nativeCapture{}
			exchange := &execution.NativeExchange{Source: io.NopCloser(bytes.NewReader(raw)), Sink: capture}
			end, _ := a.ExecuteNative(ctx, plan, exchange)
			expected := pb.NativeCompletion_RESPONSE_COMPLETE
			if mode == "drop" || mode == "response_limit" {
				expected = pb.NativeCompletion_RESPONSE_INCOMPLETE
			}
			if mode == "oversized" {
				expected = pb.NativeCompletion_NATIVE_NOT_STARTED
			}
			if end.Completion != expected || (end.Failure == nil) != (expected == pb.NativeCompletion_RESPONSE_COMPLETE) {
				t.Fatal(end)
			}
			count := 0
			for _, event := range proxy.Events() {
				if event.Command == "count" || event.Command == "findAndModify" {
					count++
				}
			}
			expectedCalls := 1
			if mode == "oversized" {
				expectedCalls = 0
			}
			if count != expectedCalls {
				t.Fatal("replayed or sent invalid command", count)
			}
			if expected == pb.NativeCompletion_RESPONSE_COMPLETE {
				for _, event := range proxy.Events() {
					if event.Command == "count" || event.Command == "findAndModify" {
						if event.ReplyDigest != sha256.Sum256(capture.body.Bytes()) {
							t.Fatal("native response changed")
						}
					}
				}
				reply := bson.Raw(capture.body.Bytes())
				if reply.Validate() != nil {
					t.Fatal("invalid raw BSON")
				}
				if mode == "native_error" && scanOK(reply.Lookup("ok")) {
					t.Fatal("lost native error", reply)
				}
				if mode == "multiframe" && capture.chunks < 2 {
					t.Fatal("not chunked")
				}
			}
			if mode == "drop" || mode == "write" {
				filter := bson.D{{Key: "_id", Value: "x"}}
				result := native.Database(db).Collection("records").FindOne(ctx, filter)
				raw, err := result.Raw()
				if err != nil || raw.Lookup("n").Int32() != 2 {
					t.Fatal("write effect absent/replayed", err, raw)
				}
				t.Log("one findAndModify; independent native read n=2", end.Completion)
			}
		})
	}
}
