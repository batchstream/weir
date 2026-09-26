//go:build integration

package mongostore

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protocol"
	"github.com/batchstream/weir/internal/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

func scanWork(t *testing.T, a *Adapter, hint uint32) *execution.Plan {
	t.Helper()
	req := &pb.ScanRequest{Resource: "weir://mongo/" + a.config.Database + "/records", FetchItemsHint: hint}
	p, f := a.PrepareScan(req)
	if f != nil {
		t.Fatal(f)
	}
	return p
}
func TestMongoScanTraversal(t *testing.T) {
	for _, size := range []int{0, 1, 8, 35} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			native, db := testmongo.Open(t)
			cfg := Config{URI: testmongo.URI, Store: "mongo", Database: db, Collection: "records", Pool: 1}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			a, err := Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			expected := map[int32][]byte{}
			for i := 0; i < size; i++ {
				decimal, _ := bson.ParseDecimal128("12345.6789")
				doc := bson.D{{Key: "_id", Value: int32(i)}, {Key: "n", Value: int64(9223372036854775807)}, {Key: "oid", Value: bson.NewObjectID()}, {Key: "decimal", Value: decimal}, {Key: "binary", Value: bson.Binary{Subtype: 0x80, Data: []byte{1, 2, 3}}}, {Key: "time", Value: bson.DateTime(12345)}}
				raw, err := bson.Marshal(doc)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := native.Database(db).Collection("records").InsertOne(ctx, doc); err != nil {
					t.Fatal(err)
				}
				expected[int32(i)] = raw
			}
			p := scanWork(t, a, 8)
			defer a.CloseScan(ctx, p)
			seen := map[int32]bool{}
			for calls := 0; ; calls++ {
				if calls > 20 {
					t.Fatal("did not exhaust")
				}
				page, _ := a.FetchScan(ctx, p)
				if page.Failure != nil {
					t.Fatalf("fetch %d: %v", calls, page.Failure)
				}
				for _, doc := range page.Documents {
					id := bson.Raw(doc.Data).Lookup("_id").Int32()
					if seen[id] || !bytes.Equal(doc.Data, expected[id]) {
						t.Fatal("BSON fidelity/duplicate", id)
					}
					seen[id] = true
				}
				if page.Exhausted {
					break
				}
			}
			if len(seen) != size {
				t.Fatal("omissions", len(seen), size)
			}
			if f := a.CloseScan(ctx, p); f != nil {
				t.Fatal(f)
			}
		})
	}
}

func TestMongoScanFaultPagesAndNoRestart(t *testing.T) {
	for _, command := range []string{"find", "getMore"} {
		for _, mode := range []string{"partial", "partial_top", "missing_batch", "truncate", "drop"} {
			t.Run(command+"_"+mode, func(t *testing.T) {
				native, db := testmongo.Open(t)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				for i := 0; i < 4; i++ {
					doc := bson.D{{Key: "_id", Value: i}}
					if _, err := native.Database(db).Collection("records").InsertOne(ctx, doc); err != nil {
						t.Fatal(err)
					}
				}
				proxy := testmongo.StartProxy(t)
				if mode == "drop" {
					proxy.DropCommand = command
					proxy.DropRemaining.Store(1)
				} else {
					proxy.AlterCommand = command
					proxy.AlterMode = mode
					proxy.AlterRemaining.Store(1)
				}
				cfg := Config{URI: proxy.URI(), Store: "mongo", Database: db, Collection: "records", Pool: 1}
				a, err := Open(ctx, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer a.Close()
				p := scanWork(t, a, 1)
				defer a.CloseScan(ctx, p)
				if command == "getMore" {
					page, _ := a.FetchScan(ctx, p)
					if page.Failure != nil || len(page.Documents) != 1 {
						t.Fatal(page)
					}
				}
				page, _ := a.FetchScan(ctx, p)
				if page.Failure == nil || len(page.Documents) != 0 || page.Exhausted {
					t.Fatal("failed page exposed documents", page)
				}
				_ = a.CloseScan(ctx, p)
				finds, gets := 0, 0
				for _, event := range proxy.Events() {
					if event.Command == "find" {
						finds++
					}
					if event.Command == "getMore" {
						gets++
					}
				}
				if finds != 1 || command == "getMore" && gets != 1 || command == "find" && gets != 0 {
					t.Fatal("restarted traversal", finds, gets)
				}
			})
		}
	}
}
func TestMongoScanCursorKilledAndFetchCancellation(t *testing.T) {
	for _, mode := range []string{"killed", "cancel_find", "cancel_getMore"} {
		t.Run(mode, func(t *testing.T) {
			native, db := testmongo.Open(t)
			cfg := Config{URI: testmongo.URI, Store: "mongo", Database: db, Collection: "records", Pool: 1}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			a, err := Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			for i := 0; i < 4; i++ {
				doc := bson.D{{Key: "_id", Value: i}}
				if _, err := native.Database(db).Collection("records").InsertOne(ctx, doc); err != nil {
					t.Fatal(err)
				}
			}
			p := scanWork(t, a, 1)
			defer a.CloseScan(ctx, p)
			if mode != "cancel_find" {
				page, _ := a.FetchScan(ctx, p)
				if page.Failure != nil || len(page.Documents) != 1 {
					t.Fatal(page)
				}
			}
			if mode == "killed" {
				n := p.Backend.(*scanPlan)
				command := bson.D{{Key: "killCursors", Value: "records"}, {Key: "cursors", Value: bson.A{n.cursor}}}
				if err := native.Database(db).RunCommand(ctx, command).Err(); err != nil {
					t.Fatal(err)
				}
			} else {
				command := "find"
				if mode == "cancel_getMore" {
					command = "getMore"
				}
				data := bson.D{{Key: "failCommands", Value: bson.A{command}}, {Key: "appName", Value: "weir:" + db}, {Key: "blockConnection", Value: true}, {Key: "blockTimeMS", Value: 200}}
				testmongo.FailCommand(t, native, data, 1)
				var stop context.CancelFunc
				ctx, stop = context.WithTimeout(ctx, 40*time.Millisecond)
				defer stop()
			}
			start := time.Now()
			page, _ := a.FetchScan(ctx, p)
			if page.Failure == nil || len(page.Documents) != 0 || time.Since(start) > 500*time.Millisecond {
				t.Fatal("cursor fault/cancel", page, time.Since(start))
			}
			cleanup, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			_ = a.CloseScan(cleanup, p)
		})
	}
}

func TestMongoScanNativeBatchBudgetAndOutputBoundary(t *testing.T) {
	for _, mode := range []string{"boundary", "large_native_batch"} {
		t.Run(mode, func(t *testing.T) {
			native, db := testmongo.Open(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			doc := bson.D{{Key: "_id", Value: int32(0)}, {Key: "pad", Value: ""}}
			raw, _ := bson.Marshal(doc)
			pad := protocol.MaxDocument - len(raw)
			count := 1
			if mode == "large_native_batch" {
				pad = 300 << 10
				count = 32
			}
			for i := 0; i < count; i++ {
				doc[0].Value = int32(i)
				doc[1].Value = strings.Repeat("x", pad)
				if _, err := native.Database(db).Collection("records").InsertOne(ctx, doc); err != nil {
					t.Fatal(err)
				}
			}
			var largest atomic.Int64
			monitor := &event.CommandMonitor{Succeeded: func(_ context.Context, e *event.CommandSucceededEvent) {
				if e.CommandName == "find" {
					largest.Store(int64(len(e.Reply)))
				}
			}}
			options := adapterTestOptions{database: db, monitor: monitor}
			a := testAdapter(t, options)
			p := scanWork(t, a, 32)
			defer a.CloseScan(ctx, p)
			page, _ := a.FetchScan(ctx, p)
			if mode == "boundary" {
				if page.Failure != nil || len(page.Documents) != 1 || len(page.Documents[0].Data) != protocol.MaxDocument {
					t.Fatal("exact byte boundary", page.Failure)
				}
			} else {
				if page.Failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED || len(page.Documents) != 0 || largest.Load() < 8<<20 || largest.Load() > scanNativeLimit {
					t.Fatal("native working-set evidence", page.Failure, largest.Load())
				}
				t.Logf("native driver batch=%d bytes, public document cap=%d, reserved native/decoder allowance=%d", largest.Load(), protocol.MaxDocument, p.PageBytes)
			}
		})
	}
}
