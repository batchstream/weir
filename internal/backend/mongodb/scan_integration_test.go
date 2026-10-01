//go:build integration

package mongodb

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
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

func scanWork(t *testing.T, a *Adapter, database string) *execution.Plan {
	t.Helper()
	req := &pb.ScanRequest{Resource: "weir://mongo/" + database + "/records"}
	p, f := a.prepareScan(req)
	if f != nil {
		t.Fatal(f)
	}
	return p
}
func TestMongoScanTraversal(t *testing.T) {
	for _, size := range []int{0, 1, 8, 35} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			backend := testmongo.Open(t)
			native, db := backend.Admin, backend.DB
			cfg := Config{URI: backend.URI, Store: "mongo", Pool: 1}
			cfg = mongoFixtureConfig(t, cfg)
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
			p := scanWork(t, a, db)
			defer a.closeScan(ctx, p)
			seen := map[int32]bool{}
			for calls := 0; ; calls++ {
				if calls > size+1 {
					t.Fatal("did not exhaust")
				}
				page, _ := a.fetchScan(ctx, p)
				if page.Failure != nil {
					t.Fatalf("fetch %d: %v", calls, page.Failure)
				}
				if len(page.Documents) > 1 {
					t.Fatal("scan exceeded one-document page bound", len(page.Documents))
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
			if f := a.closeScan(ctx, p); f != nil {
				t.Fatal(f)
			}
		})
	}
}

func TestMongoScanFaultPagesAndNoRestart(t *testing.T) {
	for _, command := range []string{"find", "getMore"} {
		for _, mode := range []string{"partial", "partial_top", "missing_batch", "truncate", "drop"} {
			t.Run(command+"_"+mode, func(t *testing.T) {
				backend := testmongo.Open(t)
				native, db := backend.Admin, backend.DB
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				for i := 0; i < 4; i++ {
					doc := bson.D{{Key: "_id", Value: i}}
					if _, err := native.Database(db).Collection("records").InsertOne(ctx, doc); err != nil {
						t.Fatal(err)
					}
				}
				proxy := testmongo.StartProxy(t, backend)
				if mode == "drop" {
					proxy.DropCommand = command
					proxy.DropRemaining.Store(1)
				} else {
					proxy.AlterCommand = command
					proxy.AlterMode = mode
					proxy.AlterRemaining.Store(1)
				}
				cfg := Config{URI: proxy.URI(), Store: "mongo", Pool: 1}
				cfg = mongoFixtureConfig(t, cfg)
				a, err := Open(ctx, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer a.Close()
				p := scanWork(t, a, db)
				defer a.closeScan(ctx, p)
				if command == "getMore" {
					page, _ := a.fetchScan(ctx, p)
					if page.Failure != nil || len(page.Documents) != 1 {
						t.Fatal(page)
					}
				}
				page, _ := a.fetchScan(ctx, p)
				if page.Failure == nil || len(page.Documents) != 0 || page.Exhausted {
					t.Fatal("failed page exposed documents", page)
				}
				_ = a.closeScan(ctx, p)
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
			backend := testmongo.Open(t)
			native, db := backend.Admin, backend.DB
			cfg := Config{URI: backend.URI, Store: "mongo", Pool: 1}
			cfg = mongoFixtureConfig(t, cfg)
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
			p := scanWork(t, a, db)
			defer a.closeScan(ctx, p)
			if mode != "cancel_find" {
				page, _ := a.fetchScan(ctx, p)
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
				data := bson.D{{Key: "failCommands", Value: bson.A{command}}, {Key: "appName", Value: "weir:mongo"}, {Key: "blockConnection", Value: true}, {Key: "blockTimeMS", Value: 200}}
				testmongo.FailCommand(t, native, data, 1)
				var stop context.CancelFunc
				ctx, stop = context.WithTimeout(ctx, 40*time.Millisecond)
				defer stop()
			}
			start := time.Now()
			page, _ := a.fetchScan(ctx, p)
			if page.Failure == nil || len(page.Documents) != 0 || time.Since(start) > 500*time.Millisecond {
				t.Fatal("cursor fault/cancel", page, time.Since(start))
			}
			cleanup, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			_ = a.closeScan(cleanup, p)
		})
	}
}

func TestMongoScanNativeBatchBudgetAndOutputBoundary(t *testing.T) {
	for _, mode := range []string{"boundary", "large_total_scan"} {
		t.Run(mode, func(t *testing.T) {
			backend := testmongo.Open(t)
			native, db := backend.Admin, backend.DB
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			document := bson.D{{Key: "_id", Value: int32(0)}, {Key: "pad", Value: ""}}
			raw, _ := bson.Marshal(document)
			pad, count := protocol.MaxDocument-len(raw), 1
			if mode == "large_total_scan" {
				pad = 300 << 10
				count = 32
			}
			expected := make(map[int32][]byte, count)
			for i := 0; i < count; i++ {
				document[0].Value = int32(i)
				document[1].Value = strings.Repeat("x", pad)
				raw, err := bson.Marshal(document)
				if err != nil {
					t.Fatal(err)
				}
				expected[int32(i)] = raw
				if _, err := native.Database(db).Collection("records").InsertOne(ctx, document); err != nil {
					t.Fatal(err)
				}
			}
			var largest atomic.Int64
			monitor := &event.CommandMonitor{Succeeded: func(_ context.Context, event *event.CommandSucceededEvent) {
				if event.CommandName == "find" || event.CommandName == "getMore" {
					size := int64(len(event.Reply))
					for previous := largest.Load(); size > previous; previous = largest.Load() {
						if largest.CompareAndSwap(previous, size) {
							break
						}
					}
				}
			}}
			options := adapterTestOptions{fixture: backend, monitor: monitor}
			adapter := testAdapter(t, options)
			work := scanWork(t, adapter, db)
			defer adapter.closeScan(ctx, work)
			seen := make(map[int32]bool, count)
			totalBytes := 0
			for fetch := 0; ; fetch++ {
				if fetch > count {
					t.Fatal("bounded scan did not produce exhaustion evidence")
				}
				page, _ := adapter.fetchScan(ctx, work)
				if page.Failure != nil || len(page.Documents) > 1 {
					t.Fatal("bounded page failed", page.Failure, len(page.Documents))
				}
				for _, document := range page.Documents {
					id := bson.Raw(document.Data).Lookup("_id").Int32()
					if seen[id] || !bytes.Equal(document.Data, expected[id]) {
						t.Fatal("scan lost BSON fidelity or duplicated a record", id)
					}
					seen[id] = true
					totalBytes += len(document.Data)
				}
				if page.Exhausted {
					break
				}
			}
			if len(seen) != count || largest.Load() <= 0 || largest.Load() > scanNativeLimit {
				t.Fatal("scan omitted data or exceeded native response bound", len(seen), count, largest.Load())
			}
			if mode == "boundary" && totalBytes != protocol.MaxDocument {
				t.Fatal("exact legal record boundary was not read", totalBytes)
			}
			if mode == "large_total_scan" && (totalBytes <= 8<<20 || largest.Load() > 400<<10) {
				t.Fatal("total scan was retained in a native page", totalBytes, largest.Load())
			}
			if failure := adapter.closeScan(ctx, work); failure != nil {
				t.Fatal("cursor cleanup failed", failure)
			}
			t.Logf("complete scan records=%d outputBytes=%d largestNativeReply=%d wireLimit=%d reservedWorking=%d", len(seen), totalBytes, largest.Load(), scanNativeLimit, work.WorkingBytes)
		})
	}
}
