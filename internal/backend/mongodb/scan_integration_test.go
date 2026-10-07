//go:build integration

package mongodb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

func TestMongoScanPublicationFailureIsTerminal(t *testing.T) {
	backend := testmongo.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	document := bson.D{{Key: "_id", Value: int32(1)}}
	if _, err := backend.Admin.Database(backend.DB).Collection("records").InsertOne(ctx, document); err != nil {
		t.Fatal(err)
	}
	options := adapterTestOptions{fixture: backend}
	adapter := testAdapter(t, options)
	work := scanWork(t, adapter, backend.DB)
	defer adapter.closeScan(ctx, work)
	var end *pb.ScanEnd
	publicationError := errors.New("publication rejected")
	emit := func(_ *execution.Plan, event *pb.Event) error {
		if event.GetDocument() != nil {
			return publicationError
		}
		end = event.GetScanEnd()
		return nil
	}
	continuation := adapter.streamScan(ctx, work, emit)
	if end == nil || end.Failure == nil || end.Failure.Code != pb.FailureCode_INTERNAL || end.DocumentCount != 0 || continuation || end.Exhausted || len(end.NextContinuationToken) != 0 || ctx.Err() != nil {
		t.Fatal("publication failure did not terminate the Scan", end, continuation, ctx.Err())
	}
}

func TestMongoScanLearnsPrefixCapacityWithinEachPage(t *testing.T) {
	backend := testmongo.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	const records = 64
	documents := make([]any, records)
	for id := range documents {
		document := bson.D{{Key: "_id", Value: int32(id)}, {Key: "pad", Value: strings.Repeat("x", 1<<20)}}
		documents[id] = document
	}
	collection := backend.Admin.Database(backend.DB).Collection("records")
	if _, err := collection.InsertMany(ctx, documents); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var limits []int32
	var nativeBytes, nativeRows atomic.Int64
	monitor := &event.CommandMonitor{
		Started: func(_ context.Context, event *event.CommandStartedEvent) {
			if event.CommandName != "find" {
				return
			}
			mu.Lock()
			limits = append(limits, event.Command.Lookup("batchSize").Int32())
			mu.Unlock()
		},
		Succeeded: func(_ context.Context, event *event.CommandSucceededEvent) {
			if event.CommandName != "find" {
				return
			}
			batch := event.Reply.Lookup("cursor", "firstBatch").Array()
			values, err := batch.Values()
			if err != nil {
				t.Error(err)
				return
			}
			nativeRows.Add(int64(len(values)))
			nativeBytes.Add(int64(len(event.Reply)))
		},
	}
	options := adapterTestOptions{fixture: backend, monitor: monitor}
	adapter := testAdapter(t, options)
	request := &pb.ScanRequest{Resource: backend.DB + "/records", PageSize: 16}
	first, firstEnd := mongoFinitePage(t, adapter, request)
	if firstEnd == nil || firstEnd.Failure != nil || firstEnd.Exhausted || len(first) != 16 || len(firstEnd.NextContinuationToken) == 0 {
		t.Fatal("first learned Scan page failed", firstEnd, len(first))
	}
	request.PageSize = 128
	request.ContinuationToken = firstEnd.NextContinuationToken
	work, failure := adapter.prepareScan(request)
	if failure != nil {
		t.Fatal(failure)
	}
	if work.Backend.(*scanPlan).batchSize != execution.ScanBatchDocuments {
		t.Fatal("resume inherited internal batch tuning", work.Backend.(*scanPlan).batchSize)
	}
	if failure := adapter.closeScan(ctx, work); failure != nil {
		t.Fatal(failure)
	}
	second, secondEnd := mongoFinitePage(t, adapter, request)
	if secondEnd == nil || secondEnd.Failure != nil || !secondEnd.Exhausted || len(second) != records-len(first) {
		t.Fatal("resumed learned Scan failed", secondEnd, len(second))
	}
	all := append(first, second...)
	for index, document := range all {
		if bson.Raw(document.Data).Lookup("_id").Int32() != int32(index) || len(bson.Raw(document.Data).Lookup("pad").StringValue()) != 1<<20 {
			t.Fatal("learned Scan skipped, duplicated or changed a record", index)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(limits) != 23 || limits[0] != 16 || limits[5] != 1 || limits[6] != execution.ScanBatchDocuments || nativeRows.Load() > 2*records || nativeBytes.Load() > 128<<20 {
		t.Fatal("Scan repeatedly fetched discarded tails within a page", limits, nativeRows.Load(), nativeBytes.Load())
	}
	t.Logf("records=%d findCommands=%d nativeRows=%d nativeBytes=%d learnedCapacity=3 shortPageTail=1 resumedCapacity=128", records, len(limits), nativeRows.Load(), nativeBytes.Load())
}

func TestMongoScanBatchesHundredsOfRecordsAcrossPages(t *testing.T) {
	backend := testmongo.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	const records = 513
	documents := make([]any, records)
	for id := range documents {
		document := bson.D{{Key: "_id", Value: int32(id)}, {Key: "n", Value: int64(id)}}
		documents[id] = document
	}
	collection := backend.Admin.Database(backend.DB).Collection("records")
	if _, err := collection.InsertMany(ctx, documents); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var commands []bson.Raw
	monitor := &event.CommandMonitor{Started: func(_ context.Context, event *event.CommandStartedEvent) {
		if event.CommandName == "find" || event.CommandName == "getMore" {
			mu.Lock()
			commands = append(commands, append(bson.Raw(nil), event.Command...))
			mu.Unlock()
		}
	}}
	options := adapterTestOptions{fixture: backend, monitor: monitor}
	adapter := testAdapter(t, options)
	request := &pb.ScanRequest{Resource: backend.DB + "/records", PageSize: 256}
	seen := 0
	for pages := 0; ; pages++ {
		if pages > 3 {
			t.Fatal("Scan did not finish")
		}
		page, end := mongoFinitePage(t, adapter, request)
		if end == nil || end.Failure != nil || end.DocumentCount != uint64(len(page)) || len(page) > int(request.PageSize) {
			t.Fatal("Scan page failed", end, len(page))
		}
		for _, document := range page {
			id := bson.Raw(document.Data).Lookup("_id").Int32()
			if id != int32(seen) || bson.Raw(document.Data).Lookup("n").Int64() != int64(seen) {
				t.Fatal("Scan skipped or duplicated a record", id, seen)
			}
			seen++
		}
		if end.Exhausted {
			break
		}
		if len(end.NextContinuationToken) == 0 {
			t.Fatal("Scan did not checkpoint the page")
		}
		request.ContinuationToken = end.NextContinuationToken
	}
	mu.Lock()
	defer mu.Unlock()
	if seen != records || len(commands) != 6 {
		t.Fatal("Scan did not batch database calls", seen, len(commands))
	}
	for index, command := range commands {
		if command.Lookup("find").Type == 0 || command.Lookup("limit").Int64() != execution.ScanBatchDocuments || command.Lookup("batchSize").Int32() != execution.ScanBatchDocuments || !command.Lookup("singleBatch").Boolean() {
			t.Fatal("Scan retained a cursor or fetched individual records", index, command)
		}
	}
	t.Logf("records=%d findCommands=%d getMoreCommands=0 batchSize=%d", seen, len(commands), execution.ScanBatchDocuments)
}

func TestMongoScanLostBatchReplyKeepsLastAcceptedCheckpoint(t *testing.T) {
	backend := testmongo.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const records = 145
	documents := make([]any, records)
	for id := range documents {
		document := bson.D{{Key: "_id", Value: int32(id)}}
		documents[id] = document
	}
	collection := backend.Admin.Database(backend.DB).Collection("records")
	if _, err := collection.InsertMany(ctx, documents); err != nil {
		t.Fatal(err)
	}
	proxy := testmongo.StartProxy(t, backend)
	proxy.DropCommand = "find"
	config := Config{URI: proxy.URI(), Store: "mongo", Pool: 1}
	config = mongoFixtureConfig(t, config)
	adapter, err := Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()
	request := &pb.ScanRequest{Resource: backend.DB + "/records", PageSize: 256}
	work, failure := adapter.prepareScan(request)
	if failure != nil {
		t.Fatal(failure)
	}
	defer adapter.closeScan(ctx, work)
	first := adapter.fetchScan(ctx, work)
	if first.Failure != nil || len(first.Documents) != execution.ScanBatchDocuments {
		t.Fatal("first Scan batch failed", first.Failure, len(first.Documents))
	}
	state := work.Backend.(*scanPlan)
	state.Count += uint64(len(first.Documents))
	checkpoint := append([]byte(nil), state.last...)
	proxy.DropRemaining.Store(1)
	lost := adapter.fetchScan(ctx, work)
	if lost.Failure == nil || len(lost.Documents) != 0 || lost.Exhausted || !bytes.Equal(state.last, checkpoint) {
		t.Fatal("lost Scan reply advanced the checkpoint", lost)
	}
	finds := 0
	for _, observed := range proxy.Events() {
		if observed.Command == "find" {
			finds++
		}
	}
	if finds != 2 {
		t.Fatal("lost batch was automatically replayed", finds)
	}
	recovered := adapter.fetchScan(ctx, work)
	if recovered.Failure != nil || len(recovered.Documents) != records-execution.ScanBatchDocuments {
		t.Fatal("explicit retry did not recover the lost batch", recovered.Failure, len(recovered.Documents))
	}
	for index, document := range recovered.Documents {
		id := bson.Raw(document.Data).Lookup("_id").Int32()
		if id != int32(execution.ScanBatchDocuments+index) {
			t.Fatal("explicit retry skipped or duplicated a record", id, index)
		}
	}
	state.Count += uint64(len(recovered.Documents))
	end := adapter.fetchScan(ctx, work)
	if end.Failure != nil || !end.Exhausted || len(end.Documents) != 0 || state.Count != records {
		t.Fatal("explicit retry did not reach exhaustion", end, state.Count)
	}
}

func scanWork(t *testing.T, a *Adapter, database string) *execution.Plan {
	t.Helper()
	req := &pb.ScanRequest{Resource: database + "/records"}
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
				page := a.fetchScan(ctx, p)
				if page.Failure != nil {
					t.Fatalf("fetch %d: %v", calls, page.Failure)
				}
				if len(page.Documents) > execution.ScanBatchDocuments {
					t.Fatal("scan exceeded document batch bound", len(page.Documents))
				}
				for _, doc := range page.Documents {
					id := bson.Raw(doc.Data).Lookup("_id").Int32()
					if seen[id] || !bytes.Equal(doc.Data, expected[id]) {
						t.Fatal("BSON fidelity/duplicate", id)
					}
					seen[id] = true
				}
				p.Backend.(*scanPlan).Count += uint64(len(page.Documents))
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

func TestMongoScanFaultPagesAndNoAutomaticRetry(t *testing.T) {
	for _, stage := range []string{"first", "next"} {
		for _, mode := range []string{"partial", "partial_top", "missing_batch", "truncate", "drop"} {
			t.Run(stage+"_"+mode, func(t *testing.T) {
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
					proxy.DropCommand = "find"
					proxy.DropRemaining.Store(0)
				} else {
					proxy.AlterCommand = "find"
					proxy.AlterMode = mode
					proxy.AlterRemaining.Store(0)
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
				if stage == "next" {
					page := a.fetchScan(ctx, p)
					if page.Failure != nil || len(page.Documents) != 4 {
						t.Fatal(page)
					}
				}
				if mode == "drop" {
					proxy.DropRemaining.Store(1)
				} else {
					proxy.AlterRemaining.Store(1)
				}
				page := a.fetchScan(ctx, p)
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
				wantFinds := 1
				if stage == "next" {
					wantFinds = 2
				}
				if finds != wantFinds || gets != 0 {
					t.Fatal("restarted traversal", finds, gets)
				}
			})
		}
	}
}
func TestMongoScanFetchCancellation(t *testing.T) {
	for _, mode := range []string{"cancel_first", "cancel_next"} {
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
			if mode == "cancel_next" {
				page := a.fetchScan(ctx, p)
				if page.Failure != nil || len(page.Documents) != 4 {
					t.Fatal(page)
				}
			}
			data := bson.D{{Key: "failCommands", Value: bson.A{"find"}}, {Key: "appName", Value: "weir:mongo"}, {Key: "blockConnection", Value: true}, {Key: "blockTimeMS", Value: 200}}
			testmongo.FailCommand(t, native, data, 1)
			attempt, stop := context.WithTimeout(ctx, 40*time.Millisecond)
			defer stop()

			start := time.Now()
			page := a.fetchScan(attempt, p)
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
			var finds atomic.Int64
			monitor := &event.CommandMonitor{Succeeded: func(_ context.Context, event *event.CommandSucceededEvent) {
				if event.CommandName == "find" || event.CommandName == "getMore" {
					finds.Add(1)
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
			totalBytes, largestOutput := 0, 0
			for fetch := 0; ; fetch++ {
				if fetch > count {
					t.Fatal("bounded scan did not produce exhaustion evidence")
				}
				page := adapter.fetchScan(ctx, work)
				if page.Failure != nil || len(page.Documents) > execution.ScanBatchDocuments {
					t.Fatal("bounded page failed", page.Failure, len(page.Documents))
				}
				pageBytes := 0
				for _, document := range page.Documents {
					id := bson.Raw(document.Data).Lookup("_id").Int32()
					if seen[id] || !bytes.Equal(document.Data, expected[id]) {
						t.Fatal("scan lost BSON fidelity or duplicated a record", id)
					}
					seen[id] = true
					totalBytes += len(document.Data)
					pageBytes += len(document.Data)
				}
				largestOutput = max(largestOutput, pageBytes)
				if pageBytes > execution.ScanBatchBytes {
					t.Fatal("Scan copied more than its output prefix budget", pageBytes)
				}
				work.Backend.(*scanPlan).Count += uint64(len(page.Documents))
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
			if mode == "large_total_scan" && (totalBytes <= 8<<20 || largest.Load() <= execution.ScanBatchBytes || finds.Load() != 4) {
				t.Fatal("large Scan did not reread a bounded prefix without skipping its tail", totalBytes, largest.Load(), finds.Load())
			}
			if failure := adapter.closeScan(ctx, work); failure != nil {
				t.Fatal("cursor cleanup failed", failure)
			}
			t.Logf("complete scan records=%d outputBytes=%d largestOutput=%d finds=%d largestNativeReply=%d wireLimit=%d reservedWorking=%d", len(seen), totalBytes, largestOutput, finds.Load(), largest.Load(), scanNativeLimit, work.WorkingBytes)
		})
	}
}
