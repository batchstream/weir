package mongodb

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
	"go.mongodb.org/mongo-driver/v2/event"
)

func TestMongoRecordPreflightBeforeAnyCommand(t *testing.T) {
	for _, path := range []string{"db/records/i:01", "bad.db/records/s:bad", "db/records"} {
		t.Run(path, func(t *testing.T) {
			var commands atomic.Int32
			monitor := &event.CommandMonitor{Started: func(context.Context, *event.CommandStartedEvent) { commands.Add(1) }}
			adapter := batchMockAdapter(t, nil, monitor)
			limits := store.DefaultLimits()
			runtime, err := store.New(adapter, limits)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := runtime.Close(ctx); err != nil {
					t.Error(err)
				}
			})
			empty := &pb.Empty{}
			action := &pb.MutateRequest_Delete{Delete: empty}
			mutation := &pb.MutateRequest{Resource: path, Action: action}
			operation := &pb.Command_Mutate{Mutate: mutation}
			command := &pb.Command{Operation: operation}
			record, err := execution.NewRecord("mongo", 1, command)
			if err != nil {
				t.Fatal(err)
			}
			prepared, failure := runtime.PrepareRecord(record)
			if failure == nil || prepared != nil || commands.Load() != 0 {
				t.Fatal("invalid backend input produced a plan or effects", prepared, failure, commands.Load())
			}
			if snapshot := runtime.Snapshot(); snapshot.Active != 0 || snapshot.Pending != 0 || snapshot.Retained != 0 || snapshot.WorkingBytes != 0 {
				t.Fatal("failed preflight reserved runtime resources", snapshot)
			}
		})
	}
}

func TestMongoNamespaceBytePolicy(t *testing.T) {
	for _, name := range []string{"a", "A", "_a", "1a", "a-b", "数据库", strings.Repeat("a", 63)} {
		if !validDatabaseName(name) {
			t.Fatal("valid database", name)
		}
	}
	for _, name := range []string{"", "a.b", "a/", "a\n", strings.Repeat("a", 64)} {
		if validDatabaseName(name) {
			t.Fatal("invalid database", name)
		}
	}
	for _, name := range []string{"a.b", "_records", "1记录", strings.Repeat("c", 252)} {
		if !validNamespace([]string{"db", name}) {
			t.Fatal("valid collection", name)
		}
	}
	for _, name := range []string{"", "system.users", "x.system.users", "a$b", strings.Repeat("c", 253)} {
		if validNamespace([]string{"db", name}) {
			t.Fatal("invalid collection", name)
		}
	}
}
