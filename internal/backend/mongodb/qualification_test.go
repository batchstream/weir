package mongodb

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

func TestMongoServerQualificationUsesTopologyOnly(t *testing.T) {
	tests := []struct {
		name     string
		response bson.D
		wantErr  string
	}{
		{
			name: "replica set without version metadata",
			response: bson.D{
				{Key: "ok", Value: 1},
				{Key: "setName", Value: "weir_test"},
				{Key: "maxMessageSizeBytes", Value: 48 << 20},
			},
		},
		{
			name: "standalone",
			response: bson.D{
				{Key: "ok", Value: 1},
				{Key: "maxMessageSizeBytes", Value: 48 << 20},
			},
			wantErr: "MongoDB requires a replica set, no mongos, and bounded native messages",
		},
		{
			name: "mongos",
			response: bson.D{
				{Key: "ok", Value: 1},
				{Key: "setName", Value: "weir_test"},
				{Key: "msg", Value: "isdbgrid"},
				{Key: "maxMessageSizeBytes", Value: 48 << 20},
			},
			wantErr: "MongoDB requires a replica set, no mongos, and bounded native messages",
		},
		{
			name: "oversized native messages",
			response: bson.D{
				{Key: "ok", Value: 1},
				{Key: "setName", Value: "weir_test"},
				{Key: "maxMessageSizeBytes", Value: (48 << 20) + 1},
			},
			wantErr: "MongoDB requires a replica set, no mongos, and bounded native messages",
		},
		{
			name: "hello rejected",
			response: bson.D{
				{Key: "ok", Value: 0},
				{Key: "code", Value: 13},
				{Key: "errmsg", Value: "private server details"},
			},
			wantErr: "MongoDB replica-set qualification failed (MongoDB code 13)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var commands []string
			monitor := &event.CommandMonitor{Started: func(_ context.Context, started *event.CommandStartedEvent) {
				commands = append(commands, started.DatabaseName+"."+started.CommandName)
			}}
			responses := []bson.D{test.response}
			adapter := batchMockAdapter(t, responses, monitor)
			err := adapter.qualify(context.Background())
			if test.wantErr == "" {
				if err != nil {
					t.Fatal("topology qualification requires no server version query", err)
				}
			} else if err == nil || err.Error() != test.wantErr {
				t.Fatal("unexpected topology qualification error", err)
			}
			if len(commands) != 1 || commands[0] != "admin.hello" {
				t.Fatal("server qualification must only request topology metadata", commands)
			}
		})
	}
}
