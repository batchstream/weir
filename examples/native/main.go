// A finite Execute batch with a bounded native request and incremental response.
// Native backend errors remain native response data, not normalized write outcomes.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/batchstream/weir/api/protocol"
	spb "github.com/batchstream/weir/api/weir/search/v1"
	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/backend/mongodb"
	"github.com/batchstream/weir/internal/backend/search"
	"github.com/batchstream/weir/weirclient"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/protobuf/proto"
)

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

func run() error {
	address := flag.String("address", "127.0.0.1:7447", "Weir initialization listener")
	store := flag.String("store", "mongo", "mongo or search")
	database := flag.String("database", "weir_m1", "configured Mongo database")
	index := flag.String("index", "weir_m2_example", "configured Search index")
	flag.Parse()

	open := &pb.NativeOpen{}
	var body []byte
	switch *store {
	case "mongo":
		open.Resource = protocol.EncodeSegment(*database) + "/records"
		open.Descriptor_ = &pb.Document{MediaType: mongodb.NativeDescriptor}
		open.BodyMediaType = "application/bson"
		command := bson.D{{Key: "count", Value: "records"}}
		var err error
		body, err = bson.Marshal(command)
		if err != nil {
			return err
		}
	case "search":
		open.Resource = protocol.EncodeSegment(*index)
		descriptor := &spb.Request{Method: "GET", Path: "/_doc/example"}
		raw, err := proto.Marshal(descriptor)
		if err != nil {
			return err
		}
		open.Descriptor_ = &pb.Document{MediaType: search.NativeDescriptor, Data: raw}
	default:
		return fmt.Errorf("unsupported store")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	openOptions := weirclient.OpenOptions{Seed: *address, Stores: []string{*store}}
	client, err := weirclient.Open(ctx, openOptions)
	if err != nil {
		return err
	}
	defer client.Close()
	native := &pb.NativeCall{Open: open, Body: body}
	variant := &pb.Call_Native{Native: native}
	call := &pb.Call{Version: 1, Operation: variant}
	produced := false
	total := 0
	var terminal *pb.NativeEnd
	opts := weirclient.Options{StoreName: *store}
	opts.Produce = func(context.Context) (*pb.Call, error) {
		if produced {
			return nil, io.EOF
		}
		produced = true
		return call, nil
	}
	opts.Consume = func(_ context.Context, _ uint64, event *pb.Event) error {
		if head := event.GetHead(); head != nil {
			fmt.Printf("metadata=%v media=%s\n", head.Metadata, head.BodyMediaType)
		}
		total += len(event.GetChunk())
		if end := event.GetNativeEnd(); end != nil {
			terminal = end
		}
		// Consume native bytes here without collecting the entire response.
		return nil
	}
	if err := client.Execute(ctx, opts); err != nil {
		return fmt.Errorf("native response incomplete; effects indeterminate: %w", err)
	}
	if terminal == nil || terminal.Completion != pb.NativeCompletion_RESPONSE_COMPLETE {
		return fmt.Errorf("native exchange: %v; effects indeterminate", terminal)
	}
	fmt.Printf("complete native response: %d bytes\n", total)
	return nil
}
