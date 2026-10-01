// Basic demonstrates one finite Route batch with incremental input and consumption.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/protocol"
	"github.com/batchstream/weir/routeclient"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}
func run() error {
	address := flag.String("address", "127.0.0.1:7447", "Weir listener")
	destination := flag.String("store", "mongo", "logical Store: mongo or search")
	database := flag.String("database", "weir_m1", "pre-created MongoDB database")
	index := flag.String("index", "weir_m2_example", "pre-created Search index")
	count := flag.Int("count", 100, "finite number of read requests")
	flag.Parse()
	if *destination != "mongo" && *destination != "search" || *count < 1 {
		return fmt.Errorf("invalid Store or count")
	}
	conn, err := routeclient.Dial(*address)
	if err != nil {
		return err
	}
	defer conn.Close()
	client := pb.NewWeirClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	target := protocol.EncodeSegment(*database) + "/records/s:example"
	record := bson.D{{Key: "_id", Value: "example"}, {Key: "n", Value: int32(1)}}
	data, err := bson.Marshal(record)
	if err != nil {
		return err
	}
	document := &pb.Document{MediaType: "application/bson", Data: data}
	if *destination == "search" {
		target = protocol.EncodeSegment(*index) + "/s:example"
		document = &pb.Document{MediaType: "application/json", Data: []byte(`{"n":1}`)}
	}
	action := &pb.MutateRequest_Put{Put: document}
	mutation := &pb.MutateRequest{Resource: target, Action: action}
	variant := &pb.Call_Mutate{Mutate: mutation}
	call := &pb.Call{Version: 1, Operation: variant}
	opts := routeclient.RecordOptions{Destination: *destination, Call: call}
	result, err := routeclient.Record(ctx, client, opts)
	if err != nil {
		return fmt.Errorf("write has no complete result; effects indeterminate: %w", err)
	}
	if result.GetMutation().GetOutcome() != pb.MutationOutcome_APPLIED {
		return fmt.Errorf("write: %v", result)
	}
	produced := 0
	batch := routeclient.Options{Destination: *destination}
	batch.Produce = func(context.Context) (*pb.Call, error) {
		if produced == *count {
			return nil, io.EOF
		}
		produced++
		read := &pb.ReadRequest{Resource: target}
		variant := &pb.Call_Read{Read: read}
		call := &pb.Call{Version: 1, Operation: variant}
		return call, nil
	}
	batch.Consume = func(_ context.Context, id uint64, event *pb.Event) error {
		read := event.GetResult().GetRead()
		if read.GetFailure() != nil {
			return fmt.Errorf("read %d: %v", id, read.GetFailure())
		}
		fmt.Printf("id=%d record=%d bytes missing=%t\n", id, len(read.GetDocument().GetData()), read.GetMissing() != nil)
		// Consume and discard here: retaining Events would require the full batch memory.
		return nil
	}
	if err := routeclient.Run(ctx, client, batch); err != nil {
		return err
	}
	fmt.Printf("completed %d reads with all request ends and final gRPC OK\n", *count)
	return nil
}
