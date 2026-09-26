package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/protocol"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}
func run() error {
	name := flag.String("store", "mongo", "mongo or search")
	database := flag.String("database", "weir_m1", "pre-created MongoDB database")
	index := flag.String("index", "weir_m2_example", "pre-created Search index")
	flag.Parse()
	if *name != "mongo" && *name != "search" {
		return fmt.Errorf("store must be mongo or search")
	}
	conn, err := grpc.NewClient("127.0.0.1:7447", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry())
	if err != nil {
		return err
	}
	defer conn.Close()
	client := pb.NewWeirClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	raw := bson.D{{Key: "_id", Value: "example"}, {Key: "n", Value: int32(1)}}
	data, err := bson.Marshal(raw)
	if err != nil {
		return err
	}
	document := &pb.Document{MediaType: "application/bson", Data: data}
	resource := "weir://mongo/" + protocol.EncodeSegment(*database) + "/records/s:example"
	if *name == "search" {
		document = &pb.Document{MediaType: "application/json", Data: []byte(`{"n":1}`)}
		resource = "weir://search/" + protocol.EncodeSegment(*index) + "/s:example"
	}
	put := &pb.MutateRequest_Put{Put: document}
	mutation := &pb.MutateRequest{Resource: resource, Action: put}
	result, err := client.Mutate(ctx, mutation)
	if err != nil {
		return fmt.Errorf("no terminal result: mutation is UNKNOWN: %w", err)
	}
	fmt.Println("Mutate:", result)
	if result.Outcome != pb.MutationOutcome_APPLIED {
		return fmt.Errorf("mutation was not acknowledged: %s", result.Outcome)
	}
	stream, err := client.Bulk(ctx)
	if err != nil {
		return err
	}
	sent := make(chan error, 1)
	go func() {
		open := &pb.BulkOpen{Store: "weir://" + *name}
		variant := &pb.BulkRequestFrame_Open{Open: open}
		frame := &pb.BulkRequestFrame{Frame: variant}
		if e := stream.Send(frame); e != nil {
			sent <- e
			return
		}
		for i := uint64(0); i < 3; i++ {
			read := &pb.ReadRequest{Resource: mutation.Resource}
			rv := &pb.BulkOperation_Read{Read: read}
			op := &pb.BulkOperation{Index: i, Operation: rv}
			ov := &pb.BulkRequestFrame_Operation{Operation: op}
			f := &pb.BulkRequestFrame{Frame: ov}
			if e := stream.Send(f); e != nil {
				sent <- e
				return
			}
		}
		sent <- stream.CloseSend()
	}()
	count := uint64(0)
	ended := false
	seen := make(map[uint64]bool)
	for {
		frame, e := stream.Recv()
		if e == io.EOF {
			break
		}
		if e != nil {
			return fmt.Errorf("truncated Bulk: %w", e)
		}
		if ended {
			return fmt.Errorf("Bulk frame after End")
		}
		if result := frame.GetResult(); result != nil {
			if result.Index >= 3 || seen[result.Index] || result.GetRead().GetDocument() == nil {
				return fmt.Errorf("invalid Bulk result")
			}
			seen[result.Index] = true
			count++
			fmt.Println("Bulk index:", result.Index, "missing:", result.GetRead().GetMissing() != nil)
		} else if end := frame.GetEnd(); end != nil {
			ended = true
			if end.ResultCount != count || end.ReceivedCount != 3 {
				return fmt.Errorf("bad Bulk counts")
			}
		}
	}
	if err := <-sent; err != nil {
		return err
	}
	if !ended {
		return fmt.Errorf("Bulk missing End")
	}
	filter := bson.D{{Key: "_id", Value: "example"}}
	selector := bson.D{{Key: "filter", Value: filter}}
	encoded, err := bson.Marshal(selector)
	if err != nil {
		return err
	}
	native := &pb.Document{MediaType: "application/bson", Data: encoded}
	if *name == "search" {
		native = &pb.Document{MediaType: "application/json", Data: []byte(`{"query":{"ids":{"values":["example"]}}}`)}
	}
	request := &pb.ScanRequest{Resource: strings.TrimSuffix(resource, "/s:example"), Selector: native, FetchItemsHint: 8}
	scan, err := client.Scan(ctx, request)
	if err != nil {
		return err
	}
	count, ended = 0, false
	var failure *pb.Failure
	for {
		frame, err := scan.Recv()
		if err == io.EOF {
			break
		} // Final gRPC OK, necessary but not sufficient.
		if err != nil {
			return fmt.Errorf("incomplete Scan: %w", err)
		}
		if ended {
			return fmt.Errorf("Scan frame after End")
		}
		if doc := frame.GetDocument(); doc != nil {
			count++
			// Process this opaque native result here; never collect an unbounded stream.
			fmt.Printf("Scan document %d: %s, %d bytes\n", count, doc.MediaType, len(doc.Data))
		} else if end := frame.GetEnd(); end != nil {
			ended, failure = true, end.Failure
			if end.DocumentCount != count {
				return fmt.Errorf("Scan count mismatch")
			}
		} else {
			return fmt.Errorf("invalid Scan frame")
		}
	}
	if !ended {
		return fmt.Errorf("Scan missing End")
	}
	if failure != nil {
		return fmt.Errorf("Scan traversal failed after %d documents: %s", count, failure.Code)
	}
	fmt.Printf("Scan complete: %d documents, matching End, final gRPC OK\n", count)
	// Search visibility is native: a just-written example may not be refreshed yet.
	return nil
}
