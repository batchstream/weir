package main

import (
	"context"
	"fmt"
	"io"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
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
	put := &pb.MutateRequest_Put{Put: document}
	mutation := &pb.MutateRequest{Resource: "weir://mongo/weir_m1/records/s:example", Action: put}
	result, err := client.Mutate(ctx, mutation)
	if err != nil {
		return fmt.Errorf("no terminal result: mutation is UNKNOWN: %w", err)
	}
	fmt.Println("Mutate:", result)
	stream, err := client.Bulk(ctx)
	if err != nil {
		return err
	}
	sent := make(chan error, 1)
	go func() {
		open := &pb.BulkOpen{Store: "weir://mongo"}
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
	for {
		frame, e := stream.Recv()
		if e == io.EOF {
			break
		}
		if e != nil {
			return fmt.Errorf("truncated Bulk: %w", e)
		}
		if result := frame.GetResult(); result != nil {
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
	return nil
}
