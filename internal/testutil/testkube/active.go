//go:build integration

package main

import (
	"context"
	"errors"
	"fmt"
	spb "github.com/batchstream/weir/api/weir/search/v1"
	pb "github.com/batchstream/weir/api/weir/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"time"
)

func active(ctx context.Context, connection *grpc.ClientConn, client pb.WeirClient, id string) error {
	started := time.Now()
	code, _, err := admin(ctx, "PUT", "/records/_settings", `{"index":{"refresh_interval":"-1"}}`)
	if err != nil || code != 200 {
		return errors.New("disable fixture refresh")
	}
	// This separate live unary upload keeps drain observable until the original
	// default 30-second input-stall deadline. It sends no mutation body and is not replayed.
	description := &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}
	partial, err := connection.NewStream(ctx, description, pb.Weir_Read_FullMethodName)
	if err != nil {
		return err
	}
	stream, err := client.Native(ctx)
	if err != nil {
		return err
	}
	descriptor := &spb.Request{Method: "POST", Path: "/_bulk", Query: "refresh=wait_for"}
	raw, err := proto.Marshal(descriptor)
	if err != nil {
		return err
	}
	doc := &pb.Document{MediaType: "application/vnd.weir.search-http.v1+protobuf", Data: raw}
	open := &pb.NativeOpen{Resource: "weir://records/records", Descriptor_: doc, BodyMediaType: "application/x-ndjson"}
	variant := &pb.NativeRequestFrame_Open{Open: open}
	frame := &pb.NativeRequestFrame{Frame: variant}
	if err := stream.Send(frame); err != nil {
		return err
	}
	chunk := &pb.NativeRequestFrame_Chunk{Chunk: []byte(fmt.Sprintf("{\"index\":{\"_id\":%q}}\n{\"n\":1}\n", id))}
	frame = &pb.NativeRequestFrame{Frame: chunk}
	if err := stream.Send(frame); err != nil {
		return err
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}
	until := time.Now().Add(time.Second)
	for {
		code, _, err := admin(ctx, "GET", "/records/_doc/"+id, "")
		if err == nil && code == 200 {
			break
		}
		if time.Now().After(until) {
			return errors.New("active mutation not persisted in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
	fmt.Println("ACTIVE mutation persisted; refresh reply pending; roll or pause worker now")
	for {
		reply, err := stream.Recv()
		if err != nil {
			fmt.Printf("Native missing result=%v effects=UNKNOWN no replay\n", err)
			break
		}
		if end := reply.GetEnd(); end != nil {
			fmt.Printf("Native completion=%s failure=%s effects=UNKNOWN no replay\n", end.Completion, end.GetFailure().GetCode())
			if end.Completion == pb.NativeCompletion_RESPONSE_COMPLETE {
				return errors.New("expected held reply to remain incomplete")
			}
			break
		}
	}
	var response pb.ReadResult
	err = partial.RecvMsg(&response)
	fmt.Printf("partial unary elapsed=%s ended=%v\n", time.Since(started), err)
	readback, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return persisted(readback, id)
}
