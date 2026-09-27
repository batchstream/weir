//go:build integration

// A bounded, opt-in client for the owned single-replica Kubernetes fixture.
// It never loads Kubernetes credentials or calls the Kubernetes API.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	spb "github.com/batchstream/weir/api/weir/search/v1"
	pb "github.com/batchstream/weir/api/weir/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	mode := flag.String("mode", "idle", "owned fixture phase")
	id := flag.String("id", "smoke", "unique operation suffix")
	flag.Parse()
	if os.Getenv("WEIR_KUBE_INTEGRATION") != "1" {
		return errors.New("explicit integration opt-in required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	switch *mode {
	case "idle":
		time.Sleep(10 * time.Minute)
		return nil
	case "setup":
		code, raw, err := admin(ctx, "PUT", "/records", `{"settings":{"number_of_shards":1,"number_of_replicas":0}}`)
		if err != nil || code != 200 {
			return fmt.Errorf("setup: %d %s %v", code, raw, err)
		}
		code, raw, err = admin(ctx, "GET", "/", "")
		fmt.Printf("backend identity status=%d %s\n", code, raw)
		return err
	case "inspect":
		return persisted(ctx, *id)
	}
	connection, err := grpc.NewClient("dns:///weir.m20.svc.cluster.local:7447", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry())
	if err != nil {
		return err
	}
	defer connection.Close()
	client := pb.NewWeirClient(connection)
	switch *mode {
	case "smoke":
		return smoke(ctx, client, *id)
	case "outage":
		started := time.Now()
		request := put(*id)
		reply, err := client.Mutate(ctx, request)
		fmt.Printf("outage elapsed=%s outcome=%s failure=%s rpc=%v\n", time.Since(started), reply.GetOutcome(), reply.GetFailure().GetCode(), err)
		if err == nil && (reply.GetOutcome() == pb.MutationOutcome_APPLIED || reply.GetFailure() == nil) {
			return errors.New("outage falsely succeeded")
		}
		if time.Since(started) > 4*time.Second {
			return errors.New("outage exceeded bound")
		}
		return nil
	case "active":
		return active(ctx, connection, client, *id)
	default:
		return errors.New("unknown fixture phase")
	}
}
func admin(ctx context.Context, method, path, body string) (int, []byte, error) {
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true, DisableCompression: true, MaxConnsPerHost: 1}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
	request, err := http.NewRequestWithContext(ctx, method, "http://elasticsearch.m20.svc.cluster.local:9200"+path, bytes.NewBufferString(body))
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.GetBody = nil
	reply, err := client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer reply.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(reply.Body, 64<<10))
	return reply.StatusCode, raw, err
}
func put(id string) *pb.MutateRequest {
	doc := &pb.Document{MediaType: "application/json", Data: []byte(`{"n":1}`)}
	action := &pb.MutateRequest_Put{Put: doc}
	request := &pb.MutateRequest{Resource: "weir://records/records/s:" + id, Action: action}
	return request
}
func persisted(ctx context.Context, id string) error {
	code, raw, err := admin(ctx, "GET", "/records/_doc/"+id, "")
	var result struct {
		Version int `json:"_version"`
		Source  struct {
			N int `json:"n"`
		} `json:"_source"`
	}
	if err != nil || code != 200 || json.Unmarshal(raw, &result) != nil || result.Version != 1 || result.Source.N != 1 {
		return fmt.Errorf("persistence %s: status=%d body=%s err=%v", id, code, raw, err)
	}
	fmt.Printf("persisted id=%s version=1 n=1\n", id)
	return nil
}
func smoke(ctx context.Context, client pb.WeirClient, id string) error {
	request := put(id)
	reply, err := client.Mutate(ctx, request)
	if err != nil || reply.GetOutcome() != pb.MutationOutcome_APPLIED || reply.GetFailure() != nil {
		return fmt.Errorf("mutation: %v %v", reply, err)
	}
	read := &pb.ReadRequest{Resource: request.Resource}
	result, err := client.Read(ctx, read)
	if err != nil || result.GetFailure() != nil || result.GetDocument() == nil {
		return fmt.Errorf("read: %v %v", result, err)
	}
	stream, err := client.Bulk(ctx)
	if err != nil {
		return err
	}
	open := &pb.BulkOpen{Store: "weir://records"}
	opening := &pb.BulkRequestFrame_Open{Open: open}
	frame := &pb.BulkRequestFrame{Frame: opening}
	if err := stream.Send(frame); err != nil {
		return err
	}
	mutation := &pb.BulkOperation_Mutate{Mutate: put(id + "-bulk")}
	op := &pb.BulkOperation{Index: 7, Operation: mutation}
	variant := &pb.BulkRequestFrame_Operation{Operation: op}
	frame = &pb.BulkRequestFrame{Frame: variant}
	if err := stream.Send(frame); err != nil {
		return err
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}
	got, end := false, false
	for {
		r, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if item := r.GetResult(); item != nil {
			if got || item.Index != 7 || item.GetMutation().GetOutcome() != pb.MutationOutcome_APPLIED {
				return errors.New("bulk correlation/outcome")
			}
			got = true
		}
		if r.GetEnd() != nil {
			end = true
		}
	}
	if !got || !end {
		return errors.New("bulk incomplete")
	}
	if err := persisted(ctx, id); err != nil {
		return err
	}
	if err := persisted(ctx, id+"-bulk"); err != nil {
		return err
	}
	fmt.Println("Service DNS Mutate=APPLIED Read=found Bulk index=7 APPLIED End+EOF")
	return nil
}
func active(ctx context.Context, connection *grpc.ClientConn, client pb.WeirClient, id string) error {
	code, _, err := admin(ctx, "PUT", "/records/_settings", `{"index":{"refresh_interval":"-1"}}`)
	if err != nil || code != 200 {
		return errors.New("disable fixture refresh")
	}
	// This separate live unary upload keeps drain observable until the original
	// 3-second input-stall deadline. It sends no mutation body and is not replayed.
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
	chunk := &pb.NativeRequestFrame_Chunk{Chunk: []byte("{\"index\":{\"_id\":\"" + id + "\"}}\n{\"n\":1}\n")}
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
	fmt.Println("ACTIVE mutation persisted; refresh reply pending; delete Pod now")
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
	fmt.Printf("partial unary ended=%v\n", err)
	return persisted(ctx, id)
}
