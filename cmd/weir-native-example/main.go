// A bounded duplex Native call. Upload never waits for all response bytes, and
// download never waits for upload half-close. Native errors remain native data.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	spb "github.com/batchstream/weir/api/weir/search/v1"
	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/mongostore"
	"github.com/batchstream/weir/internal/protocol"
	"github.com/batchstream/weir/internal/searchstore"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}
func run() error {
	address := flag.String("address", "127.0.0.1:7447", "loopback Weir listener")
	store := flag.String("store", "mongo", "mongo or search")
	database := flag.String("database", "weir_m1", "configured Mongo database")
	index := flag.String("index", "weir_m2_example", "configured Search index")
	flag.Parse()
	open := &pb.NativeOpen{}
	var body []byte
	switch *store {
	case "mongo":
		open.Resource = "weir://mongo/" + protocol.EncodeSegment(*database) + "/records"
		open.Descriptor_ = &pb.Document{MediaType: mongostore.NativeDescriptor}
		open.BodyMediaType = "application/bson"
		command := bson.D{{Key: "count", Value: "records"}}
		var err error
		body, err = bson.Marshal(command)
		if err != nil {
			return err
		}
	case "search":
		open.Resource = "weir://search/" + protocol.EncodeSegment(*index)
		descriptor := &spb.Request{Method: "GET", Path: "/_doc/example"}
		raw, err := proto.Marshal(descriptor)
		if err != nil {
			return err
		}
		open.Descriptor_ = &pb.Document{MediaType: searchstore.NativeDescriptor, Data: raw}
	default:
		return fmt.Errorf("unsupported store")
	}
	conn, err := grpc.NewClient(*address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithDisableServiceConfig())
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := pb.NewWeirClient(conn).Native(ctx)
	if err != nil {
		return err
	}
	sent := make(chan error, 1)
	go func() { sent <- upload(stream, open, body) }()
	// On every exit, wake and join the sender, including an early backend reply.
	defer func() { cancel(); <-sent }()
	var end *pb.NativeEnd
	headSeen, bodySeen := false, false
	total := 0
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			break
		} // Final gRPC OK is necessary, but not sufficient.
		if err != nil {
			return fmt.Errorf("Native response incomplete; effects unknown: %w", err)
		}
		if end != nil {
			return fmt.Errorf("frame after Native End")
		}
		switch value := frame.Frame.(type) {
		case *pb.NativeResponseFrame_Head:
			if headSeen || bodySeen || value.Head == nil {
				return fmt.Errorf("invalid Native Head order")
			}
			headSeen = true
			if *store == "search" {
				if value.Head.Metadata == nil || value.Head.Metadata.MediaType != searchstore.NativeDescriptor {
					return fmt.Errorf("missing HTTP metadata")
				}
				metadata := &spb.Response{}
				if err := proto.Unmarshal(value.Head.Metadata.Data, metadata); err != nil {
					return err
				}
				fmt.Println("Native HTTP status:", metadata.StatusCode)
			} else if value.Head.BodyMediaType != "application/bson" {
				return fmt.Errorf("unexpected Mongo body type")
			}
		case *pb.NativeResponseFrame_Chunk:
			if len(value.Chunk) == 0 || len(value.Chunk) > protocol.NativeChunk {
				return fmt.Errorf("invalid Native chunk")
			}
			bodySeen = true
			total += len(value.Chunk)
			// Process bytes incrementally here; this example does not retain the response.
		case *pb.NativeResponseFrame_End:
			end = value.End
		default:
			return fmt.Errorf("invalid Native frame")
		}
	}
	if end == nil {
		return fmt.Errorf("missing Native End; effects unknown")
	}
	switch end.Completion {
	case pb.NativeCompletion_RESPONSE_COMPLETE:
		if end.Failure != nil || !headSeen {
			return fmt.Errorf("invalid complete Native response")
		}
		fmt.Printf("Native response complete: %d bytes; database success is determined by native status/body\n", total)
		return nil
	case pb.NativeCompletion_NATIVE_NOT_STARTED, pb.NativeCompletion_RESPONSE_INCOMPLETE:
		if end.Failure == nil {
			return fmt.Errorf("invalid Native failure envelope")
		}
		return fmt.Errorf("Native %s: %s", end.Completion, end.Failure.Code)
	default:
		return fmt.Errorf("unspecified Native completion")
	}
}
func upload(stream grpc.BidiStreamingClient[pb.NativeRequestFrame, pb.NativeResponseFrame], open *pb.NativeOpen, body []byte) error {
	variant := &pb.NativeRequestFrame_Open{Open: open}
	frame := &pb.NativeRequestFrame{Frame: variant}
	if err := stream.Send(frame); err != nil {
		return err
	}
	for len(body) > 0 {
		n := min(protocol.NativeChunk, len(body))
		chunk := &pb.NativeRequestFrame_Chunk{Chunk: body[:n]}
		frame := &pb.NativeRequestFrame{Frame: chunk}
		if err := stream.Send(frame); err != nil {
			return err
		}
		body = body[n:]
	}
	return stream.CloseSend()
}
