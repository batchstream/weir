package testutil

import (
	"context"
	"errors"
	"io"

	weirclient "github.com/batchstream/weir-go"
	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/protobuf/proto"
)

// ExecuteOptions keeps wire DTOs in server conformance fixtures. The SDK's
// business interface uses opaque commands and results without protobuf oneofs.
type ExecuteOptions struct {
	StoreName string
	Produce   func(context.Context) (*pb.Command, error)
	Consume   func(context.Context, uint64, *pb.Event) error
	Complete  func(context.Context, uint64) error
}

// Execute bridges shared wire fixtures to the real SDK's bounded execution.
func Execute(ctx context.Context, client pb.StoreServiceClient, opts ExecuteOptions) error {
	options := weirclient.ExecuteOptions{StoreName: opts.StoreName, Complete: opts.Complete}
	if opts.Produce != nil {
		options.Produce = func(ctx context.Context) (*weirclient.Command, error) {
			command, err := opts.Produce(ctx)
			if err != nil {
				return nil, err
			}
			return SDKCommand(command)
		}
	}
	if opts.Consume != nil {
		options.Consume = func(ctx context.Context, id uint64, event *weirclient.Event) error {
			wire, err := wireEvent(event)
			if err != nil {
				return err
			}
			return opts.Consume(ctx, id, wire)
		}
	}
	return weirclient.Execute(ctx, client, options)
}

type RecordFixture struct {
	StoreName string
	Command   *pb.Command
}

// ExecuteRecord collects one conformance result, including evidence received
// before a missing completion frame or failed final transport status.
func ExecuteRecord(ctx context.Context, client pb.StoreServiceClient, fixture RecordFixture) (*pb.Result, error) {
	if fixture.Command == nil || fixture.Command.GetRead() == nil && fixture.Command.GetMutate() == nil {
		return nil, errors.New("record fixture requires a read or mutation command")
	}
	produced := false
	var result *pb.Result
	options := ExecuteOptions{StoreName: fixture.StoreName}
	options.Produce = func(context.Context) (*pb.Command, error) {
		if produced {
			return nil, io.EOF
		}
		produced = true
		return fixture.Command, nil
	}
	options.Consume = func(_ context.Context, _ uint64, event *pb.Event) error {
		result = event.GetResult()
		return nil
	}
	if err := Execute(ctx, client, options); err != nil {
		return result, err
	}
	if result == nil {
		return nil, errors.New("missing record fixture result")
	}
	return result, nil
}

// SDKCommand adapts a validated wire fixture without exposing wire constructors
// in the SDK's public API.
func SDKCommand(command *pb.Command) (*weirclient.Command, error) {
	data, err := proto.Marshal(command)
	if err != nil {
		return nil, err
	}
	if _, err := protocol.DecodeCommand(data); err != nil {
		return nil, err
	}
	switch operation := command.Operation.(type) {
	case *pb.Command_Read:
		return weirclient.NewReadCommand(operation.Read), nil
	case *pb.Command_Scan:
		return weirclient.NewScanCommand(operation.Scan), nil
	case *pb.Command_Native:
		request := &weirclient.NativeRequest{
			Resource: operation.Native.Open.Resource, Descriptor: operation.Native.Open.Descriptor_,
			BodyMediaType: operation.Native.Open.BodyMediaType, Body: operation.Native.Body,
		}
		return weirclient.NewNativeCommand(request), nil
	case *pb.Command_Mutate:
		mutation := operation.Mutate
		switch action := mutation.Action.(type) {
		case *pb.MutateRequest_Put:
			request := &weirclient.WriteRequest{Resource: mutation.Resource, AdapterOptions: mutation.AdapterOptions, Document: action.Put}
			return weirclient.NewPutCommand(request), nil
		case *pb.MutateRequest_Create:
			request := &weirclient.WriteRequest{Resource: mutation.Resource, AdapterOptions: mutation.AdapterOptions, Document: action.Create}
			return weirclient.NewCreateCommand(request), nil
		case *pb.MutateRequest_Replace:
			request := &weirclient.WriteRequest{Resource: mutation.Resource, AdapterOptions: mutation.AdapterOptions, Document: action.Replace}
			return weirclient.NewReplaceCommand(request), nil
		case *pb.MutateRequest_Delete:
			request := &weirclient.DeleteRequest{Resource: mutation.Resource, AdapterOptions: mutation.AdapterOptions}
			return weirclient.NewDeleteCommand(request), nil
		case *pb.MutateRequest_AtomicTransform:
			if action.AtomicTransform == nil {
				return nil, errors.New("missing transform in command fixture")
			}
			request := &weirclient.AtomicTransformRequest{Resource: mutation.Resource, AdapterOptions: mutation.AdapterOptions}
			switch transform := action.AtomicTransform.Form.(type) {
			case *pb.Transform_Program:
				request.Program = transform.Program
			case *pb.Transform_BackendExpression:
				request.BackendExpression = transform.BackendExpression
			}
			return weirclient.NewAtomicTransformCommand(request), nil
		}
	}
	return nil, errors.New("unsupported conformance command")
}

func wireEvent(event *weirclient.Event) (*pb.Event, error) {
	if event == nil {
		return nil, errors.New("missing SDK event")
	}
	wire := &pb.Event{Version: 1}
	switch {
	case event.Result != nil:
		result := &pb.Result{Index: event.Result.Index}
		if read := event.Result.Read; read != nil {
			readResult := &pb.ReadResult{}
			switch {
			case read.Document != nil:
				value := &pb.ReadResult_Document{Document: read.Document}
				readResult.Result = value
			case read.Missing:
				empty := &pb.Empty{}
				value := &pb.ReadResult_Missing{Missing: empty}
				readResult.Result = value
			case read.Failure != nil:
				value := &pb.ReadResult_Failure{Failure: read.Failure}
				readResult.Result = value
			}
			value := &pb.Result_Read{Read: readResult}
			result.Result = value
		} else {
			value := &pb.Result_Mutation{Mutation: event.Result.Mutation}
			result.Result = value
		}
		value := &pb.Event_Result{Result: result}
		wire.Value = value
	case event.Document != nil:
		value := &pb.Event_Document{Document: event.Document}
		wire.Value = value
	case event.Head != nil:
		value := &pb.Event_Head{Head: event.Head}
		wire.Value = value
	case event.Chunk != nil:
		value := &pb.Event_Chunk{Chunk: event.Chunk}
		wire.Value = value
	case event.ScanEnd != nil:
		value := &pb.Event_ScanEnd{ScanEnd: event.ScanEnd}
		wire.Value = value
	case event.NativeEnd != nil:
		value := &pb.Event_NativeEnd{NativeEnd: event.NativeEnd}
		wire.Value = value
	default:
		return nil, errors.New("missing SDK event value")
	}
	return wire, nil
}
