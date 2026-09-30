//go:build integration

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	pb "github.com/batchstream/weir/api/weir/v1"
	"io"
	"time"
)

// Each logical operation is sent once. An absent transport result stays UNKNOWN;
// direct GET is evidence only for these isolated, uniquely named fixture records.
func load(ctx context.Context, client pb.WeirClient, prefix string) error {
	for i := 0; i < 60; i++ {
		id := fmt.Sprintf("%s-%03d", prefix, i)
		if err := mutation(ctx, client, id); err != nil {
			return err
		}
		select {
		case <-time.After(250 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func mutation(ctx context.Context, client pb.WeirClient, id string) error {
	call, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	request := put(id)
	reply, err := client.Mutate(call, request)
	outcome := "UNKNOWN"
	if err == nil {
		outcome = reply.GetOutcome().String()
	}
	fmt.Printf("operation id=%s outcome=%s failure=%s rpc=%v\n", id, outcome, reply.GetFailure().GetCode(), err)
	if err == nil && reply.GetOutcome() == pb.MutationOutcome_MUTATION_OUTCOME_UNSPECIFIED {
		return errors.New("missing outcome")
	}
	return verifyEffect(ctx, id, outcome)
}

func verifyEffect(ctx context.Context, id, outcome string) error {
	code, raw, err := admin(ctx, "GET", "/records/_doc/"+id, "")
	if err != nil {
		return err
	}
	var record struct {
		Version int  `json:"_version"`
		Found   bool `json:"found"`
	}
	if json.Unmarshal(raw, &record) != nil || (code != 200 && code != 404) {
		return fmt.Errorf("invalid readback %d %s", code, raw)
	}
	if record.Found && record.Version != 1 {
		return fmt.Errorf("replay id=%s version=%d", id, record.Version)
	}
	if outcome == "APPLIED" && !record.Found {
		return errors.New("false APPLIED")
	}
	if (outcome == "NOT_STARTED" || outcome == "NOT_APPLIED") && record.Found {
		return errors.New("false non-application")
	}
	fmt.Printf("readback id=%s found=%t version=%d client=%s\n", id, record.Found, record.Version, outcome)
	return nil
}

func hold(ctx context.Context, client pb.WeirClient, prefix string) error {
	stream, err := client.Bulk(ctx)
	if err != nil {
		return err
	}
	open := &pb.BulkOpen{Store: "weir://records"}
	variant := &pb.BulkRequestFrame_Open{Open: open}
	frame := &pb.BulkRequestFrame{Frame: variant}
	if err := stream.Send(frame); err != nil {
		return err
	}
	for i := 0; i < 40; i++ {
		id := fmt.Sprintf("%s-%03d", prefix, i)
		mutation := &pb.BulkOperation_Mutate{Mutate: put(id)}
		op := &pb.BulkOperation{Index: uint64(i), Operation: mutation}
		variant := &pb.BulkRequestFrame_Operation{Operation: op}
		frame := &pb.BulkRequestFrame{Frame: variant}
		if err := stream.Send(frame); err != nil {
			return fmt.Errorf("sent bulk id=%s UNKNOWN: %w", id, err)
		}
		reply, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("sent bulk id=%s UNKNOWN: %w", id, err)
		}
		result := reply.GetResult()
		if result == nil || result.Index != uint64(i) || result.GetMutation().GetOutcome() != pb.MutationOutcome_APPLIED || result.GetMutation().GetFailure() != nil {
			return errors.New("bulk correlation or outcome")
		}
		if err := persisted(ctx, id); err != nil {
			return err
		}
		fmt.Printf("HOLD index=%d APPLIED\n", i)
		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}
	reply, err := stream.Recv()
	if err != nil || reply.GetEnd() == nil || reply.GetEnd().ReceivedCount != 40 || reply.GetEnd().ResultCount != 40 {
		return fmt.Errorf("missing or mismatched End: %v %v", reply, err)
	}
	_, err = stream.Recv()
	if err != io.EOF {
		return fmt.Errorf("expected final EOF: %v", err)
	}
	fmt.Println("HOLD End received=40 results=40 EOF")
	return nil
}
