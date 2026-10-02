//go:build integration

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	weirclient "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil"
	"io"
	"time"
)

// Each logical operation is sent once. An absent transport result stays UNKNOWN;
// direct GET is evidence only for these isolated, uniquely named fixture records.
func load(ctx context.Context, client pb.StoreServiceClient, prefix string) error {
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

func mutation(ctx context.Context, client pb.StoreServiceClient, id string) error {
	call, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	request := put(id)
	result, err := weirclient.Record(call, client, testutil.RecordCall(request))
	reply := result.GetMutation()
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

func hold(ctx context.Context, client pb.StoreServiceClient, prefix string) error {
	next := 0
	opts := weirclient.Options{StoreName: "records"}
	opts.Produce = func(ctx context.Context) (*pb.Call, error) {
		if next == 40 {
			return nil, io.EOF
		}
		if next > 0 {
			select {
			case <-time.After(500 * time.Millisecond):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		id := fmt.Sprintf("%s-%03d", prefix, next)
		next++
		fixture := testutil.RecordCall(put(id))
		return fixture.Call, nil
	}
	opts.Consume = func(ctx context.Context, id uint64, event *pb.Event) error {
		result := event.GetResult()
		if result == nil || result.Index != id || result.GetMutation().GetOutcome() != pb.MutationOutcome_APPLIED || result.GetMutation().GetFailure() != nil {
			return errors.New("Route correlation or outcome")
		}
		record := fmt.Sprintf("%s-%03d", prefix, id-1)
		if err := persisted(ctx, record); err != nil {
			return err
		}
		fmt.Printf("HOLD id=%d APPLIED\n", id)
		return nil
	}
	if err := weirclient.Execute(ctx, client, opts); err != nil {
		return err
	}
	fmt.Println("HOLD all 40 request ends and final OK")
	return nil
}
