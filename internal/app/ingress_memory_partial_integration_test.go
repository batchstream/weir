//go:build integration

package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ingressMemoryPartialOptions struct {
	fixture      *testmongo.Fixture
	process      *memoryRecoveryCLI
	dataset      *nativeBurstDataset
	documents    []ingressMemoryDocument
	client       *weirclient.Client
	healthClient *weirclient.Client
}

// This profile never sends the final16KiB. It verifies ownership and cancellation
// of incomplete messages; A independently verifies full ordinary Put completion.
func runIngressMemoryPartial(t *testing.T, options ingressMemoryPartialOptions) {
	t.Helper()
	uploads := make([]*ingressMemoryUpload, len(options.documents))
	for index := range options.documents {
		resource := options.dataset.root + "/s:" + options.documents[index].id
		uploads[index] = newIngressMemoryUpload(t, options.process, options.documents[index], resource)
		options.documents[index].raw = nil
		t.Cleanup(func() { _ = uploads[index].connection.Close() })
	}
	ingressMemoryIdle(t, options.process, time.Now().Add(5*time.Second), 34)
	started := time.Now()
	cancelDeadline := started.Add(3 * time.Second)
	callerDeadline := started.Add(5 * time.Second)
	ctx, cancel := context.WithDeadline(t.Context(), callerDeadline)
	defer cancel()
	t.Logf("ingress_launch profile=partial_cancel t0=%s common_caller_deadline=%s fixed_cancel=%s tails_never_released=true records=32", started.UTC().Format(time.RFC3339Nano), callerDeadline.UTC().Format(time.RFC3339Nano), cancelDeadline.UTC().Format(time.RFC3339Nano))
	barrier := make(chan struct{})
	results := make(chan ingressMemoryResult, len(uploads))
	var calls sync.WaitGroup
	var launched, returned, cancelledAt atomic.Int64
	for index, upload := range uploads {
		calls.Add(1)
		go func() {
			defer calls.Done()
			<-barrier
			result := ingressMemoryResult{Index: index, ID: options.documents[index].id, Started: time.Now(), TransportObserved: true}
			launched.Add(1)
			mutation, err := upload.put(ctx, &result)
			result.Finished, result.Received, result.RPCCode = time.Now(), true, status.Code(err).String()
			result.CallerCanceled = errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled
			if err != nil {
				result.Error = err.Error()
			}
			if mutation != nil {
				result.Outcome = mutation.Outcome.String()
				if mutation.Failure != nil {
					result.Failure = mutation.Failure.Code.String()
				}
			}
			returned.Add(1)
			results <- result
		}()
	}
	healthDone := make(chan struct{})
	go func() {
		defer close(healthDone)
		condition := ingressMemoryPartialCondition{options: options, uploads: uploads, started: started, launched: &launched, returned: &returned}
		index := ingressMemoryIncomplete(t, ctx, condition)
		if index < 0 {
			return
		}
		var health sync.WaitGroup
		health.Add(2)
		go func() {
			defer health.Done()
			ingressMemoryHealth(t, options.healthClient, options.dataset)
		}()
		go func() {
			defer health.Done()
			ingressMemoryConnectionHealth(t, uploads[index], options.dataset)
		}()
		health.Wait()
		if !time.Now().Before(cancelDeadline) {
			t.Error("partial-upload health did not finish within the fixed original3s missing-tail hold")
		}
	}()
	go func() {
		timer := time.NewTimer(time.Until(cancelDeadline))
		defer timer.Stop()
		select {
		case <-timer.C:
			cancelledAt.Store(time.Now().UnixNano())
			cancel()
		case <-t.Context().Done():
			cancel()
		}
	}()
	close(barrier)
	joined := make(chan struct{})
	go func() { calls.Wait(); close(joined) }()
	collected := make([]ingressMemoryResult, len(uploads))
	for index := range collected {
		collected[index] = ingressMemoryResult{Index: index, ID: options.documents[index].id, Error: "partial caller did not return within scheduled cancellation plus bounded1s join"}
	}
	joinTimer := time.NewTimer(time.Until(cancelDeadline.Add(time.Second)))
	defer joinTimer.Stop()
	received := 0
collect:
	for received < len(uploads) {
		select {
		case result := <-results:
			collected[result.Index] = result
			received++
		case <-joinTimer.C:
			t.Error("partial cancellation plus original bounded1s join lost results", received)
			break collect
		}
	}
	cancel()
	actualCancellation := time.Unix(0, cancelledAt.Load())
	if cancelledAt.Load() == 0 || actualCancellation.Before(cancelDeadline) || actualCancellation.After(callerDeadline) {
		t.Error("fixed3s partial cancellation did not occur within the original5s caller", actualCancellation)
		actualCancellation = cancelDeadline
	}
	select {
	case <-joined:
	case <-time.After(time.Until(cancelDeadline.Add(time.Second))):
		t.Error("partial upload goroutines remained after cancellation plus bounded1s")
	}
	select {
	case <-healthDone:
	case <-time.After(time.Until(callerDeadline.Add(time.Second))):
		t.Error("partial health callers did not join within original5s plus bounded1s")
	}
	for index, result := range collected {
		observer := uploads[index].observer
		if !result.Received || !result.CallerCanceled || result.Outcome != "" || result.Failure != "" || result.ResponseMessages != 0 || result.Finished.Before(cancelDeadline) || result.Finished.After(actualCancellation.Add(time.Second)) || observer.invalid.Load() || observer.allAt.Load() != 0 || observer.written.Load() > observer.prefix || result.AllWritten != 0 || result.DataWritten >= int64(len(uploads[index].frame)) {
			t.Error("partial Put lacked bounded explicit cancellation with no complete frame or mutation reply", result.ID, result.RPCCode, result.Error, result.Outcome, result.DataWritten, len(uploads[index].frame))
		}
		raw, _ := json.Marshal(result)
		t.Logf("ingress_partial_result %s scheduled_cancel=%s actual_cancel=%s final_frame_bytes=%d incomplete_message_cancellation_is_not_mutation_acknowledgement=true", raw, cancelDeadline.UTC().Format(time.RFC3339Nano), actualCancellation.UTC().Format(time.RFC3339Nano), len(uploads[index].frame))
	}
	// Keep connections open until every raw/wait owner has been released, so
	// connection teardown cannot hide a cancellation ownership leak.
	ingressMemoryIdle(t, options.process, actualCancellation.Add(5*time.Second), 34)
	for index, upload := range uploads {
		if upload.observer.invalid.Load() || upload.observer.allAt.Load() != 0 || upload.observer.written.Load() > upload.observer.prefix {
			t.Error("partial stream DATA observation changed after caller return or released the forbidden tail", index, upload.observer.written.Load(), upload.observer.prefix)
		}
		t.Logf("partial_stream_after_cancel_ownership_release index=%d DATA_socket_written=%d complete_frame_bytes=%d local_prefix_bytes=%d all_written_marker=%d", index, upload.observer.written.Load(), len(upload.frame), upload.observer.prefix, upload.observer.allAt.Load())
		_ = upload.connection.Close()
	}
	ingressMemoryIdle(t, options.process, time.Now().Add(5*time.Second), 2)
	ingressMemoryAbsent(t, options.fixture, options.documents)
	t.Logf("unconditional_same_SDK_recovery profile=partial_cancel prior_failure=%t original_client=%p TCP_connection_identity=not_observed", t.Failed(), options.client)
	ingressMemoryRecovery(t, options.client, options.dataset)
	ingressMemoryIdle(t, options.process, time.Now().Add(5*time.Second), 2)
	closeIngressMemoryClient(t, options.client)
	closeIngressMemoryClient(t, options.healthClient)
	ingressMemoryIdle(t, options.process, time.Now().Add(5*time.Second), 0)
}

type ingressMemoryPartialCondition struct {
	options  ingressMemoryPartialOptions
	uploads  []*ingressMemoryUpload
	started  time.Time
	launched *atomic.Int64
	returned *atomic.Int64
}

func ingressMemoryIncomplete(t *testing.T, ctx context.Context, condition ingressMemoryPartialCondition) int {
	t.Helper()
	holdDeadline := condition.started.Add(3 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for time.Now().Before(holdDeadline) {
		selected := -1
		for index, upload := range condition.uploads {
			if upload.observer.invalid.Load() || upload.observer.allAt.Load() != 0 || upload.observer.written.Load() >= upload.observer.total {
				t.Error("partial-upload missing-tail construction was violated", index)
				return -1
			}
			if upload.observer.prefixAt.Load() != 0 {
				selected = index
			}
		}
		metrics, err := ingressMemoryMetrics(condition.options.process)
		valid := err == nil && condition.launched.Load() == 32 && condition.returned.Load() == 0
		for _, name := range []string{"weir_execution_memory_ingress_bytes", "weir_execution_memory_ingress_reserved_bytes", "weir_execution_memory_ingress_buffers", "weir_execution_memory_ingress_messages", "weir_execution_memory_ingress_waiting"} {
			valid = valid && metrics[name] != nil && testmetrics.Sum(metrics, name) > 0
		}
		valid = valid && metrics["weir_ingress_sessions"] != nil && testmetrics.Sum(metrics, "weir_ingress_sessions") >= 32
		for _, name := range []string{"weir_execution_memory_leases", "weir_execution_memory_input_used_bytes", "weir_execution_memory_wire_bytes", "weir_execution_memory_wire_buffers", "weir_store_active_executions", "weir_store_pending_entries", "weir_store_pending_reserved_bytes"} {
			valid = valid && metrics[name] != nil && testmetrics.Sum(metrics, name) == 0
		}
		for _, name := range []string{"weir_execution_memory_used_bytes", "weir_execution_memory_resident_bytes", "weir_execution_memory_budget_bytes", "weir_execution_memory_ingress_connections", "weir_execution_memory_ingress_connection_bytes"} {
			valid = valid && metrics[name] != nil
		}
		used := testmetrics.Sum(metrics, "weir_execution_memory_used_bytes")
		resident := testmetrics.Sum(metrics, "weir_execution_memory_resident_bytes")
		ingress := testmetrics.Sum(metrics, "weir_execution_memory_ingress_bytes")
		reserved := testmetrics.Sum(metrics, "weir_execution_memory_ingress_reserved_bytes")
		budget := testmetrics.Sum(metrics, "weir_execution_memory_budget_bytes")
		connections := testmetrics.Sum(metrics, "weir_execution_memory_ingress_connections")
		connectionBytes := testmetrics.Sum(metrics, "weir_execution_memory_ingress_connection_bytes")
		valid = valid && connections == 34 && connectionBytes == 34*qualificationIngressConnectionBytes && ingress <= reserved && reserved+connectionBytes <= resident && resident <= used && used <= budget && budget > 0
		if selected >= 0 && valid {
			oracleCtx, cancel := context.WithDeadline(ctx, holdDeadline)
			filter := bson.D{{Key: "_id", Value: bson.D{{Key: "$ne", Value: "hot"}}}}
			count, countErr := condition.options.fixture.Admin.Database(condition.options.fixture.DB).Collection("records").CountDocuments(oracleCtx, filter)
			cancel()
			if countErr != nil || count != 0 || !time.Now().Before(holdDeadline) {
				t.Error("incomplete-body zero-effect oracle failed or arrived after fixed cancellation", count, countErr)
				return -1
			}
			upload := condition.uploads[selected]
			t.Logf("incomplete_body launched=32 returned=0 active_server_RPC_at_least32=true selected_upload=%d selected_stream=1 local_prefix_written=true all32_prefixes_not_required=true selected_connection=%p local=%s remote=%s elapsed=%s remaining_hold=%s Node_used=%g resident=%g budget=%g ingress_bytes=%g ingress_reserved_bytes=%g connection_bytes=%g connections=%g ingress_buffers=%g ingress_messages=%g ingress_waiting=%g execution_input_wire0=true backend_large_effects=0 selected_stream_budget_assignment=not_observed", selected, upload.connection, upload.observer.LocalAddr(), upload.observer.RemoteAddr(), time.Since(condition.started), time.Until(holdDeadline), used, resident, budget, ingress, reserved, connectionBytes, connections, testmetrics.Sum(metrics, "weir_execution_memory_ingress_buffers"), testmetrics.Sum(metrics, "weir_execution_memory_ingress_messages"), testmetrics.Sum(metrics, "weir_execution_memory_ingress_waiting"))
			return selected
		}
		select {
		case <-ctx.Done():
			t.Error("incomplete-body condition not reached before common caller cancellation", ctx.Err())
			return -1
		case <-ticker.C:
		}
	}
	t.Error("partial profile formation not reached inside fixed3s hold: requires32 started/pending calls, real bounded ingress owners and waiting, one incomplete prefix, no execution/DB effects")
	return -1
}

func ingressMemoryAbsent(t *testing.T, fixture *testmongo.Fixture, documents []ingressMemoryDocument) {
	t.Helper()
	ids := make([]string, len(documents))
	for index, document := range documents {
		ids[index] = document.id
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		filter := bson.D{{Key: "_id", Value: document.id}}
		raw, err := fixture.Admin.Database(fixture.DB).Collection("records").FindOne(ctx, filter).Raw()
		cancel()
		if !errors.Is(err, mongo.ErrNoDocuments) {
			t.Error("incomplete message produced an effect or independent per-ID oracle failed", document.id, err)
		}
		t.Logf("independent_partial_effect id=%s absent=%t observed_bytes=%d observed_SHA256=%x", document.id, errors.Is(err, mongo.ErrNoDocuments), len(raw), sha256.Sum256(raw))
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	in := bson.D{{Key: "$in", Value: ids}}
	filter := bson.D{{Key: "_id", Value: in}}
	count, err := fixture.Admin.Database(fixture.DB).Collection("records").CountDocuments(ctx, filter)
	if err != nil || count != 0 {
		t.Error("independent32-ID incomplete-message cardinality must be zero", count, err)
	}
	t.Logf("ingress_partial_effect_summary offered=32 complete_frames=0 mutation_acknowledgements=0 independent_count=%d error=%v acceptance=explicit_caller_cancellation_and_independent_absence", count, err)
}

func ingressMemoryConnectionHealth(t *testing.T, upload *ingressMemoryUpload, dataset *nativeBurstDataset) {
	t.Helper()
	started := time.Now()
	ctx, cancel := context.WithDeadline(t.Context(), started.Add(3*time.Second))
	defer cancel()
	results := make(chan nativeBurstResult, 2)
	for _, kind := range []int{0, 2} {
		go func() {
			job := nativeBurstResult{kind: kind}
			job.err = ingressMemoryConnectionCall(ctx, upload, dataset, kind)
			job.elapsed = time.Since(started)
			results <- job
		}()
	}
	timer := time.NewTimer(time.Until(started.Add(4 * time.Second)))
	defer timer.Stop()
	for range 2 {
		select {
		case job := <-results:
			t.Logf("ingress_same_TCP_health connection=%p local=%s remote=%s large_stream1_incomplete=%t kind=%d caller3s elapsed=%s error=%v", upload.connection, upload.observer.LocalAddr(), upload.observer.RemoteAddr(), upload.observer.allAt.Load() == 0, job.kind, job.elapsed, job.err)
			if job.err != nil || job.elapsed > 3*time.Second || upload.observer.allAt.Load() != 0 {
				t.Error("same-TCP small Read/Native failed or exceeded original3s during incomplete stream1", job.kind, job.elapsed, job.err)
			}
		case <-timer.C:
			t.Error("same-TCP health caller3s plus bounded1s join exhausted")
			return
		}
	}
}

func ingressMemoryConnectionCall(ctx context.Context, upload *ingressMemoryUpload, dataset *nativeBurstDataset, kind int) error {
	var command *pb.Command
	if kind == 0 {
		read := &pb.ReadRequest{Resource: dataset.root + "/s:hot"}
		operation := &pb.Command_Read{Read: read}
		command = &pb.Command{Operation: operation}
	} else {
		operation := &pb.Command_Native{Native: dataset.native}
		command = &pb.Command{Operation: operation}
	}
	request := &pb.ExecuteRequest{StoreName: "mongo", Index: 1, Command: command}
	frame, err := ingressMemoryRequestFrame(request)
	if err != nil {
		return err
	}
	options := ingressMemoryExchangeOptions{connection: upload.connection, address: upload.address, body: bytes.NewReader(frame), length: int64(len(frame))}
	exchange, err := ingressMemoryExchange(ctx, options)
	if err != nil {
		return err
	}
	var raw []byte
	if kind == 0 {
		if len(exchange.messages) != 1 {
			return fmt.Errorf("same-TCP Read requires exactly one response")
		}
		read := exchange.messages[0].GetEvent().GetReadResult()
		if read.GetDocument().GetContentType() != "application/bson" {
			return fmt.Errorf("same-TCP Read lacks BSON document: %v", read)
		}
		raw = read.GetDocument().GetData()
	} else {
		if len(exchange.messages) < 2 || exchange.messages[0].GetEvent().GetHead() == nil || exchange.messages[0].GetEvent().GetHead().BodyContentType != "application/bson" {
			return fmt.Errorf("same-TCP Native lacks first BSON head")
		}
		for index, response := range exchange.messages {
			event := response.GetEvent()
			if index == 0 {
				continue
			}
			if index == len(exchange.messages)-1 {
				end := event.GetNativeEnd()
				if end == nil || end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || end.Failure != nil {
					return fmt.Errorf("same-TCP Native lacks complete final end: %v", end)
				}
				continue
			}
			if _, ok := event.Value.(*pb.Event_Chunk); !ok || len(raw)+len(event.GetChunk()) > 4<<10 {
				return fmt.Errorf("same-TCP Native invalid/oversized body event")
			}
			raw = append(raw, event.GetChunk()...)
		}
	}
	var actual nativeBurstRecord
	var actualDocument bson.Raw
	if kind == 0 {
		actualDocument = raw
		err = bson.Unmarshal(raw, &actual)
	} else {
		var response struct {
			OK     float64 `bson:"ok"`
			Cursor struct {
				Batch []nativeBurstRecord `bson:"firstBatch"`
			} `bson:"cursor"`
		}
		err = bson.Unmarshal(raw, &response)
		if err != nil || response.OK != 1 || len(response.Cursor.Batch) != 1 {
			return fmt.Errorf("same-TCP Native response differs from seed: %v", err)
		}
		actual = response.Cursor.Batch[0]
		batch, ok := bson.Raw(raw).Lookup("cursor", "firstBatch").ArrayOK()
		if !ok {
			return fmt.Errorf("same-TCP Native response lacks BSON firstBatch")
		}
		values, valueErr := batch.Values()
		if valueErr != nil || len(values) != 1 {
			return fmt.Errorf("same-TCP Native firstBatch differs from seeded document: %v", valueErr)
		}
		actualDocument, ok = values[0].DocumentOK()
		if !ok {
			return fmt.Errorf("same-TCP Native firstBatch entry is not a document")
		}
	}
	if err != nil || actual.N != 1 || actual.Pad != strings.Repeat("x", 1<<10) {
		return fmt.Errorf("same-TCP healthy document differs in n/pad: %v", err)
	}
	seed := bson.D{{Key: "_id", Value: "hot"}, {Key: "n", Value: int64(1)}, {Key: "pad", Value: strings.Repeat("x", 1<<10)}}
	expectedDocument, err := bson.Marshal(seed)
	if err != nil || sha256.Sum256(actualDocument) != sha256.Sum256(expectedDocument) {
		return fmt.Errorf("same-TCP healthy document SHA256 differs from independent seed: %v", err)
	}
	return nil
}
