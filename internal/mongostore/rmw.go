package mongostore

import (
	"context"
	"errors"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/protocol"
	"github.com/batchstream/weir/internal/value"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

const MaxTransactionAttempts = 5
const MaxCommitAttempts = 5
const TransactionCleanup = 200 * time.Millisecond

type RMWReport struct {
	Result                         *pb.MutationResult
	Attempts, Evaluations, Commits int
}

// IncrementConformance exercises the native RMW foundation using a finite deterministic
// counter transform. It is intentionally not wired to AtomicTransform or any public RPC.
func (a *Adapter) IncrementConformance(ctx context.Context, p *Plan) RMWReport {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	report := RMWReport{}
	if ctx.Err() != nil {
		report.Result = protocol.Mutation(pb.MutationOutcome_NOT_STARTED, protocol.ContextFailure(ctx))
		return report
	}
	session, err := a.client.StartSession()
	if err != nil {
		report.Result = protocol.Mutation(pb.MutationOutcome_NOT_STARTED, protocol.Fail(pb.FailureCode_UNAVAILABLE, "session unavailable"))
		return report
	}
	ambiguous := false
	deadline, _ := ctx.Deadline()
	cleanupDeadline := deadline.Add(TransactionCleanup)
	defer func() {
		cleanup, stop := context.WithDeadline(context.Background(), minTime(time.Now().Add(TransactionCleanup), cleanupDeadline))
		defer stop()
		if ambiguous {
			stop()
		}
		bounded, release := nativeAttemptContext(cleanup)
		defer release()
		session.EndSession(bounded)
	}()
	txctx := mongo.NewSessionContext(ctx, session)
	filter := bson.D{{Key: "_id", Value: p.id}}
	for report.Attempts < MaxTransactionAttempts && ctx.Err() == nil {
		report.Attempts++
		if err = session.StartTransaction(transactionOptions()); err != nil {
			break
		}
		raw, readErr := a.collection.FindOne(txctx, filter).Raw()
		missing := errors.Is(readErr, mongo.ErrNoDocuments)
		if readErr != nil && !missing {
			err = readErr
		} else {
			current := value.Value{}
			if !missing {
				current, err = Decode(raw)
			} else {
				err = nil
			}
			if err == nil {
				idDoc := bson.D{{Key: "_id", Value: p.id}}
				var encoded []byte
				encoded, err = bson.Marshal(idDoc)
				var idValue value.Value
				if err == nil {
					idValue, err = Decode(encoded)
				}
				if err == nil {
					report.Evaluations++
					var next value.Value
					next, err = value.Increment(current, idValue.Fields[0])
					if err == nil {
						encoded, err = Encode(next)
					}
					if err == nil {
						if missing {
							_, err = a.collection.InsertOne(txctx, bson.Raw(encoded))
						} else {
							_, err = a.collection.ReplaceOne(txctx, filter, bson.Raw(encoded))
						}
					}
				}
			}
		}
		if err != nil {
			transient := hasLabel(err, "TransientTransactionError")
			cleanup, stop := context.WithDeadline(context.Background(), minTime(time.Now().Add(TransactionCleanup), cleanupDeadline))
			bounded, release := nativeAttemptContext(cleanup)
			abortErr := session.AbortTransaction(bounded)
			release()
			stop()
			// A duplicate _id race is retried only with explicit index evidence and a confirmed abort.
			conflict := transient || missing && duplicateID(err) && abortErr == nil
			if !conflict {
				report.Result = protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, "RMW rejected before commit"))
				return report
			}
			if !pause(ctx) {
				break
			}
			continue
		}
		if ctx.Err() != nil {
			break
		}
		retryTransaction := false
		for report.Commits < MaxCommitAttempts && ctx.Err() == nil {
			report.Commits++
			commitContext, release := nativeAttemptContext(txctx)
			err = session.CommitTransaction(commitContext)
			release()
			if err == nil {
				report.Result = protocol.Mutation(pb.MutationOutcome_APPLIED, nil)
				return report
			}
			if !ambiguous && hasLabel(err, "TransientTransactionError") {
				retryTransaction = true
				break
			}
			// Once a commit is sent, anything except definite transaction-abort
			// evidence is ambiguous. A later ordinary error never clears this latch.
			ambiguous = true
			if !pause(ctx) {
				break
			}
		}
		if !retryTransaction {
			break
		}
		if !pause(ctx) {
			break
		}
	}
	outcome := pb.MutationOutcome_NOT_APPLIED
	code := pb.FailureCode_CONFLICT
	if ambiguous {
		outcome = pb.MutationOutcome_UNKNOWN
		code = pb.FailureCode_UNAVAILABLE
	}
	f := protocol.Fail(code, "RMW attempt/commit budget exhausted")
	if ctx.Err() != nil {
		f = protocol.ContextFailure(ctx)
	}
	report.Result = protocol.Mutation(outcome, f)
	return report
}
func hasLabel(err error, label string) bool {
	var se mongo.ServerError
	return errors.As(err, &se) && se.HasErrorLabel(label)
}
func duplicateID(err error) bool {
	var we mongo.WriteException
	if !errors.As(err, &we) {
		return false
	}
	for _, e := range we.WriteErrors {
		if e.Code == 11000 {
			v := e.Details.Lookup("keyPattern")
			if v.Type == bson.TypeEmbeddedDocument {
				d := v.Document()
				elements, err := d.Elements()
				return err == nil && len(elements) == 1 && elements[0].Key() == "_id"
			}
		}
	}
	return false
}
func pause(ctx context.Context) bool {
	timer := time.NewTimer(5 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// Driver v2.9.1 changes required commit retry-once into unlimited retries when
// Deadline is present (CSOT). Bridge the parent's cancellation into a deadline-free
// context, retaining session values and native retry-once. The driver's socket
// listener only closes on Canceled, not DeadlineExceeded without a socket deadline.
// The enclosing operation retains the original deadline/error. No client-level
// Timeout is configured. Tests qualify both actual reply loss and blocked commits.
func nativeAttemptContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	stop := context.AfterFunc(parent, cancel)
	if parent.Err() != nil {
		cancel()
	}
	release := func() { stop(); cancel() }
	return ctx, release
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
