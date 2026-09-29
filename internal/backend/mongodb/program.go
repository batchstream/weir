package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protocol"
	"github.com/batchstream/weir/internal/value"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

const programAttempts = 5
const programLifetime = 5 * time.Second
const programCleanup = 200 * time.Millisecond

func (a *Adapter) executeProgram(parent context.Context, work *execution.Plan) ([]*pb.BulkResult, execution.Feedback) {
	native := work.Backend.(*plan)
	result, signal := a.runProgram(parent, native)
	variant := &pb.BulkResult_Mutation{Mutation: result}
	reply := &pb.BulkResult{Index: work.Operation.Index, Result: variant}
	return []*pb.BulkResult{reply}, signal
}

func (a *Adapter) runProgram(parent context.Context, native *plan) (*pb.MutationResult, execution.Feedback) {
	if parent.Err() != nil {
		return protocol.Mutation(pb.MutationOutcome_NOT_STARTED, protocol.ContextFailure(parent)), execution.Neutral
	}
	ctx, cancel := context.WithTimeout(parent, programLifetime)
	defer cancel()
	session, err := a.client.StartSession()
	if err != nil {
		failure := protocol.Fail(pb.FailureCode_UNAVAILABLE, "MongoDB transaction unavailable")
		return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure), execution.Neutral
	}
	identity, err := mongoIdentity(native.id)
	if err != nil {
		session.EndSession(context.Background())
		failure := protocol.Fail(pb.FailureCode_INTERNAL, "record identity encoding failed")
		return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure), execution.Neutral
	}
	ambiguous := false
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), programCleanup)
		defer stop()
		if ambiguous {
			stop()
		}
		bounded, release := nativeAttemptContext(cleanup)
		defer release()
		session.EndSession(bounded)
	}()
	txctx := mongo.NewSessionContext(ctx, session)
	filter := bson.D{{Key: "_id", Value: native.id}}
	for attempt := 0; attempt < programAttempts && ctx.Err() == nil; attempt++ {
		if err := session.StartTransaction(programTransactionOptions()); err != nil {
			failure := protocol.Fail(pb.FailureCode_UNAVAILABLE, "MongoDB transaction could not start")
			return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure), execution.Neutral
		}
		raw, readErr := a.collection.FindOne(txctx, filter).Raw()
		missing := errors.Is(readErr, mongo.ErrNoDocuments)
		if readErr != nil && !missing {
			if shouldRetryProgramTransaction(readErr) && a.abortProgramTransaction(session) {
				if pauseProgram(ctx) {
					continue
				}
				break
			}
			failure := backendFailure(ctx, readErr)
			return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure), feedback(ctx, readErr)
		}
		current := value.Value{Kind: value.Missing}
		if !missing {
			current, err = Decode(raw)
			if err != nil {
				a.abortProgramTransaction(session)
				failure := protocol.Fail(pb.FailureCode_UNSUPPORTED, "stored document contains unsupported BSON values")
				return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure), execution.Neutral
			}
		}
		program := *native.program
		program.Current = current
		transformed, transformErr := a.config.LuaRunner.Run(ctx, program)
		if transformErr != nil {
			a.abortProgramTransaction(session)
			return luaProgramFailure(parent, ctx, transformErr)
		}
		switch transformed.Action {
		case "keep":
			a.abortProgramTransaction(session)
			return protocol.Mutation(pb.MutationOutcome_APPLIED, nil), execution.Healthy
		case "reject":
			a.abortProgramTransaction(session)
			failure := protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, transformed.Message)
			return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure), execution.Healthy
		case "delete":
			if missing {
				a.abortProgramTransaction(session)
				return protocol.Mutation(pb.MutationOutcome_APPLIED, nil), execution.Healthy
			}
			deleted, deleteErr := a.collection.DeleteOne(txctx, filter)
			if deleteErr == nil && deleted.DeletedCount != 1 {
				deleteErr = errProgramWriteConflict
			}
			if deleteErr != nil {
				if shouldRetryProgramWrite(deleteErr, missing) && a.abortProgramTransaction(session) && pauseProgram(ctx) {
					continue
				}
				a.abortProgramTransaction(session)
				failure := backendFailure(ctx, deleteErr)
				return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure), feedback(ctx, deleteErr)
			}
		case "replace":
			replacement, valid := withMongoIdentity(transformed.Value, identity, native.id)
			if !valid {
				a.abortProgramTransaction(session)
				failure := protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "Lua replacement changes record identity")
				return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure), execution.Neutral
			}
			encoded, encodeErr := Encode(replacement)
			if encodeErr != nil {
				a.abortProgramTransaction(session)
				failure := protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "Lua replacement exceeds BSON limits")
				return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure), execution.Neutral
			}
			if missing {
				_, err = a.collection.InsertOne(txctx, encoded)
			} else {
				var updated *mongo.UpdateResult
				updated, err = a.collection.ReplaceOne(txctx, filter, encoded)
				if err == nil && updated.MatchedCount != 1 {
					err = errProgramWriteConflict
				}
			}
			if err != nil {
				if shouldRetryProgramWrite(err, missing) && a.abortProgramTransaction(session) && pauseProgram(ctx) {
					continue
				}
				a.abortProgramTransaction(session)
				failure := backendFailure(ctx, err)
				return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure), feedback(ctx, err)
			}
		default:
			a.abortProgramTransaction(session)
			failure := protocol.Fail(pb.FailureCode_INTERNAL, "Lua worker returned an invalid action")
			return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure), execution.Neutral
		}
		if ctx.Err() != nil {
			break
		}
		retryTransaction := false
		for commitAttempt := 0; commitAttempt < programAttempts && ctx.Err() == nil; commitAttempt++ {
			commitContext, release := nativeAttemptContext(txctx)
			err = session.CommitTransaction(commitContext)
			release()
			if err == nil {
				return protocol.Mutation(pb.MutationOutcome_APPLIED, nil), execution.Healthy
			}
			if !ambiguous && !programHasLabel(err, "UnknownTransactionCommitResult") && programHasLabel(err, "TransientTransactionError") {
				retryTransaction = true
				break
			}
			ambiguous = true
			if !pauseProgram(ctx) {
				break
			}
		}
		if !retryTransaction {
			break
		}
		if !a.abortProgramTransaction(session) || !pauseProgram(ctx) {
			break
		}
	}
	if parent.Err() != nil {
		outcome := pb.MutationOutcome_NOT_APPLIED
		if ambiguous {
			outcome = pb.MutationOutcome_UNKNOWN
		}
		return protocol.Mutation(outcome, protocol.ContextFailure(parent)), execution.Neutral
	}
	outcome := pb.MutationOutcome_NOT_APPLIED
	code := pb.FailureCode_CONFLICT
	message := "MongoDB transaction retry limit exceeded"
	if ambiguous {
		outcome = pb.MutationOutcome_UNKNOWN
		code = pb.FailureCode_UNAVAILABLE
		message = "MongoDB commit acknowledgement is ambiguous"
	}
	if ctx.Err() != nil {
		outcome = pb.MutationOutcome_NOT_APPLIED
		if ambiguous {
			outcome = pb.MutationOutcome_UNKNOWN
		}
		code = pb.FailureCode_DEADLINE_EXCEEDED
		message = "Lua transform execution deadline exceeded"
	}
	failure := protocol.Fail(code, message)
	return protocol.Mutation(outcome, failure), execution.Neutral
}

var errProgramWriteConflict = errors.New("program write conflict")

func mongoIdentity(id any) (value.Field, error) {
	identityDocument := bson.D{{Key: "_id", Value: id}}
	raw, err := bson.Marshal(identityDocument)
	if err != nil {
		var zero value.Field
		return zero, err
	}
	identity, err := Decode(raw)
	if err != nil || len(identity.Fields) != 1 {
		var zero value.Field
		return zero, fmt.Errorf("invalid encoded identity")
	}
	return identity.Fields[0], nil
}

func withMongoIdentity(document value.Value, identity value.Field, id any) (value.Value, bool) {
	if document.Kind != value.Object || value.Validate(document) != nil {
		return value.Value{}, false
	}
	existing, err := document.Lookup("_id")
	if err != nil || existing.Kind != value.Missing && !equalID(existing, id) {
		return value.Value{}, false
	}
	result := value.Value{Kind: value.Object, Fields: make([]value.Field, 0, len(document.Fields)+1)}
	result.Fields = append(result.Fields, identity)
	for _, field := range document.Fields {
		if field.Name != "_id" {
			result.Fields = append(result.Fields, field)
		}
	}
	if err := value.Validate(result); err != nil {
		return value.Value{}, false
	}
	return result, true
}

func luaProgramFailure(parent, ctx context.Context, err error) (*pb.MutationResult, execution.Feedback) {
	if parent.Err() != nil {
		return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, protocol.ContextFailure(parent)), execution.Neutral
	}
	code := pb.FailureCode_INVALID_ARGUMENT
	message := "Lua program evaluation failed"
	if errors.Is(err, context.DeadlineExceeded) {
		code = pb.FailureCode_DEADLINE_EXCEEDED
		message = "Lua program execution limit exceeded"
	} else if ctx.Err() != nil {
		code = pb.FailureCode_DEADLINE_EXCEEDED
		message = "Lua transform execution deadline exceeded"
	}
	failure := protocol.Fail(code, message)
	return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure), execution.Neutral
}

func shouldRetryProgramTransaction(err error) bool {
	return programHasLabel(err, "TransientTransactionError")
}

func shouldRetryProgramWrite(err error, missing bool) bool {
	if shouldRetryProgramTransaction(err) {
		return true
	}
	if missing {
		var write mongo.WriteException
		if !errors.As(err, &write) {
			return false
		}
		for _, item := range write.WriteErrors {
			if item.Code == 11000 {
				keyPattern := item.Details.Lookup("keyPattern")
				if keyPattern.Type == bson.TypeEmbeddedDocument {
					fields, fieldErr := keyPattern.Document().Elements()
					return fieldErr == nil && len(fields) == 1 && fields[0].Key() == "_id"
				}
			}
		}
	}
	return errors.Is(err, errProgramWriteConflict)
}

func programHasLabel(err error, label string) bool {
	var serverError mongo.ServerError
	return errors.As(err, &serverError) && serverError.HasErrorLabel(label)
}

func (a *Adapter) abortProgramTransaction(session *mongo.Session) bool {
	cleanup, cancel := context.WithTimeout(context.Background(), programCleanup)
	defer cancel()
	bounded, release := nativeAttemptContext(cleanup)
	defer release()
	err := session.AbortTransaction(bounded)
	return err == nil
}

func pauseProgram(ctx context.Context) bool {
	timer := time.NewTimer(5 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func programTransactionOptions() *options.TransactionOptionsBuilder {
	return options.Transaction().SetReadConcern(readconcern.Snapshot()).SetWriteConcern(writeconcern.Majority()).SetReadPreference(readpref.Primary())
}
