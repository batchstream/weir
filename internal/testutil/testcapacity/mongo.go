package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

func newMongoClient(uri string, pool int) (*mongo.Client, error) {
	opts := options.Client().ApplyURI(uri).
		SetDirect(true).SetMaxPoolSize(uint64(pool)).SetMaxConnecting(2).
		SetRetryReads(false).SetRetryWrites(false).SetMaxAdaptiveRetries(0).
		SetEnableOverloadRetargeting(false).SetCompressors(nil).
		SetServerMonitoringMode(options.ServerMonitoringModePoll).
		SetServerSelectionTimeout(2 * time.Second).SetConnectTimeout(2 * time.Second).
		SetWriteConcern(writeconcern.Majority())
	return mongo.Connect(opts)
}

// Both paths use exactly 1 KiB BSON with the resource ID first.
func mongoPayload(id string) bson.Raw {
	builder := bsoncore.NewDocumentBuilder().AppendString("_id", id).AppendString("data", "")
	overhead := len(builder.Build())
	data := payload(id)
	builder = bsoncore.NewDocumentBuilder().AppendString("_id", id).
		AppendString("data", string(data[9:9+1024-overhead]))
	return bson.Raw(builder.Build())
}

func (c *Client) directMongo(ctx context.Context, op Operation) Result {
	filter := bson.D{{Key: "_id", Value: op.ID}}
	if !op.Write {
		raw, err := c.Mongo.Database("weir_load").Collection("records").FindOne(ctx, filter).Raw()
		if err != nil {
			return failure("transport_mongo", false)
		}
		if !bytes.Equal(raw, mongoPayload(op.ID)) {
			return failure("payload_or_response", false)
		}
		result := Result{Class: "ok"}
		return result
	}
	model := mongo.NewClientReplaceOneModel().SetFilter(filter).
		SetReplacement(mongoPayload(op.ID)).SetUpsert(true)
	write := mongo.ClientBulkWrite{Database: "weir_load", Collection: "records", Model: model}
	writes := []mongo.ClientBulkWrite{write}
	opts := options.ClientBulkWrite().SetOrdered(false).SetVerboseResults(true)
	ack, err := c.Mongo.BulkWrite(ctx, writes, opts)
	if err != nil {
		return failure("transport_mongo", true)
	}
	if !ack.Acknowledged || ack.UpsertedCount != 1 || ack.MatchedCount != 0 {
		return failure("invalid_ack", true)
	}
	result := Result{Class: "ok", Outcome: applied}
	return result
}

// The fixture owns this entire server; no user database is ever reset.
func (c *Client) setupMongo(ctx context.Context, encoder *json.Encoder) error {
	database := c.Mongo.Database("weir_load")
	if err := database.Collection("records").Drop(ctx); err != nil {
		return err
	}
	if err := database.CreateCollection(ctx, "records"); err != nil {
		return err
	}
	for i := 0; i < corpusSize; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		progress := map[string]any{"type": "setup_progress", "started": i + 1}
		if err := encoder.Encode(progress); err != nil {
			return err
		}
		op := Operation{ID: fmt.Sprintf("read-%04d", i), Write: true}
		c.MutationsStarted.Add(1)
		result := c.directMongo(ctx, op)
		if result.Class != "ok" {
			return fmt.Errorf("seed %d: %s", i, result.Class)
		}
	}
	return nil
}

func (c *Client) auditMongo(ctx context.Context, trial *Trial) (Audit, error) {
	audit := Audit{Planned: len(trial.Ledger)}
	collection := c.Mongo.Database("weir_load").Collection("records")
	for start := 0; start < len(trial.Ledger); start += 100 {
		end := min(start+100, len(trial.Ledger))
		ids := make([]string, 0, end-start)
		for n := start; n < end; n++ {
			ids = append(ids, fmt.Sprintf("%s-%06d", trial.Options.Prefix, n))
		}
		selector := bson.D{{Key: "$in", Value: ids}}
		filter := bson.D{{Key: "_id", Value: selector}}
		page, cancel := context.WithTimeout(ctx, 2*time.Second)
		cursor, err := collection.Find(page, filter)
		if err != nil {
			cancel()
			return audit, err
		}
		found := make(map[string]bson.Raw, len(ids))
		for cursor.Next(page) {
			raw := append(bson.Raw(nil), cursor.Current...)
			id, ok := raw.Lookup("_id").StringValueOK()
			if !ok || found[id] != nil {
				err = errors.New("invalid or duplicate audit ID")
				break
			}
			found[id] = raw
		}
		err = errors.Join(err, cursor.Err(), cursor.Close(page))
		cancel()
		if err != nil {
			return audit, err
		}
		audit.Pages++
		for i, id := range ids {
			outcome := trial.Ledger[start+i]
			raw, exists := found[id]
			if exists {
				audit.FoundDocuments++
				if !bytes.Equal(raw, mongoPayload(id)) {
					return audit, fmt.Errorf("audit payload %s", id)
				}
				if outcome != applied && outcome != unknown {
					return audit, fmt.Errorf("false non-application %s", id)
				}
				if outcome == unknown {
					audit.UnknownFound++
				}
			} else {
				audit.Absent++
				if outcome == applied {
					return audit, fmt.Errorf("false APPLIED %s", id)
				}
			}
			if outcome == applied {
				audit.Applied++
			}
		}
	}
	return audit, nil
}
