// Command localinit initiates the single-member replica set started by
// scripts/mongo-local.sh and creates its records collection.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const replicaSet = "weir_local"

// MongoDB server error codes.
const (
	alreadyInitialized = 23
	notYetInitialized  = 94
)

func main() {
	address := flag.String("address", "127.0.0.1:27028", "member address of the local mongod")
	flag.Parse()
	if err := initiate(*address); err != nil {
		fmt.Fprintln(os.Stderr, "local replica set initialization failed:", err)
		os.Exit(1)
	}
}

func initiate(address string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	opts := options.Client().ApplyURI("mongodb://" + address + "/?directConnection=true").SetServerSelectionTimeout(10 * time.Second)
	client, err := mongo.Connect(opts)
	if err != nil {
		return err
	}
	defer func() { _ = client.Disconnect(context.Background()) }()
	admin := client.Database("admin")
	status := bson.D{{Key: "replSetGetStatus", Value: 1}}
	err = admin.RunCommand(ctx, status).Err()
	if commandCode(err) == notYetInitialized {
		member := bson.D{{Key: "_id", Value: 0}, {Key: "host", Value: address}}
		configuration := bson.D{{Key: "_id", Value: replicaSet}, {Key: "members", Value: bson.A{member}}}
		command := bson.D{{Key: "replSetInitiate", Value: configuration}}
		err = admin.RunCommand(ctx, command).Err()
		if commandCode(err) == alreadyInitialized {
			err = nil
		}
	}
	if err != nil {
		return err
	}
	if err := waitPrimary(ctx, admin); err != nil {
		return err
	}
	database := client.Database(replicaSet)
	filter := bson.D{{Key: "name", Value: "records"}}
	names, err := database.ListCollectionNames(ctx, filter)
	if err != nil || len(names) != 0 {
		return err
	}
	return database.CreateCollection(ctx, "records")
}

func waitPrimary(ctx context.Context, admin *mongo.Database) error {
	command := bson.D{{Key: "hello", Value: 1}}
	for {
		var hello struct {
			Set     string `bson:"setName"`
			Primary bool   `bson:"isWritablePrimary"`
		}
		err := admin.RunCommand(ctx, command).Decode(&hello)
		if err == nil && hello.Primary {
			if hello.Set != replicaSet {
				return fmt.Errorf("wrong replica set %q", hello.Set)
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("member did not become primary: %w", errors.Join(err, ctx.Err()))
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func commandCode(err error) int32 {
	var command mongo.CommandError
	if errors.As(err, &command) {
		return command.Code
	}
	return 0
}
