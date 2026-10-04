//go:build integration

package testmongo

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestSecureMongoFixture(t *testing.T) {
	if os.Getenv("WEIR_MONGO_SECURE_INTEGRATION") != "1" {
		t.Skip("secure MongoDB fixture is explicit opt-in")
	}
	fixture := OpenSecure(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := fixture.Client.Ping(ctx, nil); err != nil {
		t.Fatal("secure fixture cannot authenticate")
	}
	hello := bson.D{{Key: "hello", Value: 1}}
	if err := fixture.Client.Database("admin").RunCommand(ctx, hello).Err(); err != nil {
		t.Fatal("fixture user cannot query replica-set metadata")
	}
	buildInfo := bson.D{{Key: "buildInfo", Value: 1}}
	if err := fixture.Client.Database("admin").RunCommand(ctx, buildInfo).Err(); err != nil {
		t.Fatal("fixture user cannot query server version metadata")
	}
	filter := bson.D{{Key: "name", Value: "records"}}
	specifications, err := fixture.Client.Database(fixture.DB).ListCollectionSpecifications(ctx, filter)
	if err != nil || len(specifications) != 1 {
		t.Fatal("fixture user cannot inspect the configured collection")
	}
	collection := fixture.Client.Database(fixture.DB).Collection("records")
	document := bson.D{{Key: "_id", Value: "secure"}, {Key: "value", Value: int32(1)}}
	documentFilter := bson.D{{Key: "_id", Value: "secure"}}
	if _, err := collection.InsertOne(ctx, document); err != nil {
		t.Fatal("fixture role cannot insert")
	}
	if _, err := collection.FindOne(ctx, documentFilter).Raw(); err != nil {
		t.Fatal("fixture role cannot read")
	}
	update := bson.D{{Key: "$inc", Value: bson.D{{Key: "value", Value: int32(1)}}}}
	if _, err := collection.UpdateOne(ctx, documentFilter, update); err != nil {
		t.Fatal("fixture role cannot update")
	}
	if _, err := collection.DeleteOne(ctx, documentFilter); err != nil {
		t.Fatal("fixture role cannot delete")
	}
	for _, uri := range []string{fixture.BadPassURI, fixture.WrongHostURI, fixture.BadCAURI, fixture.MissingPassURI} {
		assertSecureConnectionRejected(t, uri)
	}
	deniedOptions := options.Client().ApplyURI(fixture.DeniedURI).SetMaxPoolSize(4).SetMaxConnecting(2).SetConnectTimeout(2 * time.Second).SetServerSelectionTimeout(2 * time.Second)
	deniedClient, err := mongo.Connect(deniedOptions)
	if err != nil {
		t.Fatal("cannot configure insufficient-privilege fixture client")
	}
	defer fixture.disconnect(t, deniedClient)
	deniedContext, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	_, err = deniedClient.Database(fixture.DB).ListCollectionSpecifications(deniedContext, filter)
	var commandError mongo.CommandError
	if !errors.As(err, &commandError) || commandError.Code != 13 {
		t.Fatal("fixture user without database privileges was accepted")
	}
}

func assertSecureConnectionRejected(t *testing.T, uri string) {
	t.Helper()
	clientOptions := options.Client().ApplyURI(uri).SetMaxPoolSize(4).SetMaxConnecting(2).SetConnectTimeout(2 * time.Second).SetServerSelectionTimeout(2 * time.Second)
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		return
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := client.Disconnect(cleanup); err != nil {
			t.Error("cannot close negative TLS/SCRAM client")
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx, nil); err == nil {
		t.Fatal("invalid TLS/SCRAM connection was accepted")
	}
}
