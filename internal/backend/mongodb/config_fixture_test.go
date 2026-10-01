package mongodb

import (
	"net/url"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func collectionQualificationResponse(database, collection string) bson.D {
	specification := bson.D{
		{Key: "name", Value: collection},
		{Key: "type", Value: "collection"},
		{Key: "options", Value: bson.D{}},
	}
	cursor := bson.D{
		{Key: "id", Value: int64(0)},
		{Key: "ns", Value: database + ".$cmd.listCollections"},
		{Key: "firstBatch", Value: bson.A{specification}},
	}
	response := bson.D{{Key: "ok", Value: 1}, {Key: "cursor", Value: cursor}}
	return response
}

// Owned backend fixtures expose driver URIs; production accepts separate values.
func mongoFixtureConfig(t *testing.T, cfg Config) Config {
	t.Helper()
	parsed, err := url.Parse(cfg.URI)
	if err != nil {
		t.Fatal("invalid owned MongoDB fixture URI")
	}
	if parsed.User != nil {
		cfg.Username = parsed.User.Username()
		cfg.Password, _ = parsed.User.Password()
		parsed.User = nil
	}
	cfg.URI = parsed.String()
	return cfg
}
