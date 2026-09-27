package mongostore

import (
	"strings"
	"testing"
)

func TestValidateMongoURIProfiles(t *testing.T) {
	for _, uri := range []string{
		"mongodb://127.0.0.1:27028/?directConnection=true&serverMonitoringMode=poll",
		"mongodb://[::1]:27028/",
	} {
		if err := ValidateURI(uri); err != nil {
			t.Fatalf("rejected supported profile: %v", err)
		}
	}
	secureURI := "mongodb://user%40name:p%40ss@mongo.example.test:27028/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=true&tlsCAFile=%2Fetc%2Fweir%2Fmongo-ca.pem"
	if err := ValidateURI(secureURI); err != nil {
		t.Fatalf("secure profile was rejected: %v", err)
	}

}

func TestValidateMongoURIRejectsOutsideProfile(t *testing.T) {
	secret := "private-sentinel"
	for _, uri := range []string{
		"mongodb+srv://mongo.example.test/",
		"mongodb://127.0.0.1/",
		"mongodb://127.0.0.1:0/",
		"mongodb://127.0.0.1:27028,127.0.0.2:27028/",
		"mongodb://127.0.0.1:27028/test/",
		"mongodb://user:" + secret + "@127.0.0.1:27028/?authSource=admin&tls=true",
		"mongodb://user:" + secret + "@127.0.0.1:27028/?authMechanism=MONGODB-OIDC&authSource=admin&tls=true",
		"mongodb://user:" + secret + "@127.0.0.1:27028/?authMechanism=SCRAM-SHA-256&authSource=admin",
		"mongodb://user:" + secret + "@127.0.0.1:27028/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=true&tlsInsecure=true",
		"mongodb://user:" + secret + "@127.0.0.1:27028/?authMechanism=SCRAM-SHA-256&authMechanism=SCRAM-SHA-256&authSource=admin&tls=true",
		"mongodb://user:" + secret + "@127.0.0.1:27028/?authMechanism=SCRAM-SHA-256&authSource=admin&authsource=admin&tls=true",
		"mongodb://127.0.0.1:27028/?directConnection=false",
		"mongodb://127.0.0.1:27028/?serverMonitoringMode=stream",
		"mongodb://127.0.0.1:27028/?retryWrites=true",
		"mongodb://127.0.0.1:27028/?authSource=",
		"mongodb://127.0.0.1:27028/?authMechanism=",
		"mongodb://127.0.0.1:27028/?tls=",
		"mongodb://127.0.0.1:27028/?tlsCAFile=",
		"mongodb://127.0.0.1:27028/?tls=true",
		"mongodb://127.0.0.1:27028/?tlsCAFile=/tmp/ca.pem",
		"mongodb://user:" + secret + "@127.0.0.1:27028/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=true&tlsCAFile=bad%0Apath",
		strings.Repeat("a", maxMongoURIBytes+1),
	} {
		err := ValidateURI(uri)
		if err == nil {
			t.Fatalf("accepted unsupported URI")
		}
		if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "bad") {
			t.Fatalf("URI details leaked: %v", err)
		}
	}
}
