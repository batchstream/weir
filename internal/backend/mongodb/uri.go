package mongodb

import (
	"errors"
	"strings"

	"github.com/batchstream/weir/internal/backend"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/connstring"
)

// ValidateConfig is pure: credentials and trust files remain separate from the URI.
func ValidateConfig(cfg Config) error {
	_, err := parseConfig(cfg)
	return err
}

func parseConfig(cfg Config) (*connstring.ConnString, error) {
	if (cfg.Username == "") != (cfg.Password == "") || !backend.ValidText(cfg.Username) || !backend.ValidText(cfg.Password) {
		return nil, errors.New("invalid MongoDB credential pair")
	}
	uri, err := parseURI(cfg.URI)
	if err != nil {
		return nil, err
	}
	// parseURI allows tls only with SCRAM, so this covers both.
	if (cfg.Username != "") != (uri.AuthMechanism != "") {
		return nil, errors.New("MongoDB credentials and SCRAM require each other")
	}
	if cfg.CAFile != "" && !uri.SSL {
		return nil, errors.New("MongoDB CA file requires TLS")
	}
	return uri, nil
}

// parseURI holds a URI to Weir's connection policy. Errors stay fixed because
// the driver's repeat the URI.
func parseURI(raw string) (*connstring.ConnString, error) {
	// Validation does no network IO, but parsing mongodb+srv:// resolves DNS.
	if !strings.HasPrefix(raw, "mongodb://") || strings.Contains(raw, "@") {
		return nil, errors.New("MongoDB URI requires the mongodb:// scheme and separate credentials")
	}
	uri, err := connstring.Parse(raw)
	if err != nil {
		return nil, errors.New("invalid MongoDB URI")
	}
	if uri.Database != "" {
		return nil, errors.New("MongoDB URI path is unsupported; the resource supplies the database")
	}
	// The client is built from these options alone, so any other would be
	// silently ignored.
	for name := range uri.Options {
		switch name {
		case "directconnection", "replicaset", "authsource", "authmechanism", "tls":
		default:
			return nil, errors.New("unsupported MongoDB URI option")
		}
	}
	if _, named := uri.Options["replicaset"]; named {
		if uri.ReplicaSet == "" || uri.DirectConnectionSet && uri.DirectConnection {
			return nil, errors.New("MongoDB replica-set discovery requires a name and cannot use direct mode")
		}
	} else if len(uri.Hosts) != 1 || uri.DirectConnectionSet && !uri.DirectConnection {
		return nil, errors.New("MongoDB discovery requires an explicit replica-set name")
	}
	// Authentication must state tls. tls=false sends SCRAM-SHA-256, which never
	// reveals the password, and all data unencrypted, so it belongs only on a
	// network that only the backend and its clients can reach.
	if uri.AuthMechanism != "" || uri.AuthSourceSet || uri.SSLSet {
		if uri.AuthMechanism != "SCRAM-SHA-256" || !uri.AuthSourceSet || !validDatabaseName(uri.AuthSource) || !uri.SSLSet {
			return nil, errors.New("MongoDB URI authentication requires explicit SCRAM-SHA-256, authSource, and tls")
		}
	}
	return uri, nil
}
