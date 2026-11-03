package mongodb

import (
	"crypto/tls"
	"time"

	"github.com/batchstream/weir/internal/backend"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/connstring"
)

// The URI must be parsed before any trust-file or network access. The driver
// verifies each member's certificate, including OCSP.
func connectionOptions(cfg Config, uri *connstring.ConnString) (*options.ClientOptions, error) {
	opts := options.Client().SetHosts(uri.Hosts).SetDirect(uri.ReplicaSet == "")
	if uri.ReplicaSet != "" {
		opts.SetReplicaSet(uri.ReplicaSet).SetHeartbeatInterval(time.Second)
	}
	if cfg.Username != "" {
		credential := options.Credential{
			AuthMechanism: uri.AuthMechanism,
			AuthSource:    uri.AuthSource,
			Username:      cfg.Username,
			Password:      cfg.Password,
			PasswordSet:   true,
		}
		opts.SetAuth(credential)
	}
	if uri.SSL {
		config := &tls.Config{}
		if cfg.CAFile != "" {
			pool, err := backend.CertPool(cfg.CAFile)
			if err != nil {
				return nil, err
			}
			config.RootCAs = pool
		}
		opts.SetTLSConfig(config)
	}
	return opts, nil
}
