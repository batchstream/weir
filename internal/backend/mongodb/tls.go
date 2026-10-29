package mongodb

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"time"

	"github.com/batchstream/weir/internal/backend"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// The profile must be parsed before any trust-file or network access. The
// driver verifies each member's certificate, including OCSP.
func connectionOptions(cfg Config, profile connectionProfile) (*options.ClientOptions, error) {
	opts := options.Client().SetHosts(profile.hosts).SetDirect(profile.direct)
	if profile.replicaSet != "" {
		opts.SetReplicaSet(profile.replicaSet).SetHeartbeatInterval(time.Second)
	}
	if cfg.Username != "" {
		credential := options.Credential{
			AuthMechanism: profile.authMechanism,
			AuthSource:    profile.authSource,
			Username:      cfg.Username,
			Password:      cfg.Password,
			PasswordSet:   true,
		}
		opts.SetAuth(credential)
	}
	if profile.tls {
		config := &tls.Config{}
		if cfg.CAFile != "" {
			pool, err := mongoRoots(cfg.CAFile)
			if err != nil {
				return nil, err
			}
			config.RootCAs = pool
		}
		opts.SetTLSConfig(config)
	}
	return opts, nil
}

func mongoRoots(name string) (*x509.CertPool, error) {
	data, err := backend.ReadRegularFile(name)
	if err != nil {
		return nil, errors.New("MongoDB CA file unavailable")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(data) {
		return nil, errors.New("invalid MongoDB CA file")
	}
	return roots, nil
}
