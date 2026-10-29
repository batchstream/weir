package mongodb

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var errTLSProfile = errors.New("invalid MongoDB TLS profile")

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
	// Reject special files before Open, which could otherwise block on a FIFO.
	info, err := os.Stat(name)
	if err != nil {
		return nil, errors.New("MongoDB CA file unavailable")
	}
	if !info.Mode().IsRegular() {
		return nil, errTLSProfile
	}
	file, err := os.Open(name)
	if err != nil {
		return nil, errors.New("MongoDB CA file unavailable")
	}
	defer file.Close()
	info, err = file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, errTLSProfile
	}
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, errTLSProfile
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(data) {
		return nil, errors.New("invalid MongoDB CA file")
	}
	return roots, nil
}
