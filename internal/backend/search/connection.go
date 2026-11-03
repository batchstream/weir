package search

import (
	"crypto/tls"
	"errors"
	"net/url"
	"strings"

	"github.com/batchstream/weir/internal/backend"
)

// ValidateConfig is pure: app validates the complete graph with these same
// rules before Open can read a CA file, resolve a hostname or contact a backend.
func ValidateConfig(cfg Config) error {
	endpoint, err := canonicalURL(cfg.URL)
	if err != nil {
		return err
	}
	if (cfg.Username != "" || cfg.CAFile != "") && !strings.HasPrefix(endpoint, "https://") {
		return errors.New("Search credentials and CA file require HTTPS")
	}
	if (cfg.Username == "") != (cfg.Password == "") || strings.Contains(cfg.Username, ":") ||
		!backend.ValidText(cfg.Username) || !backend.ValidText(cfg.Password) {
		return errors.New("invalid Search credential pair")
	}
	return nil
}

// canonicalURL requires an endpoint that names only a scheme, host and port.
// A host that cannot be reached fails when Open qualifies the backend.
func canonicalURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" && u.Scheme != "https" || u.User != nil || u.Path != "" ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Port() == "" {
		return "", errors.New("Search requires one explicit HTTP or HTTPS host and port")
	}
	return u.Scheme + "://" + u.Host, nil
}

func connectionTLS(cfg Config) (*tls.Config, error) {
	if !strings.HasPrefix(cfg.URL, "https://") {
		return nil, nil
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, NextProtos: []string{"http/1.1"}}
	if cfg.CAFile != "" {
		roots, err := backend.CertPool(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		config.RootCAs = roots
	}
	return config, nil
}
