package mongodb

import (
	"net/url"
	"testing"
)

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
