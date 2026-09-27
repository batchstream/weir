package search

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/netip"
	"net/url"
	"os"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/batchstream/weir/internal/protocol"
)

// Connection is static backend client configuration, never caller identity.
// HTTPS without this block uses the system trust roots and no credentials.
type Connection struct {
	Username string `json:"username"`
	Password string `json:"password"`
	CAFile   string `json:"ca_file"`
}

// ValidateConfig is pure: app validates the complete graph with these same
// rules before Open can read a CA file, resolve a hostname or contact a backend.
func ValidateConfig(cfg Config) error {
	name, segments, err := protocol.ParseResource("weir://" + cfg.Store)
	if err != nil || name != cfg.Store || len(segments) != 0 || !indexPattern.MatchString(cfg.Index) || cfg.Pool < 1 || cfg.Pool > 32 {
		return errors.New("invalid Search configuration")
	}
	if cfg.Profile != ElasticsearchProfile && cfg.Profile != OpenSearchProfile {
		return errors.New("unsupported Search profile")
	}
	endpoint, err := canonicalURL(cfg.URL)
	if err != nil {
		return err
	}
	if c := cfg.Connection; c != nil {
		if !strings.HasPrefix(endpoint, "https://") || c.Username == "" && c.Password == "" && c.CAFile == "" {
			return errors.New("Search connection options require HTTPS and explicit content")
		}
		if (c.Username == "") != (c.Password == "") || len(c.Username) > 128 || len(c.Password) > 256 || strings.Contains(c.Username, ":") || !safeText(c.Username) || !safeText(c.Password) {
			return errors.New("invalid Search credential pair")
		}
		if len(c.CAFile) > 2048 || !safeText(c.CAFile) {
			return errors.New("invalid Search CA file configuration")
		}
	}
	return nil
}

func safeText(value string) bool {
	if !utf8.ValidString(value) {
		return false
	}
	for _, char := range value {
		if char < 32 || char == 127 {
			return false
		}
	}
	return true
}

func canonicalURL(raw string) (string, error) {
	invalid := errors.New("Search requires one explicit HTTP or HTTPS host and port")
	if len(raw) > 1024 || strings.ContainsAny(raw, "%?#\\") || strings.TrimSpace(raw) != raw {
		return "", invalid
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" && u.Scheme != "https" || u.User != nil || u.Opaque != "" || u.Path != "" || u.RawPath != "" || u.ForceQuery {
		return "", invalid
	}
	host, port, err := net.SplitHostPort(u.Host)
	number, portErr := strconv.Atoi(port)
	if err != nil || portErr != nil || number < 1 || number > 65535 || strings.Trim(port, "0123456789") != "" {
		return "", invalid
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		if ip.Zone() != "" || ip.IsUnspecified() || ip.IsMulticast() {
			return "", invalid
		}
		host = ip.Unmap().String()
	} else {
		if len(host) == 0 || len(host) > 253 || strings.Trim(host, "0123456789.") == "" || strings.ContainsAny(u.Host, "[]") {
			return "", invalid
		}
		for _, label := range strings.Split(host, ".") {
			if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
				return "", invalid
			}
			for _, c := range label {
				if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-') {
					return "", invalid
				}
			}
		}
		host = strings.ToLower(host)
	}
	return u.Scheme + "://" + net.JoinHostPort(host, strconv.Itoa(number)), nil
}

func connectionTLS(cfg Config) (*tls.Config, error) {
	if !strings.HasPrefix(cfg.URL, "https://") {
		return nil, nil
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, NextProtos: []string{"http/1.1"}}
	if cfg.Connection == nil || cfg.Connection.CAFile == "" {
		return config, nil
	}
	invalid := errors.New("Search CA file unavailable or invalid")
	name := cfg.Connection.CAFile
	info, err := os.Stat(name)
	if err != nil || !info.Mode().IsRegular() || info.Size() > 256<<10 {
		return nil, invalid
	}
	file, err := os.Open(name)
	if err != nil {
		return nil, invalid
	}
	defer file.Close()
	info, err = file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > 256<<10 {
		return nil, invalid
	}
	data, err := io.ReadAll(io.LimitReader(file, (256<<10)+1))
	if err != nil || len(data) > 256<<10 {
		return nil, invalid
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(data) {
		return nil, invalid
	}
	config.RootCAs = roots
	return config, nil
}
