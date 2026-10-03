package mongodb

import (
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/batchstream/weir-protocol/api/protocol"
)

const maxMongoURIBytes = 4096

// ValidateConfig is pure and accepts only the audited single-endpoint profiles.
// Credentials are supplied separately; the URI never carries user information.
func ValidateConfig(cfg Config) error {
	if cfg.Pool < 1 {
		return errors.New("invalid MongoDB configuration")
	}
	if cfg.MaxReadSize != 0 && (cfg.MaxReadSize < 1024 || cfg.MaxReadSize > protocol.MaxDocument) {
		return errors.New("MongoDB maximum read size must be between 1 KiB and 2 MiB")
	}
	name, segments, err := protocol.ParseResource("weir://" + cfg.Store)
	if err != nil || name != cfg.Store || len(segments) != 0 {
		return errors.New("invalid MongoDB Store")
	}
	if (cfg.Username == "") != (cfg.Password == "") ||
		!validCredential(cfg.Username, 128) || !validCredential(cfg.Password, 256) {
		return errors.New("invalid MongoDB credential pair")
	}

	raw := cfg.URI
	if len(raw) == 0 || len(raw) > maxMongoURIBytes || strings.TrimSpace(raw) != raw {
		return errors.New("invalid MongoDB URI length or whitespace")
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "mongodb" || parsed.Opaque != "" || parsed.Fragment != "" || parsed.RawFragment != "" {
		return errors.New("MongoDB URI must use the standard mongodb scheme")
	}
	if parsed.User != nil {
		return errors.New("MongoDB URI userinfo is unsupported; configure credentials separately")
	}
	if parsed.Path != "" && parsed.Path != "/" || parsed.RawPath != "" {
		return errors.New("MongoDB URI path is unsupported; the resource supplies the database")
	}
	host, port, err := net.SplitHostPort(parsed.Host)
	if err != nil || !validMongoHost(host) || !validMongoPort(port) {
		return errors.New("MongoDB URI requires one explicit host and port")
	}

	query, err := url.ParseQuery(parsed.RawQuery)
	if err != nil {
		return errors.New("invalid MongoDB URI options")
	}
	options := make(map[string]string, len(query))
	seen := make(map[string]bool, len(query))
	for key, values := range query {
		name := strings.ToLower(key)
		if len(values) != 1 || seen[name] {
			return errors.New("duplicate MongoDB URI option")
		}
		seen[name] = true
		if len(values[0]) > 2048 {
			return errors.New("MongoDB URI option exceeds bound")
		}
		options[name] = values[0]
	}
	for name := range options {
		switch name {
		case "directconnection", "servermonitoringmode", "authsource", "authmechanism", "tls", "tlscafile":
		default:
			return errors.New("unsupported MongoDB URI option")
		}
	}
	if value, ok := options["directconnection"]; ok && value != "true" {
		return errors.New("MongoDB URI cannot override the direct single-endpoint profile")
	}
	if value, ok := options["servermonitoringmode"]; ok && value != "poll" {
		return errors.New("MongoDB URI cannot override bounded server monitoring")
	}

	if cfg.Username == "" {
		_, hasAuthSource := options["authsource"]
		_, hasAuthMechanism := options["authmechanism"]
		_, hasTLS := options["tls"]
		_, hasTLSCAFile := options["tlscafile"]
		if hasAuthSource || hasAuthMechanism || hasTLS || hasTLSCAFile {
			return errors.New("MongoDB authentication and TLS options require SCRAM credentials")
		}
		return nil
	}
	if options["authmechanism"] != "SCRAM-SHA-256" || !namespacePattern.MatchString(options["authsource"]) || options["tls"] != "true" {
		return errors.New("authenticated MongoDB connections require explicit SCRAM-SHA-256, authSource, and TLS")
	}
	if caFile, ok := options["tlscafile"]; ok && (caFile == "" || strings.ContainsAny(caFile, "\x00\r\n")) {
		return errors.New("invalid MongoDB CA file option")
	}
	return nil
}

func validCredential(value string, limit int) bool {
	if len(value) > limit || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func validMongoHost(host string) bool {
	if net.ParseIP(host) != nil {
		return true
	}
	if len(host) == 0 || len(host) > 253 || strings.HasSuffix(host, ".") {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, char := range label {
			if char != '-' && (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char < '0' || char > '9') {
				return false
			}
		}
	}
	return true
}

func validMongoPort(port string) bool {
	if port == "" || strings.Trim(port, "0123456789") != "" {
		return false
	}
	number, err := strconv.Atoi(port)
	return err == nil && number >= 1 && number <= 65535
}
