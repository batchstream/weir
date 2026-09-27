package mongostore

import (
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"
)

const maxMongoURIBytes = 4096

// ValidateURI accepts the credential-free profile and refuses unaudited TLS/SCRAM connections.
func ValidateURI(raw string) error {
	authenticated, err := validateURI(raw)
	if err != nil {
		return err
	}
	if authenticated {
		return errors.New("MongoDB SCRAM/TLS profile is not qualified with the bounded wire reader")
	}
	return nil
}

func validateURI(raw string) (bool, error) {
	if len(raw) == 0 || len(raw) > maxMongoURIBytes || strings.TrimSpace(raw) != raw {
		return false, errors.New("invalid MongoDB URI length or whitespace")
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "mongodb" || parsed.Opaque != "" || parsed.Fragment != "" || parsed.RawFragment != "" {
		return false, errors.New("MongoDB URI must use the standard mongodb scheme")
	}
	if parsed.Path != "" && parsed.Path != "/" || parsed.RawPath != "" {
		return false, errors.New("MongoDB URI path is unsupported; configure the database separately")
	}
	host, port, err := net.SplitHostPort(parsed.Host)
	if err != nil || !validMongoHost(host) || !validMongoPort(port) {
		return false, errors.New("MongoDB URI requires one explicit host and port")
	}

	query, err := url.ParseQuery(parsed.RawQuery)
	if err != nil {
		return false, errors.New("invalid MongoDB URI options")
	}
	options := make(map[string]string, len(query))
	seen := make(map[string]bool, len(query))
	for key, values := range query {
		name := strings.ToLower(key)
		if len(values) != 1 || seen[name] {
			return false, errors.New("duplicate MongoDB URI option")
		}
		seen[name] = true
		if len(values[0]) > 2048 {
			return false, errors.New("MongoDB URI option exceeds bound")
		}
		options[name] = values[0]
	}
	for name := range options {
		switch name {
		case "directconnection", "servermonitoringmode", "authsource", "authmechanism", "tls", "tlscafile":
		default:
			return false, errors.New("unsupported MongoDB URI option")
		}
	}
	if value, ok := options["directconnection"]; ok && value != "true" {
		return false, errors.New("MongoDB URI cannot override the direct single-endpoint profile")
	}
	if value, ok := options["servermonitoringmode"]; ok && value != "poll" {
		return false, errors.New("MongoDB URI cannot override bounded server monitoring")
	}

	username := ""
	password := ""
	hasPassword := false
	if parsed.User != nil {
		username = parsed.User.Username()
		password, hasPassword = parsed.User.Password()
		if username == "" || len(username) > 128 || !hasPassword || password == "" || len(password) > 256 {
			return false, errors.New("MongoDB credentials require a bounded username and non-empty password")
		}
	}
	if username == "" {
		_, hasAuthSource := options["authsource"]
		_, hasAuthMechanism := options["authmechanism"]
		_, hasTLS := options["tls"]
		_, hasTLSCAFile := options["tlscafile"]
		if hasPassword || hasAuthSource || hasAuthMechanism || hasTLS || hasTLSCAFile {
			return false, errors.New("MongoDB authentication and TLS options require SCRAM credentials")
		}
		return false, nil
	}
	if options["authmechanism"] != "SCRAM-SHA-256" || !namespacePattern.MatchString(options["authsource"]) || options["tls"] != "true" {
		return false, errors.New("authenticated MongoDB connections require explicit SCRAM-SHA-256, authSource, and TLS")
	}
	if caFile, ok := options["tlscafile"]; ok && (caFile == "" || strings.ContainsAny(caFile, "\x00\r\n")) {
		return false, errors.New("invalid MongoDB CA file option")
	}
	return true, nil
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
