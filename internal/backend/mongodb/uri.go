package mongodb

import (
	"errors"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/batchstream/weir-protocol/api/protocol"
)

const maxMongoURIBytes = 4096
const maxMongoSeeds = 7

type connectionProfile struct {
	hosts          []string
	replicaSet     string
	direct         bool
	tls            bool
	authSource     string
	authMechanism  string
	authentication bool
}

// ValidateConfig is pure: credentials and trust files remain separate from the URI.
func ValidateConfig(cfg Config) error {
	_, err := parseConfig(cfg)
	return err
}

func parseConfig(cfg Config) (connectionProfile, error) {
	var profile connectionProfile
	if !protocol.ValidStoreName(cfg.Store) {
		return profile, errors.New("invalid MongoDB Store")
	}
	if (cfg.Username == "") != (cfg.Password == "") ||
		!validCredential(cfg.Username) || !validCredential(cfg.Password) {
		return profile, errors.New("invalid MongoDB credential pair")
	}
	profile, err := parseURI(cfg.URI)
	if err != nil {
		return profile, err
	}
	if cfg.Username == "" {
		if profile.authentication || cfg.CAFile != "" {
			return profile, errors.New("MongoDB authentication and TLS options require SCRAM credentials")
		}
	} else if !profile.authentication {
		return profile, errors.New("authenticated MongoDB connections require explicit SCRAM-SHA-256, authSource, and TLS")
	}
	if !validCredential(cfg.CAFile) || cfg.CAFile != "" && !profile.tls {
		return profile, errors.New("MongoDB CA file requires TLS and a valid path")
	}
	return profile, nil
}

func parseURI(raw string) (connectionProfile, error) {
	var profile connectionProfile
	if len(raw) == 0 || len(raw) > maxMongoURIBytes || strings.TrimSpace(raw) != raw || !strings.HasPrefix(raw, "mongodb://") {
		return profile, errors.New("invalid MongoDB URI or scheme")
	}
	// net/url treats an authority as one host, rejecting valid mixed IPv6 seeds.
	// Separate MongoDB's authority before parsing only its query component.
	authority, query, hasQuery := strings.Cut(strings.TrimPrefix(raw, "mongodb://"), "?")
	if strings.Contains(raw, "#") {
		return profile, errors.New("MongoDB URI fragments are unsupported")
	}
	authority, path, hasPath := strings.Cut(authority, "/")
	if hasPath && path != "" || hasQuery && !hasPath {
		return profile, errors.New("MongoDB URI path is unsupported; the resource supplies the database")
	}
	if strings.Contains(authority, "@") {
		return profile, errors.New("MongoDB URI must not contain credentials")
	}
	if strings.ContainsAny(authority, "%\\") {
		return profile, errors.New("MongoDB URI requires literal hosts without escapes")
	}
	seeds := strings.Split(authority, ",")
	if len(seeds) > maxMongoSeeds {
		return profile, errors.New("MongoDB URI has too many seeds")
	}
	seen := make(map[string]bool, len(seeds))
	for _, seed := range seeds {
		address, err := canonicalMongoSeed(seed)
		if err != nil {
			return profile, err
		}
		if seen[address] {
			return profile, errors.New("duplicate MongoDB seed")
		}
		seen[address] = true
		profile.hosts = append(profile.hosts, address)
	}
	queryValues, err := url.ParseQuery(query)
	if err != nil {
		return profile, errors.New("invalid MongoDB URI options")
	}
	options := make(map[string]string, len(queryValues))
	for key, values := range queryValues {
		name := strings.ToLower(key)
		_, exists := options[name]
		if len(values) != 1 || exists {
			return profile, errors.New("duplicate MongoDB URI option")
		}
		options[name] = values[0]
	}
	for name := range options {
		switch name {
		case "directconnection", "replicaset", "servermonitoringmode", "authsource", "authmechanism", "tls":
		default:
			return profile, errors.New("unsupported MongoDB URI option")
		}
	}
	setName, hasSet := options["replicaset"]
	if hasSet {
		if !validReplicaSetName(setName) {
			return profile, errors.New("invalid MongoDB replica-set name")
		}
		if direct, ok := options["directconnection"]; ok && direct != "false" {
			return profile, errors.New("MongoDB replica-set discovery cannot use direct mode")
		}
		profile.replicaSet = setName
	} else {
		if len(profile.hosts) != 1 {
			return profile, errors.New("MongoDB seed lists require an explicit replica-set name")
		}
		if direct, ok := options["directconnection"]; ok && direct != "true" {
			return profile, errors.New("MongoDB discovery requires an explicit replica-set name")
		}
		profile.direct = true
	}
	if mode, ok := options["servermonitoringmode"]; ok && mode != "poll" {
		return profile, errors.New("MongoDB URI cannot override bounded server monitoring")
	}
	_, hasAuthSource := options["authsource"]
	_, hasAuthMechanism := options["authmechanism"]
	_, hasTLS := options["tls"]
	if hasAuthSource || hasAuthMechanism || hasTLS {
		// Presence matters: an empty option is not equivalent to omission. There
		// is no valid authenticated profile without the complete SCRAM/TLS set.
		if options["authmechanism"] != "SCRAM-SHA-256" || !validDatabaseName(options["authsource"]) || options["tls"] != "true" {
			return profile, errors.New("MongoDB URI authentication requires explicit SCRAM-SHA-256, authSource, and TLS")
		}
		profile.authentication = true
		profile.tls = true
		profile.authSource = options["authsource"]
		profile.authMechanism = options["authmechanism"]
	}
	return profile, nil
}

func canonicalMongoSeed(seed string) (string, error) {
	host, port, err := net.SplitHostPort(seed)
	if err != nil || !validMongoHost(host) || !validMongoPort(port) {
		return "", errors.New("MongoDB seeds require an explicit valid host and port")
	}
	ip, ipErr := netip.ParseAddr(host)
	if strings.ContainsAny(seed, "[]") && (ipErr != nil || !ip.Is6()) {
		return "", errors.New("invalid MongoDB seed brackets")
	}
	if ipErr == nil {
		host = ip.Unmap().String()
	} else {
		host = strings.ToLower(host)
	}
	number, _ := strconv.Atoi(port)
	return net.JoinHostPort(host, strconv.Itoa(number)), nil
}

func validReplicaSetName(name string) bool {
	if len(name) == 0 || len(name) > 64 {
		return false
	}
	for _, char := range name {
		if char != '_' && char != '-' && (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char < '0' || char > '9') {
			return false
		}
	}
	return true
}

func validCredential(value string) bool {
	if !utf8.ValidString(value) {
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
