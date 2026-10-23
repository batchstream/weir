package app

import (
	"errors"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	"github.com/batchstream/weir/internal/backend"
	"github.com/batchstream/weir/internal/backend/mongodb"
	"github.com/batchstream/weir/internal/backend/search"
	"github.com/batchstream/weir/internal/store"
)

// Config is a node's complete configuration, loaded from one YAML file.
type Config struct {
	Listeners Listeners `yaml:"listeners"`
	// ShutdownTimeout bounds the drain of admitted work. Zero selects 5s.
	ShutdownTimeout Duration      `yaml:"shutdown_timeout,omitempty"`
	Stores          []StoreConfig `yaml:"stores,omitempty"`
}

// Listeners are explicit IP:port addresses. Diagnostics is optional.
type Listeners struct {
	Application string `yaml:"application"`
	Diagnostics string `yaml:"diagnostics,omitempty"`
}

// StoreConfig hosts one Store on exactly one backend.
type StoreConfig struct {
	Name           string       `yaml:"name"`
	MongoDB        *MongoDB     `yaml:"mongodb,omitempty"`
	Search         *Search      `yaml:"search,omitempty"`
	Authentication *Credentials `yaml:"authentication,omitempty"`
	TLS            *TLS         `yaml:"tls,omitempty"`
	Batching       Batching     `yaml:"batching,omitempty"`
}

type MongoDB struct {
	URI string `yaml:"uri"`
}

type Search struct {
	URL string `yaml:"url"`
}

type TLS struct {
	CAFile string `yaml:"ca_file"`
}

type Credentials struct {
	Username     string `yaml:"username,omitempty"`
	Password     string `yaml:"password,omitempty"`
	UsernameFile string `yaml:"username_file,omitempty"`
	PasswordFile string `yaml:"password_file,omitempty"`
}

// Batching bounds how queued records combine into backend executions. Zero
// values select the defaults; MaxWait is a pointer so an explicit 0s disables
// collection.
type Batching struct {
	MaxOperations int       `yaml:"max_operations,omitempty"`
	MaxBytes      ByteSize  `yaml:"max_bytes,omitempty"`
	MaxWait       *Duration `yaml:"max_wait,omitempty"`
	MaxInFlight   int       `yaml:"max_in_flight,omitempty"`
}

const defaultShutdownTimeout = 5 * time.Second

func (cfg Config) shutdownTimeout() time.Duration {
	if cfg.ShutdownTimeout == 0 {
		return defaultShutdownTimeout
	}
	return time.Duration(cfg.ShutdownTimeout)
}

// Validate checks the configuration without performing IO. Credential files
// must already be resolved, as Load does.
func (cfg Config) Validate() error {
	if err := cfg.Listeners.validate(); err != nil {
		return err
	}
	if cfg.ShutdownTimeout < 0 {
		return errors.New("shutdown_timeout must not be negative")
	}
	if err := cfg.validateSources(); err != nil {
		return err
	}
	for _, definition := range cfg.Stores {
		if err := definition.validate(); err != nil {
			return err
		}
	}
	return nil
}

func (l Listeners) validate() error {
	if !address(l.Application) {
		return errors.New("listeners.application requires an explicit IP and port")
	}
	if l.Diagnostics == "" {
		return nil
	}
	if !address(l.Diagnostics) {
		return errors.New("listeners.diagnostics requires an explicit IP and port")
	}
	host, port, _ := net.SplitHostPort(l.Application)
	diagnosticHost, diagnosticPort, _ := net.SplitHostPort(l.Diagnostics)
	number, _ := strconv.Atoi(port)
	diagnosticNumber, _ := strconv.Atoi(diagnosticPort)
	ip, diagnosticIP := net.ParseIP(host), net.ParseIP(diagnosticHost)
	// Each port 0 bind requests its own ephemeral port.
	if number != 0 && number == diagnosticNumber && (ip.Equal(diagnosticIP) || ip.IsUnspecified() || diagnosticIP.IsUnspecified()) {
		return errors.New("listeners.application and listeners.diagnostics overlap")
	}
	return nil
}

func address(value string) bool {
	host, port, err := net.SplitHostPort(value)
	number, portErr := strconv.Atoi(port)
	return err == nil && net.ParseIP(host) != nil &&
		port != "" && strings.Trim(port, "0123456789") == "" &&
		portErr == nil && number >= 0 && number <= 65535
}

// validateSources checks Store identities, backend URIs and credential
// sources: everything Load must trust before it reads any credential file.
func (cfg Config) validateSources() error {
	names := make(map[string]bool, len(cfg.Stores))
	for _, definition := range cfg.Stores {
		if !protocol.ValidStoreName(definition.Name) || names[definition.Name] {
			return errors.New("invalid or duplicate Store name")
		}
		names[definition.Name] = true
		if (definition.MongoDB == nil) == (definition.Search == nil) {
			return errors.New("Store requires exactly one of mongodb or search")
		}
		if definition.MongoDB != nil {
			if err := mongodb.ValidateURI(definition.MongoDB.URI); err != nil {
				return err
			}
		}
		if credentials := definition.Authentication; credentials != nil {
			if err := credentials.validateSources(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (definition StoreConfig) validate() error {
	if err := definition.runtimeLimits().Validate(); err != nil {
		return err
	}
	if c := definition.Authentication; c != nil && (c.UsernameFile != "" || c.PasswordFile != "") {
		return errors.New("unresolved credential file")
	}
	if definition.MongoDB != nil {
		return mongodb.ValidateConfig(definition.mongoConfig())
	}
	return search.ValidateConfig(definition.searchConfig())
}

func (definition StoreConfig) runtimeLimits() store.Limits {
	limits := store.DefaultLimits()
	b := definition.Batching
	if b.MaxOperations != 0 {
		limits.BatchOperations = b.MaxOperations
	}
	if b.MaxBytes != 0 {
		limits.BatchBytes = boundedSize(b.MaxBytes)
	}
	if b.MaxWait != nil {
		limits.BatchWait = time.Duration(*b.MaxWait)
	}
	if b.MaxInFlight != 0 {
		limits.MaxInFlight = b.MaxInFlight
	}
	return limits
}

func (definition StoreConfig) backendOptions() backend.Options {
	opts := backend.DefaultOptions()
	opts.BatchBytes = definition.runtimeLimits().BatchBytes
	return opts
}

// boundedSize maps sizes beyond the platform integer to an invalid bound.
func boundedSize(size ByteSize) int {
	if uint64(size) > uint64(^uint(0)>>1) {
		return -1
	}
	return int(size)
}

func (definition StoreConfig) searchConfig() search.Config {
	var connection *search.Connection
	if definition.Authentication != nil || definition.TLS != nil {
		connection = &search.Connection{}
		if c := definition.Authentication; c != nil {
			connection.Username, connection.Password = c.Username, c.Password
		}
		if tls := definition.TLS; tls != nil {
			connection.CAFile = tls.CAFile
		}
	}
	settings := definition.backendOptions()
	cfg := search.Config{Options: &settings, Store: definition.Name, URL: definition.Search.URL, Connection: connection}
	return cfg
}

func (definition StoreConfig) mongoConfig() mongodb.Config {
	settings := definition.backendOptions()
	cfg := mongodb.Config{Options: &settings, URI: definition.MongoDB.URI, Store: definition.Name}
	if c := definition.Authentication; c != nil {
		cfg.Username, cfg.Password = c.Username, c.Password
	}
	if tls := definition.TLS; tls != nil {
		cfg.CAFile = tls.CAFile
	}
	return cfg
}
