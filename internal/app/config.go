package app

import (
	"errors"
	"net/netip"
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
	MaxOperations Count     `yaml:"max_operations,omitempty"`
	MaxBytes      ByteSize  `yaml:"max_bytes,omitempty"`
	MaxWait       *Duration `yaml:"max_wait,omitempty"`
	MinInFlight   Count     `yaml:"min_in_flight,omitempty"`
	MaxInFlight   Count     `yaml:"max_in_flight,omitempty"`
}

const defaultShutdownTimeout = 5 * time.Second

func (cfg Config) shutdownTimeout() time.Duration {
	if cfg.ShutdownTimeout == 0 {
		return defaultShutdownTimeout
	}
	return time.Duration(cfg.ShutdownTimeout)
}

// Validate checks the configuration without performing IO. Load has already
// replaced credential files with their contents.
func (cfg Config) Validate() error {
	if err := cfg.Listeners.validate(); err != nil {
		return err
	}
	if cfg.ShutdownTimeout < 0 {
		return errors.New("shutdown_timeout must not be negative")
	}
	names := make(map[string]bool, len(cfg.Stores))
	for _, definition := range cfg.Stores {
		if !protocol.ValidStoreName(definition.Name) || names[definition.Name] {
			return errors.New("invalid or duplicate Store name")
		}
		names[definition.Name] = true
		if (definition.MongoDB == nil) == (definition.Search == nil) {
			return errors.New("Store requires exactly one of mongodb or search")
		}
		if err := definition.runtimeLimits().Validate(); err != nil {
			return err
		}
		if err := definition.validateBackend(); err != nil {
			return err
		}
	}
	return nil
}

func (l Listeners) validate() error {
	if _, err := netip.ParseAddrPort(l.Application); err != nil {
		return errors.New("listeners.application requires an explicit IP and port")
	}
	if l.Diagnostics == "" {
		return nil
	}
	if _, err := netip.ParseAddrPort(l.Diagnostics); err != nil {
		return errors.New("listeners.diagnostics requires an explicit IP and port")
	}
	return nil
}

func (definition StoreConfig) validateBackend() error {
	if definition.MongoDB != nil {
		return mongodb.ValidateConfig(definition.mongoConfig())
	}
	return search.ValidateConfig(definition.searchConfig())
}

func (definition StoreConfig) runtimeLimits() store.Limits {
	limits := store.DefaultLimits()
	b := definition.Batching
	if b.MaxOperations != 0 {
		limits.BatchOperations = int(b.MaxOperations)
	}
	if b.MaxBytes != 0 {
		limits.BatchBytes = int(b.MaxBytes)
	}
	if b.MaxWait != nil {
		limits.BatchWait = time.Duration(*b.MaxWait)
	}
	if b.MinInFlight != 0 {
		limits.MinInFlight = int(b.MinInFlight)
	}
	if b.MaxInFlight != 0 {
		limits.MaxInFlight = int(b.MaxInFlight)
	}
	return limits
}

func (definition StoreConfig) backendOptions() backend.Options {
	opts := backend.DefaultOptions()
	opts.BatchBytes = definition.runtimeLimits().BatchBytes
	return opts
}

func (definition StoreConfig) searchConfig() search.Config {
	settings := definition.backendOptions()
	cfg := search.Config{Options: &settings, URL: definition.Search.URL}
	if c := definition.Authentication; c != nil {
		cfg.Username, cfg.Password = c.Username, c.Password
	}
	if tls := definition.TLS; tls != nil {
		cfg.CAFile = tls.CAFile
	}
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
