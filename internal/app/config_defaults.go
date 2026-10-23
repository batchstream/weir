package app

import (
	"github.com/batchstream/weir/internal/backend"
	"github.com/batchstream/weir/internal/backend/mongodb"
	"github.com/batchstream/weir/internal/server"
	"github.com/batchstream/weir/internal/store"
	"github.com/iceisfun/golua/vm"
)

// Resolve applies defaults to a separate configuration copy and validates it.
// Decoding, programmatic startup and inspection share this normalization path.
func (cfg Config) Resolve() (Config, error) {
	cfg.Basic = cfg.Basic.resolved()
	cfg.Routing = cfg.Routing.resolved()
	return cfg, cfg.validate()
}

func (cfg Config) Validate() error {
	_, err := cfg.Resolve()
	return err
}

func (cfg BasicConfig) Validate() error {
	cfg = cfg.resolved()
	return cfg.validate()
}

func (cfg RoutingConfig) Validate() error {
	cfg = cfg.resolved()
	return cfg.validate()
}

func (cfg BasicConfig) resolved() BasicConfig {
	if cfg.Transport.MaxPendingRecords == 0 {
		cfg.Transport.MaxPendingRecords = server.DefaultLimits().MaxPendingRecords
	}
	if cfg.Lua.MaxCallDepth == 0 {
		cfg.Lua.MaxCallDepth = vm.DefaultMaxCallDepth
	}
	if cfg.Lua.MaxStackSlots == 0 {
		cfg.Lua.MaxStackSlots = vm.DefaultMaxStackSlots
	}
	return cfg
}

func (cfg RoutingConfig) resolved() RoutingConfig {
	if cfg.Stores == nil {
		return cfg
	}
	stores := make([]StoreConfig, len(cfg.Stores))
	for i, definition := range cfg.Stores {
		stores[i] = definition
		if definition.Local == nil {
			continue
		}
		local := *definition.Local
		defaults := store.DefaultLimits()
		options := backend.DefaultOptions()
		if local.Queue.MaxOperations == 0 {
			local.Queue.MaxOperations = defaults.QueueOperations
		}
		if local.Batching.MaxOperations == 0 {
			local.Batching.MaxOperations = defaults.BatchOperations
		}
		local.Queue.MaxBytes = configDefault(local.Queue.MaxBytes, ByteSize(defaults.QueueBytes))
		local.Batching.MaxBytes = configDefault(local.Batching.MaxBytes, ByteSize(defaults.BatchBytes))
		local.Batching.MaxInFlight = configDefault(local.Batching.MaxInFlight, defaults.MaxInFlight)
		local.Batching.MaxWait = configDefault(local.Batching.MaxWait, Duration(defaults.BatchWait))
		local.Backend.ConnectTimeout = configDefault(local.Backend.ConnectTimeout, Duration(options.ConnectTimeout))
		local.Backend.MaxExchangeBytes = configDefault(local.Backend.MaxExchangeBytes, ByteSize(options.ExchangeBytes))
		if local.Backend.MongoDB != nil {
			mongo := *local.Backend.MongoDB
			if mongo.MaxConnecting == 0 {
				mongo.MaxConnecting = mongodb.DefaultMaxConnecting
			}
			if mongo.RetryAttempts == 0 {
				mongo.RetryAttempts = options.RetryAttempts
			}
			mongo.MetadataCacheEntries = configDefault(mongo.MetadataCacheEntries, options.MetadataCacheEntries)
			mongo.RetryBackoff = configDefault(mongo.RetryBackoff, Duration(options.RetryBackoff))
			local.Backend.MongoDB = &mongo
		}
		if local.Backend.Search != nil {
			search := *local.Backend.Search
			if search.RetryAttempts == 0 {
				search.RetryAttempts = options.RetryAttempts
			}
			search.IdleTimeout = configDefault(search.IdleTimeout, Duration(options.IdleTimeout))
			local.Backend.Search = &search
		}
		if local.Backend.Authentication != nil {
			authentication := *local.Backend.Authentication
			local.Backend.Authentication = &authentication
		}
		if local.Backend.TLS != nil {
			trust := *local.Backend.TLS
			local.Backend.TLS = &trust
		}
		stores[i].Local = &local
	}
	cfg.Stores = stores
	return cfg
}

func configDefault[T any](value *T, fallback T) *T {
	if value != nil {
		fallback = *value
	}
	return &fallback
}
