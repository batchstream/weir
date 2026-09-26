// Package app assembles the finite static local Store set, unwinding partial startup.
package app

import (
	"context"
	"errors"
	"time"

	"github.com/batchstream/weir/internal/mongostore"
	"github.com/batchstream/weir/internal/searchstore"
	"github.com/batchstream/weir/internal/store"
)

type Config struct {
	Mongo  mongostore.Config
	Search *searchstore.Config
	Limits store.Limits
}

func OpenStores(ctx context.Context, cfg Config) (map[string]*store.Runtime, error) {
	if err := cfg.Limits.Validate(); err != nil {
		return nil, err
	}
	if cfg.Search != nil && cfg.Mongo.Store == cfg.Search.Store {
		return nil, errors.New("duplicate store name")
	}
	stores := make(map[string]*store.Runtime)
	complete := false
	defer func() {
		if !complete {
			cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			for _, runtime := range stores {
				_ = runtime.Close(cleanup)
			}
		}
	}()
	cfg.Mongo.Pool = uint64(cfg.Limits.Concurrency)
	mongo, err := mongostore.Open(ctx, cfg.Mongo)
	if err != nil {
		return nil, errors.New("MongoDB startup qualification failed")
	}
	runtime, err := store.New(mongo, cfg.Limits)
	if err != nil {
		return nil, err
	}
	stores[cfg.Mongo.Store] = runtime
	if cfg.Search != nil {
		searchConfig := *cfg.Search
		searchConfig.Pool = cfg.Limits.Concurrency
		search, err := searchstore.Open(ctx, searchConfig)
		if err != nil {
			return nil, err
		}
		runtime, err := store.New(search, cfg.Limits)
		if err != nil {
			return nil, err
		}
		stores[searchConfig.Store] = runtime
	}
	complete = true
	return stores, nil
}
