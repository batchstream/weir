// Package backend holds runtime settings shared by the native adapters.
package backend

import (
	"fmt"
	"time"

	"github.com/batchstream/weir/internal/luaengine"
)

type Options struct {
	ConnectTimeout       time.Duration
	CleanupTimeout       time.Duration
	RetryAttempts        int
	RetryBackoff         time.Duration
	IdleTimeout          time.Duration
	MetadataCacheEntries int
	BatchBytes           int
	ExchangeBytes        int
	Lua                  luaengine.Limits
}

func DefaultOptions() Options {
	opts := Options{ConnectTimeout: 2 * time.Second, CleanupTimeout: 2 * time.Second,
		RetryAttempts: 5, RetryBackoff: 5 * time.Millisecond, IdleTimeout: 30 * time.Second, MetadataCacheEntries: 64,
		BatchBytes: 4 << 20, ExchangeBytes: 32 << 20, Lua: luaengine.DefaultLimits()}
	return opts
}

func (opts Options) Validate() error {
	if opts.ConnectTimeout <= 0 || opts.MetadataCacheEntries < 0 || opts.BatchBytes < 1 || opts.ExchangeBytes < 1 || opts.ExchangeBytes > int(^uint(0)>>1)/3 {
		return fmt.Errorf("invalid backend connection, cache or batch exchange configuration")
	}
	if opts.CleanupTimeout <= 0 || opts.RetryAttempts < 1 || opts.RetryBackoff < 0 || opts.IdleTimeout < 0 {
		return fmt.Errorf("invalid backend cleanup, retry or idle timeout configuration")
	}
	return opts.Lua.Validate()
}
