// Package backend holds runtime settings shared by the native adapters.
package backend

import (
	"fmt"
	"time"

	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/luaengine"
)

type Options struct {
	ConnectTimeout       time.Duration
	MetadataCacheEntries int
	ExchangeBytes        int
	Scan                 execution.ScanLimits
	Lua                  luaengine.Limits
}

func DefaultOptions() Options {
	opts := Options{ConnectTimeout: 2 * time.Second, MetadataCacheEntries: 64,
		ExchangeBytes: 32 << 20, Scan: execution.DefaultScanLimits(), Lua: luaengine.DefaultLimits()}
	return opts
}

func (opts Options) Validate() error {
	if opts.ConnectTimeout <= 0 || opts.MetadataCacheEntries < 0 || opts.ExchangeBytes < 4<<20 || opts.ExchangeBytes > int(^uint(0)>>1)/3 {
		return fmt.Errorf("invalid backend connection, cache or batch exchange configuration")
	}
	if err := opts.Scan.Validate(); err != nil {
		return err
	}
	return opts.Lua.Validate()
}
