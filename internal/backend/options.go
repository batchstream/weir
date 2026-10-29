// Package backend holds settings and helpers shared by the native adapters.
package backend

import "time"

type Options struct {
	ConnectTimeout       time.Duration
	CleanupTimeout       time.Duration
	RetryAttempts        int
	RetryBackoff         time.Duration
	IdleTimeout          time.Duration
	MetadataCacheEntries int
	BatchBytes           int
	ExchangeBytes        int
}

func DefaultOptions() Options {
	opts := Options{ConnectTimeout: 2 * time.Second, CleanupTimeout: 2 * time.Second,
		RetryAttempts: 5, RetryBackoff: 5 * time.Millisecond, IdleTimeout: 30 * time.Second, MetadataCacheEntries: 64,
		BatchBytes: 4 << 20, ExchangeBytes: 32 << 20}
	return opts
}
