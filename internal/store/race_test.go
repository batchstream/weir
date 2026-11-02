//go:build race

package store

// raceEnabled skips tests whose timing the race detector distorts.
const raceEnabled = true
