package luaengine

import (
	"sync"

	"github.com/iceisfun/golua/compiler"
	"github.com/iceisfun/golua/parser"
)

// Callers send the same few transforms with every record, so compiling once
// per distinct source removes most of an evaluation's CPU. A prototype usually
// holds about five times its source in memory and up to about twenty times for
// code made of many small functions, so the cache bounds source bytes as well
// as the number of programs.
const (
	maxCachedPrograms    = 256
	maxCachedSourceBytes = 1 << 20
)

var compiledPrograms = newProgramCache(maxCachedPrograms, maxCachedSourceBytes)

// programCache maps Lua source text to its compiled prototype. golua never
// modifies a prototype after compilation, and each VM keeps its own closures,
// globals and converted constants, so concurrent evaluations share one safely.
type programCache struct {
	mu          sync.RWMutex
	programs    map[string]*compiler.Proto
	sourceBytes int
	maxPrograms int
	maxBytes    int
}

func newProgramCache(maxPrograms, maxBytes int) *programCache {
	cache := &programCache{programs: make(map[string]*compiler.Proto), maxPrograms: maxPrograms, maxBytes: maxBytes}
	return cache
}

// compile returns the prototype for source, compiling it on a cache miss.
// Compilation errors are not cached.
func (c *programCache) compile(source string) (*compiler.Proto, error) {
	c.mu.RLock()
	prototype, ok := c.programs[source]
	c.mu.RUnlock()
	if ok {
		return prototype, nil
	}
	block, err := parser.Parse("transform.lua", source)
	if err != nil {
		return nil, err
	}
	prototype, err = compiler.Compile("transform.lua", block)
	if err != nil {
		return nil, err
	}
	c.store(source, prototype)
	return prototype, nil
}

// store keeps prototype, evicting arbitrary entries until it fits. A source
// larger than the whole budget is compiled on every evaluation instead.
func (c *programCache) store(source string, prototype *compiler.Proto) {
	if len(source) > c.maxBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.programs[source]; ok {
		return
	}
	for cached := range c.programs {
		if len(c.programs) < c.maxPrograms && c.sourceBytes+len(source) <= c.maxBytes {
			break
		}
		delete(c.programs, cached)
		c.sourceBytes -= len(cached)
	}
	c.programs[source] = prototype
	c.sourceBytes += len(source)
}
