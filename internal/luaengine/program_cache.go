package luaengine

import (
	"reflect"
	"sync"

	"github.com/iceisfun/golua/compiler"
	"github.com/iceisfun/golua/parser"
)

// Callers send the same few transforms with every record, so compiling once
// per distinct source removes most of an evaluation's CPU. A prototype usually
// retains about five times its source, but code made of many small functions
// can retain thirty times, so the cache bounds the estimated retained bytes.
const (
	maxCachedPrograms = 256
	maxCachedBytes    = 16 << 20
)

var compiledPrograms = newProgramCache(maxCachedPrograms, maxCachedBytes)

// programCache maps Lua source text to its compiled prototype. golua never
// modifies a prototype after compilation, and each VM keeps its own closures,
// globals and converted constants, so concurrent evaluations share one safely.
type programCache struct {
	mu          sync.RWMutex
	programs    map[string]cachedProgram
	bytes       int
	maxPrograms int
	maxBytes    int
}

type cachedProgram struct {
	prototype *compiler.Proto
	bytes     int
}

func newProgramCache(maxPrograms, maxBytes int) *programCache {
	cache := &programCache{programs: make(map[string]cachedProgram), maxPrograms: maxPrograms, maxBytes: maxBytes}
	return cache
}

// compile returns the prototype for source, compiling it on a cache miss.
// Compilation errors are not cached.
func (c *programCache) compile(source string) (*compiler.Proto, error) {
	c.mu.RLock()
	cached, ok := c.programs[source]
	c.mu.RUnlock()
	if ok {
		return cached.prototype, nil
	}
	block, err := parser.Parse("transform.lua", source)
	if err != nil {
		return nil, err
	}
	prototype, err := compiler.Compile("transform.lua", block)
	if err != nil {
		return nil, err
	}
	c.store(source, prototype)
	return prototype, nil
}

// store keeps prototype, evicting arbitrary entries until it fits. A program
// larger than the whole budget is compiled on every evaluation instead.
func (c *programCache) store(source string, prototype *compiler.Proto) {
	entry := cachedProgram{prototype: prototype, bytes: len(source) + prototypeBytes(prototype)}
	if entry.bytes > c.maxBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.programs[source]; ok {
		return
	}
	for cached, evicted := range c.programs {
		if len(c.programs) < c.maxPrograms && c.bytes+entry.bytes <= c.maxBytes {
			break
		}
		delete(c.programs, cached)
		c.bytes -= evicted.bytes
	}
	c.programs[source] = entry
	c.bytes += entry.bytes
}

var (
	protoSize       = int(reflect.TypeFor[compiler.Proto]().Size())
	instructionSize = int(reflect.TypeFor[compiler.Instruction]().Size())
	lineSize        = int(reflect.TypeFor[int]().Size())
	constantSize    = int(reflect.TypeFor[compiler.Value]().Size())
	pointerSize     = int(reflect.TypeFor[*compiler.Proto]().Size())
	upvalueSize     = int(reflect.TypeFor[compiler.UpvalDesc]().Size())
	localSize       = int(reflect.TypeFor[compiler.LocalVar]().Size())
)

// prototypeBytes estimates the heap a prototype tree retains from its slice
// capacities and strings. It leaves out allocator rounding, which adds up to
// about a third for code with many locals or upvalues per function.
func prototypeBytes(root *compiler.Proto) int {
	total := 0
	pending := []*compiler.Proto{root}
	for len(pending) != 0 {
		p := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		total += protoSize + len(p.Source)
		total += cap(p.Code)*instructionSize + cap(p.Lines)*lineSize
		total += cap(p.Constants)*constantSize + cap(p.Protos)*pointerSize
		total += cap(p.Upvalues)*upvalueSize + cap(p.Locals)*localSize
		for _, constant := range p.Constants {
			total += len(constant.SVal)
		}
		for _, upvalue := range p.Upvalues {
			total += len(upvalue.Name)
		}
		for _, local := range p.Locals {
			total += len(local.Name)
		}
		pending = append(pending, p.Protos...)
	}
	return total
}
