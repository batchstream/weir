package parser

import (
	"fmt"
	"unsafe"

	"github.com/iceisfun/golua/ast"
	"github.com/iceisfun/golua/budget"
)

type parseSettings struct {
	strip   bool
	maxVars int
	memory  *budget.Budget
}

// A guarded grammar level can retain several Go frames: a bare table-call
// argument passes through suffixed expressions, argument parsing and fields.
// The four Go 1.27.1 release targets retain less than 5 KiB between guards.
// Four times that size covers geometric stack growth, including old/new stacks;
// RequireAllocation adds its physical-allocation margin to this 16 KiB payload.
const grammarStackBytes = 16 << 10

// Startup and nonrecursive lexer/diagnostic tails need separate credit before
// the first grammar entry. This 32 KiB payload covers a fixed 8 KiB tail under
// the same four-times bound. It does not account for arbitrary external callers.
const initialStackBytes = 32 << 10

// ParseWithBudget keeps parsing and all retained AST allocations inside one
// evaluation scope. A resource refusal wins over any later syntax diagnostic.
func ParseWithBudget(source, input string, memory *budget.Budget) (block *ast.Block, err error) {
	defer func() {
		if memoryErr := memory.Check(); memoryErr != nil {
			block, err = nil, memoryErr
		}
	}()
	defer budget.Recover(&err)
	memory.Require(0)
	settings := parseSettings{strip: true, memory: memory}
	return parseImpl(source, input, settings)
}

// Constructors in ast allocate exactly one concrete node. NewGlobalStarStmt's
// additional one-element string slice is charged at its call site.
func requireNode[T any](p *parser) {
	var node T
	p.memory.RequireAllocation(int(unsafe.Sizeof(node)))
}

func requireArray[T any](p *parser, count int) {
	var item T
	p.memory.RequireArray(unsafe.Sizeof(item), count)
}

func (p *parser) format(format string, args ...any) string {
	if p.memory != nil {
		p.memory.RequireAllocation(len(format) + 256)
		for _, arg := range args {
			if text, ok := arg.(string); ok {
				for range 8 {
					p.memory.RequireAllocation(len(text))
				}
			} else {
				p.memory.RequireAllocation(64)
			}
		}
	}
	return fmt.Sprintf(format, args...)
}
