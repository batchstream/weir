package lexer

import (
	"fmt"
	"unsafe"

	"github.com/iceisfun/golua/budget"
)

// NewWithBudget uses the evaluation's allocation and cancellation budget.
// New remains available to callers that do not supply an allocation owner.
func NewWithBudget(source, input string, stripShebang bool, memory *budget.Budget) *Lexer {
	var allocation1 Lexer
	memory.RequireAllocation(int(unsafe.Sizeof(allocation1)))
	l := &Lexer{source: source, input: input, line: 1, col: 1, memory: memory}
	l.readChar()
	if stripShebang && l.current == '#' {
		for l.current != eof && !isNewline(l.current) {
			l.readChar()
		}
		if isNewline(l.current) {
			_ = l.incLine()
		}
	}
	return l
}

func (l *Lexer) format(format string, args ...any) string {
	if l.memory != nil {
		l.memory.RequireAllocation(len(format) + 256)
		for _, arg := range args {
			if text, ok := arg.(string); ok {
				// Formatting a quoted byte string can expand each byte to \\xNN.
				for range 8 {
					l.memory.RequireAllocation(len(text))
				}
			} else {
				l.memory.RequireAllocation(64)
			}
		}
	}
	return fmt.Sprintf(format, args...)
}

func (l *Lexer) quotedRange(start, end int) string {
	l.memory.RequireAllocation(end - start + 2)
	return "'" + l.input[start:end] + "'"
}

func (l *Lexer) runeText(value rune) string {
	l.memory.RequireAllocation(4)
	return string(value)
}
