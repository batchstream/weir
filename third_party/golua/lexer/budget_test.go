package lexer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/iceisfun/golua/budget"
	"github.com/iceisfun/golua/token"
)

func budgetedToken(input string, memory *budget.Budget) (result token.Token, err error) {
	defer budget.Recover(&err)
	l := NewWithBudget("budget.lua", input, true, memory)
	return l.Next()
}

func TestBudgetRejectsLexicalBuffersBeforeGrowth(t *testing.T) {
	const limit = 32 << 10
	refused := errors.New("test allocation capacity")
	cases := []struct {
		name, source string
	}{
		{"identifier", strings.Repeat("a", 128<<10)},
		{"quoted", "'" + strings.Repeat("a", 128<<10) + "'"},
		{"long", "[[" + strings.Repeat("a", 128<<10) + "]]"},
		{"number", strings.Repeat("1", 128<<10)},
		{"invalid_escape", "'" + strings.Repeat("a", 16<<10) + "\\q'"},
	}
	for _, item := range cases {
		t.Run(item.name, func(t *testing.T) {
			accepted, denied := 0, false
			options := budget.Options{Reserve: func(next int) error {
				if next > limit {
					denied = true
					return refused
				}
				accepted = next
				return nil
			}}
			memory := budget.New(options)
			_, err := budgetedToken(item.source, memory)
			if !errors.Is(err, refused) || !denied || accepted > limit {
				t.Fatalf("error=%v denied=%t accepted=%d", err, denied, accepted)
			}
			if memory.Err() == nil || memory.UsedBytes() != accepted {
				t.Fatal("allocation refusal was not retained by the owner")
			}
		})
	}
}

type checkedContext struct {
	checks, stopAt int
	done           chan struct{}
}

func (c *checkedContext) Deadline() (time.Time, bool) {
	var zero time.Time
	return zero, false
}
func (c *checkedContext) Done() <-chan struct{} { return c.done }
func (c *checkedContext) Value(any) any         { return nil }
func (c *checkedContext) Err() error {
	c.checks++
	if c.checks >= c.stopAt {
		select {
		case <-c.done:
		default:
			close(c.done)
		}
		return context.Canceled
	}
	return nil
}

func TestBudgetCancellationChecksLongCommentsAndWhitespace(t *testing.T) {
	for _, prefix := range []string{"", "--", "--[["} {
		ctx := &checkedContext{stopAt: 8, done: make(chan struct{})}
		options := budget.Options{Context: ctx}
		memory := budget.New(options)
		source := prefix + strings.Repeat(" ", 128<<10)
		_, err := budgetedToken(source, memory)
		if !errors.Is(err, context.Canceled) || ctx.checks > ctx.stopAt+1 {
			t.Fatalf("prefix=%q checks=%d error=%v", prefix, ctx.checks, err)
		}
	}
}

func TestBudgetConsecutiveCommentsDoNotRecurse(t *testing.T) {
	options := budget.Options{}
	memory := budget.New(options)
	source := strings.Repeat("--x\n--[[y]]\n", 20000) + "return 1"
	tok, err := budgetedToken(source, memory)
	if err != nil || tok.Type != token.RETURN {
		t.Fatalf("token=%v error=%v", tok.Type, err)
	}
	if memory.UsedBytes() > 4096 {
		t.Fatalf("discarded comments accumulated allocation credit: %d", memory.UsedBytes())
	}
}
