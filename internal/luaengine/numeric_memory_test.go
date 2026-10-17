package luaengine

import (
	"errors"
	"strings"
	"testing"

	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/value"
)

func TestEvaluateReusedNumericStringsStayWithinAllocationBudget(t *testing.T) {
	// Each successful conversion tries integer parsing before float parsing.
	// strconv copies this already-owned input when the integer parse fails.
	number := value.Value{Kind: value.String, Text: strings.Repeat("0", 65536) + "1.0"}
	field := value.Field{Name: "number", Value: number}
	input := value.Value{Kind: value.Object, Fields: []value.Field{field}}
	bodies := []string{
		`for attempt=1,128 do for index=1,input.number do break end end`,
		`for attempt=1,128 do for index=input.number,1 do break end end`,
		`for attempt=1,128 do for index=1,1,input.number do break end end`,
		`for attempt=1,128 do table.concat({},'',input.number,0) end`,
		`for attempt=1,128 do assert(table.concat({'a'},'',1,input.number)=='a') end`,
		`for attempt=1,128 do assert(-input.number==-1) end`,
		`for attempt=1,128 do assert(select(input.number,7)==7) end`,
		`local iterator=ipairs({}); for attempt=1,128 do local index,item=iterator({[2]=7},input.number); assert(index==2 and item==7) end`,
	}
	for _, body := range bodies {
		t.Run(body[:min(65, len(body))], func(t *testing.T) {
			for _, limit := range []int{1 << 20, 64 << 20} {
				last := 0
				reserve := func(bytes int) error {
					if bytes < last {
						t.Fatal("numeric conversion released live credit", last, bytes)
					}
					if bytes > limit {
						return execution.ErrExecutionMemory
					}
					last = bytes
					return nil
				}
				source := "return function(_,input) local ok=pcall(function() " + body + " end); assert(ok); return weir.keep() end"
				program := Program{Source: source, Input: input, Reserve: reserve}
				result, err := Evaluate(t.Context(), program)
				if limit == 1<<20 {
					if !errors.Is(err, execution.ErrExecutionMemory) || result.Action != "" || result.Allocations != nil {
						t.Fatal("pcall hid unowned numeric work", result.Action, err, last)
					}
				} else if err != nil || result.Action != Keep || last > limit {
					t.Fatal("normal numeric conversion changed", result.Action, err, last)
				}
			}
		})
	}
}

func TestEvaluateNumericStringSemanticsRemainLua54(t *testing.T) {
	source := `return function()
		assert(tonumber('0x10000000000000000')==0)
		assert(tonumber('ffffffffffffffff',16)==-1)
		assert(math.tointeger('1.0')==1 and math.tointeger('1.5')==nil)
		assert(math.floor('-0')==0 and math.floor('0x1.8')==1)
		assert(select('1.0',7)==7 and string.format('%d','0x10')=='16')
		assert(-'0x1.8'==-1.5 and '2.5'+'0x1.8'==4)
		local count=0; for index='1', '3', '1' do count=count+index end
		assert(count==6 and table.concat({'a','b'},'', '1.0','2.0')=='ab')
		for _,text in ipairs({'inf','NaN','1_0','not-a-number'}) do
			assert(tonumber(text)==nil and not pcall(function() return -text end))
			assert(not pcall(function() for index=1,text do end end))
		end
		assert(not pcall(function() return '1' << 1 end))
		return weir.keep()
	end`
	program := Program{Source: source}
	result, err := Evaluate(t.Context(), program)
	if err != nil || result.Action != Keep {
		t.Fatal("numeric coercion semantics changed", result.Action, err)
	}
}
