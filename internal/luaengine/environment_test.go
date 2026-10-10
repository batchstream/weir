package luaengine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/value"
	"github.com/iceisfun/golua/vm"
)

func TestEnvironmentIsolatesGlobalsLibrariesAndStringMetatable(t *testing.T) {
	source := `return function()
		assert(leaked == nil and string.custom == nil and math.custom == nil and table.custom == nil)
		assert(("aBc"):upper() == "ABC" and ("aBc"):lower() == "abc")
		assert(("abc"):sub(2) == "bc" and ("abc"):reverse() == "cba")
		assert(("abc"):len() == 3 and ("abc"):byte(2) == 98)
		assert(string.char(65, 66) == "AB" and ("%04d"):format(2) == "0002")
		assert(("2" + "3") == 5 and ("7" // "2") == 3 and ("7" % "2") == 1)
		assert(math.floor(2.9) == 2 and table.concat({"a","b"}) == "ab")
		assert(("abc").rep == nil and ("abc").dump == nil and ("abc").gsub == nil)
		leaked = true
		string.custom, math.custom, table.custom = true, true, true
		string.upper, math.floor, table.concat = nil, nil, nil
		assert(("abc").upper == nil)
		_ENV.assert, _ENV.pairs = nil, nil
		return weir.keep()
	end`
	program := Program{Source: source}
	var calls sync.WaitGroup
	for range 32 {
		calls.Go(func() {
			if _, err := Evaluate(t.Context(), program); err != nil {
				t.Error(err)
			}
		})
	}
	calls.Wait()
}

func TestEnvironmentLimitsAndObservationTimeArePerCall(t *testing.T) {
	for _, operation := range []string{
		`string.format("%080d", 1)`,
		`table.sort({5,4,3,2,1})`,
	} {
		t.Run(operation, func(t *testing.T) {
			source := `return function(_,incoming)
				assert(weir.time.now() == incoming.stamp)
				` + operation + `
				return weir.keep()
			end`
			var calls sync.WaitGroup
			for index := range 24 {
				calls.Go(func() {
					limits := DefaultLimits()
					restricted := index%2 == 0
					if restricted {
						limits.Values.MaxBytes = 64
						limits.Values.MaxNodes = 4
					}
					observedAt := time.Unix(int64(index), int64(index)).UTC()
					stamp := value.Value{Kind: value.String, Text: observedAt.Format(time.RFC3339Nano)}
					field := value.Field{Name: "stamp", Value: stamp}
					input := value.Value{Kind: value.Object, Fields: []value.Field{field}}
					program := Program{Source: source, Input: input, ObservedAt: observedAt, Limits: &limits}
					_, err := Evaluate(t.Context(), program)
					if restricted && err == nil || !restricted && err != nil {
						t.Errorf("restricted=%t: %v", restricted, err)
					}
				})
			}
			calls.Wait()
		})
	}
}

func TestEnvironmentStringMetatablesAreIndependent(t *testing.T) {
	first, second := vm.New(), vm.New()
	defer first.Close(context.Background())
	defer second.Close(context.Background())
	limits := value.DefaultLimits()
	installEnvironment(first, time.Unix(1, 0), limits)
	installEnvironment(second, time.Unix(2, 0), limits)
	firstMeta := first.StringMeta().(*vm.Table)
	secondMeta := second.StringMeta().(*vm.Table)
	if firstMeta == secondMeta || firstMeta.GetString(vm.MetaIndex).AsTable() == secondMeta.GetString(vm.MetaIndex).AsTable() {
		t.Fatal("string metatable or string library is shared")
	}
	firstMeta.SetString(vm.MetaAdd, vm.Nil)
	if secondMeta.GetString(vm.MetaAdd).IsNil() {
		t.Fatal("string arithmetic metatable mutation leaked")
	}
	firstMeta.GetString(vm.MetaIndex).AsTable().(*vm.Table).SetString("upper", vm.Nil)
	if secondMeta.GetString(vm.MetaIndex).AsTable().(*vm.Table).GetString("upper").IsNil() {
		t.Fatal("string method mutation leaked")
	}
}
