package luaengine

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/value"
	"github.com/iceisfun/golua/compiler"
	"github.com/iceisfun/golua/parser"
	"github.com/iceisfun/golua/stdlib"
	"github.com/iceisfun/golua/vm"
)

func TestBridgeNativeNumbersAndFieldWidths(t *testing.T) {
	i32 := value.Value{Kind: value.Int32, Integer: 3}
	i64 := value.Value{Kind: value.Int64, Integer: 3}
	large := value.Value{Kind: value.Int64, Integer: 9007199254740993}
	float := value.Value{Kind: value.Float64, Float: 3}
	negativeZero := value.Value{Kind: value.Float64, Float: math.Copysign(0, -1)}
	input := bridgeObject("small32", i32, "small64", i64, "large", large, "float", float, "negativeZero", negativeZero)
	source := `
		assert(type(input.small32) == "number" and math.type(input.small32) == "integer")
		assert(type(input.large) == "number" and tostring(input.large) == "9007199254740993")
		assert(input.large ~= 9007199254740992.0 and input.large > 9007199254740992.0)
		assert(math.type(input.float) == "float" and input.float == 3)
		input.small32 = input.small32 + 1
		input.small64 = input.small64 + 1
		input.large = input.large + 1
		input.newInteger = 3
		input.newFloat = 3.0
		return input
	`
	result := bridgeRun(t, source, input)
	checks := []struct {
		name string
		kind value.Kind
		n    int64
	}{
		{name: "small32", kind: value.Int32, n: 4},
		{name: "small64", kind: value.Int64, n: 4},
		{name: "large", kind: value.Int64, n: 9007199254740994},
		{name: "newInteger", kind: value.Int64, n: 3},
		{name: "newFloat", kind: value.Float64},
		{name: "float", kind: value.Float64},
		{name: "negativeZero", kind: value.Float64},
	}
	for _, check := range checks {
		field, err := result.Lookup(check.name)
		if err != nil || field.Kind != check.kind || field.Integer != check.n {
			t.Fatalf("%s = %+v, %v", check.name, field, err)
		}
	}
	zero, _ := result.Lookup("negativeZero")
	if !math.Signbit(zero.Float) {
		t.Fatal("negative zero lost its sign")
	}
	unchanged, _ := input.Lookup("small32")
	if unchanged.Integer != 3 {
		t.Fatal("Lua mutation changed its input")
	}
	promoted := bridgeRun(t, "input.small32 = 2147483648; return input", input)
	field, _ := promoted.Lookup("small32")
	if field.Kind != value.Int64 || field.Integer != 2147483648 {
		t.Fatalf("int32 overflow did not promote: %+v", field)
	}
}

func TestBridgeContainersAndOpaqueLeaves(t *testing.T) {
	exact := value.Value{Kind: value.Extended, Type: value.JSONNumberType, Data: []byte("1234567890.12345678901234567890")}
	bson := value.Value{Kind: value.Extended, Type: "mongodb.bson.decimal128", Data: []byte{0, 1, 255}}
	bytes := value.Value{Kind: value.Bytes, Data: []byte{0, 128, 255}}
	null := value.Value{Kind: value.Null}
	object := value.Value{Kind: value.Object}
	array := value.Value{Kind: value.Array}
	input := bridgeObject("exact", exact, "bson", bson, "bytes", bytes, "null", null, "object", object, "array", array)
	source := `
		assert(input.absent == nil and input.null ~= nil and input.null == weir.null())
		assert(type(input.object) == "table" and type(input.array) == "table" and #input.array == 0)
		assert(weir.kind(input.object) == "object" and weir.kind(input.array) == "array")
		assert(weir.kind(input.bytes) == "bytes" and weir.kind(input.exact) == "extended")
		local typ, data = weir.data(input.exact)
		assert(typ == "weir.json.number.v1" and data == "1234567890.12345678901234567890")
		input.bytes = weir.bytes(weir.data(input.bytes))
		input.exact = weir.extended(typ, data)
		input.createdObject = weir.object()
		input.createdArray = weir.array()
		input.object.x = "remove me"
		input.object.x = nil
		input.array[1] = "b"
		input.array[2] = "a"
		table.sort(input.array)
		assert(table.concat(input.array) == "ab")
		local count = 0
		for _ in pairs(input.array) do count = count + 1 end
		assert(count == 2)
		return input
	`
	result := bridgeRun(t, source, input)
	for _, name := range []string{"exact", "bson", "bytes", "null", "object"} {
		before, _ := input.Lookup(name)
		after, _ := result.Lookup(name)
		if !reflect.DeepEqual(before, after) {
			t.Fatalf("%s changed: before=%+v after=%+v", name, before, after)
		}
	}
	for _, name := range []string{"createdObject", "createdArray"} {
		field, _ := result.Lookup(name)
		if name == "createdObject" && field.Kind != value.Object || name == "createdArray" && field.Kind != value.Array {
			t.Fatalf("%s has kind %v", name, field.Kind)
		}
	}
	gotArray, _ := result.Lookup("array")
	if gotArray.Kind != value.Array || gotArray.Items[0].Text != "a" || gotArray.Items[1].Text != "b" {
		t.Fatalf("array operations failed: %+v", gotArray)
	}
	gotBytes, _ := result.Lookup("bytes")
	gotBytes.Data[0] = 17
	if bytes.Data[0] != 0 {
		t.Fatal("output shares opaque bytes with input")
	}
}

func TestBridgeRejectsInvalidTablesAndBudgets(t *testing.T) {
	cases := []struct {
		name   string
		source string
	}{
		{name: "cycle", source: "local t = {}; t.self = t; return t"},
		{name: "sparse", source: "return { [2] = true }"},
		{name: "mixed", source: "return { [1] = true, x = true }"},
		{name: "negative", source: "return { [-1] = true }"},
		{name: "fractional", source: "return { [1.5] = true }"},
		{name: "table key", source: "return { [{}] = true }"},
		{name: "array fields", source: "local t = weir.array(); t.x = true; return t"},
		{name: "object indices", source: "local t = weir.object(); t[1] = true; return t"},
		{name: "nonfinite", source: "return { x = 1/0 }"},
		{name: "function", source: "return { x = function() end }"},
		{name: "NUL key", source: `return { ["\0"] = true }`},
		{name: "invalid UTF8", source: `return { x = "\255" }`},
		{name: "depth", source: "local t = {}; for i=1,33 do t={x=t} end; return t"},
		{name: "nodes", source: "local t = {}; for i=1,4096 do t[i]=true end; return t"},
		{name: "bytes", source: "return {x=string.rep('a',2097153)}"},
		{name: "shared expansion", source: "local x={}; for i=1,2048 do x[i]=true end; return {a=x,b=x}"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			b, state := bridgeState(t)
			result := bridgeExecute(t, state, test.source)
			if _, err := b.fromLua(result); err == nil {
				t.Fatal("invalid Lua document accepted")
			}
		})
	}
	b, _ := bridgeState(t)
	foreign := vm.NewUserdataValue("not a bridge leaf", nil)
	if _, err := b.fromLua(foreign); err == nil {
		t.Fatal("foreign userdata accepted")
	}
	missing := value.Value{Kind: value.Missing}
	array := value.Value{Kind: value.Array, Items: []value.Value{missing}}
	if _, err := b.toLua(array); err == nil {
		t.Fatal("missing array item silently removed")
	}
}

func TestBridgeSharedTablesProduceIndependentOutput(t *testing.T) {
	input := value.Value{Kind: value.Object}
	result := bridgeRun(t, "local x={a=weir.bytes('bytes')}; return {left=x,right=x}", input)
	left, _ := result.Lookup("left")
	right, _ := result.Lookup("right")
	left.Fields[0].Value.Data[0] = 'X'
	if right.Fields[0].Value.Data[0] != 'b' {
		t.Fatal("shared Lua table was not copied per output occurrence")
	}
}

func TestBridgeExactJSONNumbersRoundTrip(t *testing.T) {
	raw := `{"fraction":0.10000000000000000001,"negativeZero":-0,"exponent":1e100,"wide":9223372036854775808,"integer":9007199254740993,"price":4.800,"normal":0.1,"zeros":0.00e999999999,"underflow":1e-10000}`
	input, err := value.DecodeJSON([]byte(raw), value.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	result := bridgeRun(t, "return input", input)
	encoded, err := value.EncodeJSON(result, value.DefaultLimits())
	if err != nil || string(encoded) != raw {
		t.Fatalf("exact JSON round trip: %s, %v", encoded, err)
	}
}

func TestBridgeNaturalJSONFloatsRetainOnlyUnchangedSlotSpelling(t *testing.T) {
	input := bridgeDecodeJSON(t, `{"price":4.800,"rating":0.1,"negativeZero":-0,"precise":0.10000000000000000001,"array":[0.500,1e3]}`)
	source := `
		assert(type(input.price) == "number" and math.type(input.price) == "float")
		assert(type(input.rating) == "number" and input.rating > 0)
		assert(type(input.array[1]) == "number" and input.array[1] == 0.5)
		assert(weir.kind(input.precise) == "extended")
		input.price = input.price + 0.2
		input.moved = input.rating
		input.negativeZero = 0.0
		return input
	`
	result := bridgeRun(t, source, input)
	price, _ := result.Lookup("price")
	if price.Kind != value.Float64 || price.Float != 5 {
		t.Fatalf("natural price arithmetic failed: %+v", price)
	}
	rating, _ := result.Lookup("rating")
	if rating.Kind != value.Extended || string(rating.Data) != "0.1" {
		t.Fatalf("unchanged rating spelling lost: %+v", rating)
	}
	moved, _ := result.Lookup("moved")
	if moved.Kind != value.Float64 || moved.Float != 0.1 {
		t.Fatalf("moved float inherited unrelated spelling: %+v", moved)
	}
	zero, _ := result.Lookup("negativeZero")
	if zero.Kind != value.Float64 || math.Signbit(zero.Float) {
		t.Fatalf("modified signed zero incorrectly restored: %+v", zero)
	}
	array, _ := result.Lookup("array")
	if string(array.Items[0].Data) != "0.500" || string(array.Items[1].Data) != "1e3" {
		t.Fatalf("unchanged array float spelling lost: %+v", array)
	}
}

func TestBridgeSimpleProductBusinessScript(t *testing.T) {
	source, err := os.ReadFile("../../examples/lua/product.lua")
	if err != nil {
		t.Fatal(err)
	}
	observedAt := time.Date(2026, 10, 7, 1, 2, 3, 456, time.UTC)
	current := bridgeDecodeJSON(t, `{"title":"keep","tags":["a","b","a"],"untouched":{"n":3}}`)
	input := bridgeDecodeJSON(t, `{"title":"","tags":["b","c","","c"]}`)
	beforeCurrent, _ := value.EncodeJSON(current, value.DefaultLimits())
	beforeInput, _ := value.EncodeJSON(input, value.DefaultLimits())
	program := Program{Source: string(source), Current: current, Input: input, ObservedAt: observedAt}
	result, err := Evaluate(context.Background(), program)
	if err != nil || result.Action != Replace {
		t.Fatalf("product update: %+v, %v", result, err)
	}
	title, _ := result.Value.Lookup("title")
	tags, _ := result.Value.Lookup("tags")
	updated, _ := result.Value.Lookup("updated_at")
	if title.Text != "keep" || len(tags.Items) != 3 || tags.Items[0].Text != "a" || tags.Items[1].Text != "b" || tags.Items[2].Text != "c" || updated.Text != observedAt.Format(time.RFC3339Nano) {
		t.Fatalf("product business result: %+v", result.Value)
	}
	afterCurrent, _ := value.EncodeJSON(current, value.DefaultLimits())
	afterInput, _ := value.EncodeJSON(input, value.DefaultLimits())
	if string(beforeCurrent) != string(afterCurrent) || string(beforeInput) != string(afterInput) {
		t.Fatal("business script changed Go input values")
	}
	program.Current = value.Value{}
	program.Input = bridgeDecodeJSON(t, `{"title":"create","tags":[]}`)
	created, err := Evaluate(context.Background(), program)
	if err != nil {
		t.Fatal(err)
	}
	title, _ = created.Value.Lookup("title")
	tags, _ = created.Value.Lookup("tags")
	if title.Text != "create" || tags.Kind != value.Array || len(tags.Items) != 0 {
		t.Fatalf("product create: %+v", created.Value)
	}
	program.Source = `return function(current, incoming) return { first=weir.time.now(), second=weir.time.now() } end`
	clock, err := Evaluate(context.Background(), program)
	if err != nil {
		t.Fatal(err)
	}
	first, _ := clock.Value.Lookup("first")
	second, _ := clock.Value.Lookup("second")
	if first.Text != second.Text || first.Text != observedAt.Format(time.RFC3339Nano) {
		t.Fatalf("clock changed within one operation: %+v", clock.Value)
	}
}

func TestBridgeComplexProductBusinessScript(t *testing.T) {
	source, err := os.ReadFile("testdata/product_merge.lua")
	if err != nil {
		t.Fatal(err)
	}
	current := bridgeDecodeJSON(t, `{"uid":"old","uids":["old","shared"],"title":"keep","brand":"OLD","comment_count":3,"rating":4.800,"first_found_at":"first","offers":[{"uid":"shared","price":1}],"comments":[{"a":1,"nested":{"b":true,"c":null}}],"stocks":[{"stock":2,"variables":{"x":1,"y":2}}],"solds":[{"sold":2,"period_hours":3,"record_at":"2026-10-07T01:02:03Z"}],"allowed_countries":["US"]}`)
	input := bridgeDecodeJSON(t, `{"uid":"new","uids":["shared","new",""],"title":"","brand":"new","comment_count":0,"rating":0.5,"first_found_at":"later","last_found_at":"last","offers":[{"uid":"shared","price":2},{"uid":"new","price":3}],"comments":[{"nested":{"c":null,"b":true},"a":1}],"stocks":[{"stock":2,"variables":{"y":2,"x":1}}],"solds":[{"sold":2,"period_hours":3,"record_at":"2026-10-07T09:08:07Z"}],"allowed_countries":["US","CA"],"available":true}`)
	beforeCurrent, _ := value.EncodeJSON(current, value.DefaultLimits())
	beforeInput, _ := value.EncodeJSON(input, value.DefaultLimits())
	program := Program{Source: string(source), Current: current, Input: input}
	result, err := Evaluate(context.Background(), program)
	if err != nil {
		t.Fatal(err)
	}
	for name, expected := range map[string]string{"title": "keep", "brand": "NEW", "uid": "new", "first_found_at": "first", "last_found_at": "last"} {
		field, _ := result.Value.Lookup(name)
		if field.Text != expected {
			t.Fatalf("%s=%+v, want %q", name, field, expected)
		}
	}
	for name, expected := range map[string]int{"uids": 3, "comments": 1, "stocks": 1, "solds": 1, "offers": 2, "allowed_countries": 2} {
		field, _ := result.Value.Lookup(name)
		if field.Kind != value.Array || len(field.Items) != expected {
			t.Fatalf("%s=%+v, want %d array items", name, field, expected)
		}
	}
	count, _ := result.Value.Lookup("comment_count")
	offers, _ := result.Value.Lookup("offers")
	price, _ := offers.Items[0].Lookup("price")
	if count.Integer != 3 || price.Integer != 2 {
		t.Fatalf("nonzero count or incoming offer preference changed: %+v", result.Value)
	}
	afterCurrent, _ := value.EncodeJSON(current, value.DefaultLimits())
	afterInput, _ := value.EncodeJSON(input, value.DefaultLimits())
	if string(beforeCurrent) != string(afterCurrent) || string(beforeInput) != string(afterInput) {
		t.Fatal("complex script changed Go inputs")
	}
	program.Current = value.Value{}
	program.Input = bridgeDecodeJSON(t, `{}`)
	empty, err := Evaluate(context.Background(), program)
	if err != nil {
		t.Fatal(err)
	}
	uids, _ := empty.Value.Lookup("uids")
	if uids.Kind != value.Array || len(uids.Items) != 0 {
		t.Fatalf("empty business array became an object: %+v", uids)
	}
}

func TestBridgeComplexProductHistoryAndOfferLimits(t *testing.T) {
	source, err := os.ReadFile("testdata/product_merge.lua")
	if err != nil {
		t.Fatal(err)
	}
	comments := make([]any, 0, 25)
	solds := make([]any, 0, 25)
	stocks := make([]any, 0, 25)
	offers := make([]any, 0, 51)
	for index := 0; index < 25; index++ {
		comments = append(comments, fmt.Sprintf("comment-%02d", index))
		sold := map[string]any{"sold": index, "period_hours": 1, "record_at": "2026-10-07"}
		stock := map[string]any{"stock": index, "variables": map[string]any{"x": 1}}
		solds = append(solds, sold)
		stocks = append(stocks, stock)
	}
	for index := 0; index < 51; index++ {
		offer := map[string]any{"uid": fmt.Sprintf("offer-%02d", index)}
		offers = append(offers, offer)
	}
	document := map[string]any{"comments": comments, "solds": solds, "stocks": stocks, "offers": offers}
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	input := bridgeDecodeJSON(t, string(raw))
	program := Program{Source: string(source), Input: input}
	result, err := Evaluate(context.Background(), program)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"comments", "solds", "stocks"} {
		field, _ := result.Value.Lookup(name)
		if len(field.Items) != 20 {
			t.Fatalf("%s retained %d items", name, len(field.Items))
		}
	}
	gotComments, _ := result.Value.Lookup("comments")
	gotSolds, _ := result.Value.Lookup("solds")
	gotStocks, _ := result.Value.Lookup("stocks")
	firstSold, _ := gotSolds.Items[0].Lookup("sold")
	firstStock, _ := gotStocks.Items[0].Lookup("stock")
	if gotComments.Items[0].Text != "comment-05" || firstSold.Integer != 5 || firstStock.Integer != 5 {
		t.Fatal("history did not retain its final twenty items")
	}
	gotOffers, _ := result.Value.Lookup("offers")
	firstOffer, _ := gotOffers.Items[0].Lookup("uid")
	lastOffer, _ := gotOffers.Items[len(gotOffers.Items)-1].Lookup("uid")
	if len(gotOffers.Items) != 50 || firstOffer.Text != "offer-01" || lastOffer.Text != "offer-50" {
		t.Fatalf("offers did not retain final fifty: %+v", gotOffers)
	}
}

func TestBridgeChecksCancelledConversion(t *testing.T) {
	b, state := bridgeState(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	state.SetContext(ctx)
	input := value.Value{Kind: value.Object}
	if _, err := b.toLua(input); err == nil {
		t.Fatal("cancelled input conversion accepted")
	}
	if _, err := b.fromLua(vm.NewInt(1)); err == nil {
		t.Fatal("cancelled output conversion accepted")
	}
}

func bridgeState(t *testing.T) (*bridge, *vm.VM) {
	t.Helper()
	state := vm.New()
	t.Cleanup(func() { _ = state.Close(context.Background()) })
	stdlib.Open(state)
	module := vm.NewEmptyTable()
	b := newBridge(value.DefaultLimits())
	b.install(state, module)
	state.SetGlobal("weir", vm.NewTable(module))
	return b, state
}

func bridgeExecute(t *testing.T, state *vm.VM, source string) vm.Value {
	t.Helper()
	block, err := parser.Parse("bridge-test", source)
	if err != nil {
		t.Fatal(err)
	}
	proto, err := compiler.Compile("bridge-test", block)
	if err != nil {
		t.Fatal(err)
	}
	results, err := state.Run(proto)
	if err != nil || len(results) != 1 {
		t.Fatalf("Lua results=%v, err=%v", results, err)
	}
	return results[0]
}

func bridgeRun(t *testing.T, source string, input value.Value) value.Value {
	t.Helper()
	b, state := bridgeState(t)
	converted, err := b.toLua(input)
	if err != nil {
		t.Fatal(err)
	}
	state.SetGlobal("input", converted)
	luaResult := bridgeExecute(t, state, strings.TrimSpace(source))
	result, err := b.fromLua(luaResult)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func bridgeObject(fields ...any) value.Value {
	object := value.Value{Kind: value.Object}
	for index := 0; index < len(fields); index += 2 {
		field := value.Field{Name: fields[index].(string), Value: fields[index+1].(value.Value)}
		object.Fields = append(object.Fields, field)
	}
	return object
}

func bridgeDecodeJSON(t *testing.T, raw string) value.Value {
	t.Helper()
	decoded, err := value.DecodeJSON([]byte(raw), value.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}
