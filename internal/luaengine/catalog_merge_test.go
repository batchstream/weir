package luaengine

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/batchstream/weir/internal/value"
)

// These fixtures express a product/offer catalog merge workload with Weir
// primitives and local policy.
//
//go:embed testdata/catalog_common.lua
var catalogCommonSource string

//go:embed testdata/catalog_product.lua
var catalogProductSource string

//go:embed testdata/catalog_offer.lua
var catalogOfferSource string

const catalogObservedAt = "2026-10-14T00:00:00Z"

type catalogMergeOptions struct {
	product           bool
	current, incoming string
}

func runCatalogMerge(t *testing.T, opts catalogMergeOptions) value.Value {
	t.Helper()
	source := catalogCommonSource + catalogOfferSource
	if opts.product {
		source = catalogCommonSource + catalogProductSource
	}
	current := value.Value{Kind: value.Missing}
	if opts.current != "" {
		current = bridgeDecodeJSON(t, opts.current)
	}
	incoming := bridgeDecodeJSON(t, opts.incoming)
	beforeCurrent, _ := value.EncodeJSON(t.Context(), current)
	beforeIncoming, _ := value.EncodeJSON(t.Context(), incoming)
	program := Program{Source: source, Current: current, Input: incoming}
	result, err := Evaluate(t.Context(), program)
	if err != nil || result.Action != Replace {
		t.Fatal("catalog merge failed", result.Action, err)
	}
	afterCurrent, _ := value.EncodeJSON(t.Context(), current)
	afterIncoming, _ := value.EncodeJSON(t.Context(), incoming)
	if string(beforeCurrent) != string(afterCurrent) || string(beforeIncoming) != string(afterIncoming) {
		t.Fatal("catalog merge changed the caller's Go input")
	}
	return result.Value
}

func catalogField(t *testing.T, document value.Value, name string) value.Value {
	t.Helper()
	field, err := document.Lookup(name)
	if err != nil {
		t.Fatal(name, err)
	}
	return field
}

func catalogEnvelope(t *testing.T, document any) string {
	t.Helper()
	envelope := map[string]any{"document": document, "observed_at": catalogObservedAt}
	raw, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestCatalogProductTwoPassMergeUnicodeAndIncomingPriority(t *testing.T) {
	opts := catalogMergeOptions{product: true,
		current:  `{"uid":"old","uids":["old","shared",""],"title":"keep","brand":"café","first_found_at":"first","evicted_at":"old","comment_count":12,"available":true,"sold_by_platform":true,"allowed_countries":["US"],"unknown":"discard","offers":[{"uid":"seller","id":"s","name":"old"}]}`,
		incoming: `{"observed_at":"2026-10-14T00:00:00Z","document":{"uid":"new","uids":["shared","new"],"title":"","brand":"straße café","comment_count":0,"available":false,"sold_by_platform":false,"allowed_countries":["US","CA"],"offers":[{"uid":"seller","id":"s","name":"new"}]}}`,
	}
	result := runCatalogMerge(t, opts)
	for name, want := range map[string]string{"title": "keep", "brand": "STRAßE CAFÉ", "first_found_at": "first", "last_found_at": catalogObservedAt, "uid": "new"} {
		if got := catalogField(t, result, name); got.Text != want {
			t.Fatal("two-pass merge changed a business field", name, got, want)
		}
	}
	if catalogField(t, result, "comment_count").Integer != 12 {
		t.Fatal("zero input replaced the nonzero current count")
	}
	for _, name := range []string{"available", "sold_by_platform", "evicted_at", "unknown"} {
		if catalogField(t, result, name).Kind != value.Missing {
			t.Fatal("boolean normalization, eviction clearing or selected-field rebuild changed", name)
		}
	}
	uids := catalogField(t, result, "uids")
	if uids.Kind != value.Array || len(uids.Items) != 3 || uids.Items[0].Text != "old" || uids.Items[1].Text != "shared" || uids.Items[2].Text != "new" {
		t.Fatal("UID union order/type changed", uids)
	}
	offers := catalogField(t, result, "offers")
	if len(offers.Items) != 1 || catalogField(t, offers.Items[0], "name").Text != "new" {
		t.Fatal("incoming seller did not win the duplicate", offers)
	}
	opts.incoming = `{"observed_at":"2026-10-14T00:00:00Z","document":{"uid":"old"}}`
	result = runCatalogMerge(t, opts)
	if catalogField(t, result, "brand").Text != "CAFÉ" {
		t.Fatal("current brand normalization was skipped when incoming omitted brand")
	}
	opts.current = ""
	result = runCatalogMerge(t, opts)
	if catalogField(t, result, "first_found_at").Text != catalogObservedAt {
		t.Fatal("missing product did not initialize observation time")
	}
}

func TestCatalogProductStructuralKeysAndCommentTimeNormalization(t *testing.T) {
	opts := catalogMergeOptions{product: true,
		current:  `{"uid":"p","stocks":[{"stock":3,"variables":{"color":"red","nested":{"a":1,"b":2}}}],"comments":[{"description":"same","score":4,"date":"2026-09-01T00:00:00.1000+00:00"}],"solds":[{"sold":9007199254740993,"period_hours":24,"record_at":"2026-09-01T00:00:00Z"}]}`,
		incoming: `{"observed_at":"2026-10-14T00:00:00Z","document":{"uid":"p","stocks":[{"stock":3,"variables":{"nested":{"b":2,"a":1},"color":"red"}},{"stock":3,"variables":{"nested":[],"color":"red"}},{"stock":3,"variables":{"nested":{},"color":"red"}},{"stock":3,"variables":{"kind":1}},{"stock":3,"variables":{"kind":"1"}},{"stock":3,"variables":{"kind":true}},{"stock":3,"variables":{"kind":null}}],"comments":[{"description":"same","score":4,"date":"2026-09-01T00:00:00.1Z"}],"solds":[{"sold":9007199254740993,"period_hours":24,"record_at":"2026-09-01T23:59:59Z"}]}}`,
	}
	result := runCatalogMerge(t, opts)
	stocks := catalogField(t, result, "stocks")
	comments := catalogField(t, result, "comments")
	solds := catalogField(t, result, "solds")
	if len(stocks.Items) != 7 || len(comments.Items) != 1 || len(solds.Items) != 1 || catalogField(t, solds.Items[0], "sold").Integer != 9007199254740993 {
		t.Fatal("structural type keys, normalized date identity or exact integer changed", len(stocks.Items), len(comments.Items), solds)
	}
	raw, err := value.EncodeJSON(t.Context(), result)
	if err != nil {
		t.Fatal(err)
	}
	opts.current = string(raw)
	replayed := runCatalogMerge(t, opts)
	if len(catalogField(t, replayed, "stocks").Items) != 7 || len(catalogField(t, replayed, "comments").Items) != 1 || len(catalogField(t, replayed, "solds").Items) != 1 {
		t.Fatal("repeating an observation appended duplicate history")
	}
	for _, date := range []string{"2026-09-01T00:00:00.000-00:00", "2026-09-01T00:00:00+00:00", "2026-09-01T00:00:00Z"} {
		comment := map[string]any{"description": "same", "date": date}
		document := map[string]any{"uid": "p", "comments": []any{comment}}
		zeroOpts := catalogMergeOptions{product: true, current: `{"uid":"p","comments":[{"description":"same","date":"2026-09-01T00:00:00Z"}]}`, incoming: catalogEnvelope(t, document)}
		if got := runCatalogMerge(t, zeroOpts); len(catalogField(t, got, "comments").Items) != 1 {
			t.Fatal("zero fractional/time-zone identity changed", date)
		}
	}
}

func TestCatalogProductHistoryAndSellerTailLimits(t *testing.T) {
	var solds, stocks, comments, offers []any
	for i := 0; i < 35; i++ {
		sold := map[string]any{"sold": i, "period_hours": 24, "record_at": "2026-10-14"}
		stock := map[string]any{"stock": i}
		comment := map[string]any{"description": fmt.Sprintf("comment-%d", i)}
		solds, stocks, comments = append(solds, sold), append(stocks, stock), append(comments, comment)
	}
	for i := 0; i < 51; i++ {
		offer := map[string]any{"uid": fmt.Sprintf("seller-%d", i)}
		offers = append(offers, offer)
	}
	old := map[string]any{"uid": "p", "solds": solds[:25], "stocks": stocks[:25], "comments": comments[:25], "offers": offers}
	incoming := map[string]any{"uid": "p", "solds": solds[20:], "stocks": stocks[20:], "comments": comments[20:]}
	raw, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	opts := catalogMergeOptions{product: true, current: string(raw), incoming: catalogEnvelope(t, incoming)}
	result := runCatalogMerge(t, opts)
	for _, name := range []string{"solds", "stocks", "comments"} {
		if len(catalogField(t, result, name).Items) != 20 {
			t.Fatal("history tail changed", name)
		}
	}
	soldHistory := catalogField(t, result, "solds")
	if catalogField(t, soldHistory.Items[0], "sold").Integer != 15 || catalogField(t, soldHistory.Items[19], "sold").Integer != 34 {
		t.Fatal("deduplication/tail order changed", soldHistory)
	}
	sellerHistory := catalogField(t, result, "offers")
	if len(sellerHistory.Items) != 50 || catalogField(t, sellerHistory.Items[0], "uid").Text != "seller-1" || catalogField(t, sellerHistory.Items[49], "uid").Text != "seller-50" {
		t.Fatal("seller tail changed", sellerHistory)
	}
}

func TestCatalogProductReconciliationAndClassifierPatch(t *testing.T) {
	opts := catalogMergeOptions{product: true,
		current:  `{"uid":"p","platform":"shopify","url":"https://product.example/p","title":"keep","unknown":"keep","offers":[{"id":"match","uid":"match","url":"https://user:pass@[2001:db8::1]:8443/a","product_url":"","expiry_detected_at":"old","product_price":[{"price":1}]},{"id":"removed","uid":"removed","url":"https://[2001:db8::1]:8000/b"},{"id":"external","uid":"external","url":"https://other.example/a","expiry_detected_at":"old"}]}`,
		incoming: `{"observed_at":"2026-10-14T00:00:00Z","reconcile_offers":true,"offer_product_url_base":"https://product.example/p","offer_product_urls":{"match":"https://product.example/p?seller=match"},"document":{"uid":"p","platform":"shopify","offers":[{"id":"match","uid":"match","url":"https://[2001:db8::1]/a","product_price":[{"price":2}]}]}}`,
	}
	result := runCatalogMerge(t, opts)
	offers := catalogField(t, result, "offers")
	if catalogField(t, offers.Items[0], "product_url").Text != "https://product.example/p?seller=match" || catalogField(t, offers.Items[0], "expiry_detected_at").Kind != value.Null || catalogField(t, offers.Items[1], "expiry_detected_at").Text != catalogObservedAt || catalogField(t, offers.Items[2], "expiry_detected_at").Kind != value.Null {
		t.Fatal("userinfo/IPv6/port reconciliation or explicit null changed", offers)
	}
	prices := catalogField(t, offers.Items[0], "product_price")
	if catalogField(t, prices.Items[0], "price").Integer != 2 || catalogField(t, result, "unknown").Text != "keep" {
		t.Fatal("reconciliation lost latest price or unrelated field")
	}
	opts.current = strings.Replace(opts.current, `https://product.example/p`, `https://product.example/new`, 1)
	result = runCatalogMerge(t, opts)
	if catalogField(t, catalogField(t, result, "offers").Items[0], "product_url").Text != "" {
		t.Fatal("reconciliation applied a stale product URL")
	}
	opts.incoming = `{"observed_at":"2026-10-14T00:00:00Z","document":{"uid":"p"},"patch":{"ecommerce_class":"","brands":[]}}`
	result = runCatalogMerge(t, opts)
	if catalogField(t, result, "title").Text != "keep" || catalogField(t, result, "unknown").Text != "keep" || catalogField(t, result, "ecommerce_class").Kind != value.String || catalogField(t, result, "brands").Kind != value.Array || len(catalogField(t, result, "brands").Items) != 0 {
		t.Fatal("classifier patch failed to clear owned fields without replacing others")
	}
	opts.current = ""
	result = runCatalogMerge(t, opts)
	if catalogField(t, result, "uid").Text != "p" || catalogField(t, result, "first_found_at").Text != catalogObservedAt {
		t.Fatal("classifier patch could not initialize a missing product")
	}
}

func TestCatalogOfferMergeTrackingHistoryAndBasePlaceholder(t *testing.T) {
	opts := catalogMergeOptions{
		current:  `{"uid":"seller","name":"old","first_found_at":"first","sold":9007199254740993,"has_paypal":true,"unknown":"preserve only for base","evicted_at":"old","addresses":[{"raw":"road","country":"US"}],"contacts":[{"type":"email","value":"one"}],"tracking_ids":{"ga4":["old"],"script_urls":["old-script"]}}`,
		incoming: `{"observed_at":"2026-10-14T00:00:00Z","document":{"uid":"seller","name":"new","sold":0,"has_paypal":false,"addresses":[{"raw":"road","country":"US"},{"raw":"road2","country":"CA"}],"contacts":[{"type":"email","value":"one"},{"type":"phone","value":"two"}],"tracking_ids":{"ga4":["old","new"],"script_urls":["new-script"]}}}`,
	}
	result := runCatalogMerge(t, opts)
	tracking := catalogField(t, result, "tracking_ids")
	if catalogField(t, result, "name").Text != "new" || catalogField(t, result, "sold").Integer != 9007199254740993 || catalogField(t, result, "first_found_at").Text != "first" || catalogField(t, result, "last_found_at").Text != catalogObservedAt || len(catalogField(t, tracking, "ga4").Items) != 2 || len(catalogField(t, result, "addresses").Items) != 2 || len(catalogField(t, result, "contacts").Items) != 2 {
		t.Fatal("seller merge/tracking union/history/exact integer changed", result)
	}
	for _, name := range []string{"has_paypal", "evicted_at", "unknown"} {
		if catalogField(t, result, name).Kind != value.Missing {
			t.Fatal("normal seller rebuild retained an omitted field", name)
		}
	}
	opts.incoming = `{"observed_at":"2026-10-14T00:00:00Z","base":true,"document":{"uid":"seller","name":"placeholder"}}`
	result = runCatalogMerge(t, opts)
	if catalogField(t, result, "name").Text != "old" || catalogField(t, result, "unknown").Text != "preserve only for base" || catalogField(t, result, "has_paypal").Kind != value.Bool {
		t.Fatal("base placeholder replaced the complete existing seller")
	}
}

func TestCatalogOfferAddressTailRetainsTwoPassDeduplicationOrder(t *testing.T) {
	var addresses []any
	for index := 0; index < 18; index++ {
		address := map[string]any{"raw": fmt.Sprintf("road-%d", index), "country": "US"}
		addresses = append(addresses, address)
	}
	current := map[string]any{"uid": "seller", "addresses": addresses[:12]}
	incoming := map[string]any{"uid": "seller", "addresses": addresses[8:]}
	raw, err := json.Marshal(current)
	if err != nil {
		t.Fatal(err)
	}
	opts := catalogMergeOptions{current: string(raw), incoming: catalogEnvelope(t, incoming)}
	result := runCatalogMerge(t, opts)
	got := catalogField(t, result, "addresses")
	if len(got.Items) != 10 || catalogField(t, got.Items[0], "raw").Text != "road-8" || catalogField(t, got.Items[9], "raw").Text != "road-17" {
		t.Fatal("seller address history tail/order changed", got)
	}
}

func TestCatalogReconciliationHostnameParsingUsesBusinessAuthorityRules(t *testing.T) {
	urls := []string{
		"https://user:pass@shop.example:8443/path?q#f",
		"x+v.1-z://old@user@shop.example:443?query",
		"http://[2001:db8::1]:8080/p",
	}
	for _, incomingURL := range urls {
		oldURL := "https://shop.example/old"
		if strings.Contains(incomingURL, "[2001:") {
			oldURL = "https://[2001:db8::1]/old"
		}
		oldOffer := map[string]any{"uid": "old", "id": "old", "url": oldURL}
		newOffer := map[string]any{"uid": "new", "id": "new", "url": incomingURL}
		current := map[string]any{"uid": "p", "platform": "store", "offers": []any{oldOffer}}
		document := map[string]any{"uid": "p", "platform": "store", "offers": []any{newOffer}}
		envelope := map[string]any{"document": document, "observed_at": catalogObservedAt, "reconcile_offers": true}
		currentRaw, err := json.Marshal(current)
		if err != nil {
			t.Fatal(err)
		}
		incomingRaw, err := json.Marshal(envelope)
		if err != nil {
			t.Fatal(err)
		}
		opts := catalogMergeOptions{product: true, current: string(currentRaw), incoming: string(incomingRaw)}
		result := runCatalogMerge(t, opts)
		offer := catalogField(t, result, "offers").Items[0]
		if catalogField(t, offer, "expiry_detected_at").Text != catalogObservedAt {
			t.Fatal("a removed seller with the same authority was kept", incomingURL)
		}
	}
}
