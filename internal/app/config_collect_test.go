package app

import (
	"strings"
	"testing"
)

func TestRemovedBatchCollectSettingIsRejected(t *testing.T) {
	input := "stores:\n  - name: records\n    mongodb:\n      uri: mongodb://127.0.0.1:27017\n    batch_collect: 0ms\n"
	if _, err := DecodeRouting(strings.NewReader(input)); err == nil {
		t.Fatal("removed batch_collect silently accepted")
	}
}
