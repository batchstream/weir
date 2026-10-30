package search

import (
	"bytes"
	"encoding/json/jsontext"
	"strconv"
	"strings"

	"github.com/batchstream/weir/internal/execution"
)

// optional is a JSON integer that may be absent or null.
type optional struct {
	value int64
	set   bool
}

type bulkShards struct {
	total, successful, failed optional
}

// bulkItem is one action's acknowledgement in a _bulk reply.
type bulkItem struct {
	index, id, result  string
	version, seq, term optional
	shards             bulkShards
	hasShards          bool
	errorType          string
	hasError           bool
	status             int64
}

// bulkReply holds the items of a 200 _bulk reply in action order.
type bulkReply struct {
	items []bulkItem
}

// decodeBulkReply reads a validated 200 _bulk reply in one pass. It requires
// errors and took, and exactly one item per work: an object whose only name is
// the work's action, naming the work's index and id. errors must be set
// exactly when an item failed. Names match exactly, and a name that differs
// from a known one only in case rejects the reply. Null reads as absent, and a
// value of another type rejects the reply.
func decodeBulkReply(raw []byte, works []*execution.Plan) (bulkReply, bool) {
	reply := bulkReply{}
	decoder := jsontext.NewDecoder(bytes.NewBuffer(raw))
	if !readDelim(decoder, '{') {
		return reply, false
	}
	var errors bool
	var errorsSet, tookSet bool
	for decoder.PeekKind() == '"' {
		name, err := decoder.ReadValue()
		if err != nil {
			return reply, false
		}
		ok := true
		switch string(name) {
		case `"errors"`:
			errors, errorsSet, ok = readBool(decoder)
		case `"took"`:
			var took optional
			took, ok = readInteger(decoder)
			tookSet = took.set
		case `"items"`:
			reply.items, ok = readBulkItems(decoder, works)
		default:
			ok = skipUnknown(decoder, name, "errors", "took", "items")
		}
		if !ok {
			return reply, false
		}
	}
	if !readDelim(decoder, '}') || !errorsSet || !tookSet || len(reply.items) != len(works) {
		return reply, false
	}
	failed := false
	for i := range reply.items {
		failed = failed || reply.items[i].hasError
	}
	return reply, failed == errors
}

func readBulkItems(decoder *jsontext.Decoder, works []*execution.Plan) ([]bulkItem, bool) {
	if decoder.PeekKind() == 'n' {
		_, err := decoder.ReadToken()
		return nil, err == nil
	}
	if !readDelim(decoder, '[') {
		return nil, false
	}
	items := make([]bulkItem, 0, len(works))
	for decoder.PeekKind() != ']' {
		if len(items) == len(works) {
			return nil, false
		}
		native := works[len(items)].Backend.(*plan)
		items = append(items, bulkItem{})
		if !readBulkEntry(decoder, native, &items[len(items)-1]) {
			return nil, false
		}
	}
	return items, readDelim(decoder, ']')
}

// readBulkEntry reads one {"<action>": item} entry for native.
func readBulkEntry(decoder *jsontext.Decoder, native *plan, item *bulkItem) bool {
	if !readDelim(decoder, '{') {
		return false
	}
	action := native.bulkAction()
	name, err := decoder.ReadValue()
	if err != nil || len(name) != len(action)+2 || string(name[1:len(name)-1]) != action {
		return false
	}
	if !readBulkItem(decoder, item) || !readDelim(decoder, '}') {
		return false
	}
	return item.index == native.index && item.id == native.id
}

var bulkItemNames = []string{"_index", "_id", "result", "_version", "_seq_no", "_primary_term", "_shards", "error", "status"}

func readBulkItem(decoder *jsontext.Decoder, item *bulkItem) bool {
	if !readDelim(decoder, '{') {
		return false
	}
	for decoder.PeekKind() == '"' {
		name, err := decoder.ReadValue()
		if err != nil {
			return false
		}
		ok := true
		switch string(name) {
		case `"_index"`:
			item.index, ok = readString(decoder)
		case `"_id"`:
			item.id, ok = readString(decoder)
		case `"result"`:
			item.result, ok = readString(decoder)
		case `"_version"`:
			item.version, ok = readInteger(decoder)
		case `"_seq_no"`:
			item.seq, ok = readInteger(decoder)
		case `"_primary_term"`:
			item.term, ok = readInteger(decoder)
		case `"_shards"`:
			ok = readShards(decoder, item)
		case `"error"`:
			ok = readError(decoder, item)
		case `"status"`:
			var status optional
			status, ok = readInteger(decoder)
			item.status = status.value
		default:
			ok = skipUnknown(decoder, name, bulkItemNames...)
		}
		if !ok {
			return false
		}
	}
	return readDelim(decoder, '}')
}

func readShards(decoder *jsontext.Decoder, item *bulkItem) bool {
	if decoder.PeekKind() == 'n' {
		_, err := decoder.ReadToken()
		return err == nil
	}
	if !readDelim(decoder, '{') {
		return false
	}
	item.hasShards = true
	for decoder.PeekKind() == '"' {
		name, err := decoder.ReadValue()
		if err != nil {
			return false
		}
		ok := true
		switch string(name) {
		case `"total"`:
			item.shards.total, ok = readInteger(decoder)
		case `"successful"`:
			item.shards.successful, ok = readInteger(decoder)
		case `"failed"`:
			item.shards.failed, ok = readInteger(decoder)
		default:
			ok = skipUnknown(decoder, name, "total", "successful", "failed")
		}
		if !ok {
			return false
		}
	}
	return readDelim(decoder, '}')
}

func readError(decoder *jsontext.Decoder, item *bulkItem) bool {
	if decoder.PeekKind() == 'n' {
		_, err := decoder.ReadToken()
		return err == nil
	}
	if !readDelim(decoder, '{') {
		return false
	}
	item.hasError = true
	for decoder.PeekKind() == '"' {
		name, err := decoder.ReadValue()
		if err != nil {
			return false
		}
		ok := true
		if string(name) == `"type"` {
			item.errorType, ok = readString(decoder)
		} else {
			ok = skipUnknown(decoder, name, "type")
		}
		if !ok {
			return false
		}
	}
	return readDelim(decoder, '}')
}

// skipUnknown skips the value of an unknown name. A name that differs from one
// of known only in case is ambiguous evidence and rejects the reply.
func skipUnknown(decoder *jsontext.Decoder, name []byte, known ...string) bool {
	unquoted := string(name[1 : len(name)-1])
	for _, candidate := range known {
		if strings.EqualFold(unquoted, candidate) {
			return false
		}
	}
	return decoder.SkipValue() == nil
}

func readDelim(decoder *jsontext.Decoder, kind jsontext.Kind) bool {
	token, err := decoder.ReadToken()
	return err == nil && token.Kind() == kind
}

func readBool(decoder *jsontext.Decoder) (value, set, ok bool) {
	token, err := decoder.ReadToken()
	if err != nil {
		return false, false, false
	}
	switch token.Kind() {
	case 'n':
		return false, false, true
	case 't', 'f':
		return token.Bool(), true, true
	}
	return false, false, false
}

func readString(decoder *jsontext.Decoder) (string, bool) {
	token, err := decoder.ReadToken()
	if err != nil {
		return "", false
	}
	switch token.Kind() {
	case 'n':
		return "", true
	case '"':
		return token.String(), true
	}
	return "", false
}

// readInteger reads a JSON number without a fraction or exponent that fits
// an int64, or null.
func readInteger(decoder *jsontext.Decoder) (optional, bool) {
	number := optional{}
	if decoder.PeekKind() != '0' {
		token, err := decoder.ReadToken()
		return number, err == nil && token.Kind() == 'n'
	}
	value, err := decoder.ReadValue()
	if err != nil {
		return number, false
	}
	number.value, err = strconv.ParseInt(string(value), 10, 64)
	number.set = err == nil
	return number, number.set
}
