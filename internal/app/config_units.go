package app

import (
	"encoding/json"
	"errors"
	"strconv"
	"time"
)

// Duration is a Go duration encoded as a JSON string, including its unit.
type Duration time.Duration

func (value Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(value).String())
}

func (value *Duration) UnmarshalJSON(raw []byte) error {
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return errors.New("duration requires a string with a time unit")
	}

	parsed, err := time.ParseDuration(text)
	if err != nil {
		return errors.New("invalid duration")
	}

	*value = Duration(parsed)
	return nil
}

// ByteSize is an integer byte count encoded with an explicit binary unit.
type ByteSize uint64

func (value ByteSize) MarshalJSON() ([]byte, error) {
	bytes := uint64(value)
	for _, unit := range []struct {
		name string
		size uint64
	}{
		{"GiB", 1 << 30},
		{"MiB", 1 << 20},
		{"KiB", 1 << 10},
		{"B", 1},
	} {
		if bytes != 0 && bytes%unit.size == 0 || unit.size == 1 {
			return json.Marshal(strconv.FormatUint(bytes/unit.size, 10) + unit.name)
		}
	}

	return nil, errors.New("invalid byte size")
}

func (value *ByteSize) UnmarshalJSON(raw []byte) error {
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return errors.New("memory requires a string with a byte unit")
	}

	end := 0
	for end < len(text) && text[end] >= '0' && text[end] <= '9' {
		end++
	}
	if end == 0 {
		return errors.New("invalid byte size")
	}

	var multiplier uint64
	switch text[end:] {
	case "B":
		multiplier = 1
	case "KiB":
		multiplier = 1 << 10
	case "MiB":
		multiplier = 1 << 20
	case "GiB":
		multiplier = 1 << 30
	default:
		return errors.New("invalid byte size unit")
	}

	amount, err := strconv.ParseUint(text[:end], 10, 64)
	if err != nil || amount > ^uint64(0)/multiplier {
		return errors.New("byte size exceeds bound")
	}

	*value = ByteSize(amount * multiplier)
	return nil
}
