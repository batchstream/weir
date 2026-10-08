package app

import (
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"go.yaml.in/yaml/v3"
)

// Duration is a Go duration encoded as a string, including its unit.
type Duration time.Duration

func (value Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(value).String())
}

func (value Duration) MarshalYAML() (any, error) {
	return time.Duration(value).String(), nil
}

func (value *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
		return errors.New("duration requires a string with a time unit")
	}

	parsed, err := time.ParseDuration(node.Value)
	if err != nil {
		return errors.New("invalid duration")
	}

	*value = Duration(parsed)
	return nil
}

// ByteSize is an integer byte count encoded with an explicit binary unit.
type ByteSize uint64

func (value ByteSize) MarshalJSON() ([]byte, error) {
	return json.Marshal(value.text())
}

func (value ByteSize) MarshalYAML() (any, error) {
	return value.text(), nil
}

func (value ByteSize) text() string {
	bytes := uint64(value)
	for _, unit := range []struct {
		name string
		size uint64
	}{
		{"GiB", 1 << 30},
		{"MiB", 1 << 20},
		{"KiB", 1 << 10},
	} {
		if bytes != 0 && bytes%unit.size == 0 {
			return strconv.FormatUint(bytes/unit.size, 10) + unit.name
		}
	}
	return strconv.FormatUint(bytes, 10) + "B"
}

func (value *ByteSize) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
		return errors.New("memory requires a string with a byte unit")
	}

	text := node.Value
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
