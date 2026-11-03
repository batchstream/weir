package app

import (
	"errors"
	"math"
	"strconv"
	"time"

	"go.yaml.in/yaml/v3"
)

// Duration is a Go duration encoded as a string, including its unit.
type Duration time.Duration

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

// Count is a YAML integer; fractional and quoted values are rejected.
type Count int

func (value *Count) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode || node.ShortTag() != "!!int" {
		return errors.New("count requires an integer")
	}
	var parsed int
	if err := node.Decode(&parsed); err != nil {
		return errors.New("invalid count")
	}
	*value = Count(parsed)
	return nil
}

// ByteSize is an integer byte count encoded with an explicit binary unit.
type ByteSize int

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

	var multiplier int
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

	amount, err := strconv.Atoi(text[:end])
	if err != nil || amount > math.MaxInt/multiplier {
		return errors.New("byte size exceeds bound")
	}

	*value = ByteSize(amount * multiplier)
	return nil
}
