package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

const corpusSize = 1000
const seed = "weir-m22-seed-20260928"

func readID(n int) string {
	// Independent deterministic uniform hash selection, modulo bias < 1/2^64.
	h := sha256.Sum256([]byte(fmt.Sprintf("%s/%d", seed, n)))
	v := uint64(0)
	for _, b := range h[:8] {
		v = v<<8 | uint64(b)
	}
	return fmt.Sprintf("read-%04d", v%corpusSize)
}

func payload(id string) []byte {
	out := make([]byte, 0, 1024)
	out = append(out, `{"data":"`...)
	for n := 0; len(out) < 1022; n++ {
		h := sha256.Sum256([]byte(fmt.Sprintf("%s/%s/%d", seed, id, n)))
		out = hex.AppendEncode(out, h[:])
	}
	out = out[:1022]
	out = append(out, '"', '}')
	return out
}

func validPayload(raw []byte, id string) bool { return bytes.Equal(raw, payload(id)) }
