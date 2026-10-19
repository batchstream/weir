package testexecution

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"unicode/utf8"
)

const maxJSON = 1 << 20
const maxBinary = 128 << 20

func decode(raw []byte, into any) error {
	if len(raw) == 0 || len(raw) > maxJSON || !utf8.Valid(raw) {
		return fmt.Errorf("JSON input missing or exceeds %d bytes", maxJSON)
	}
	tokens := json.NewDecoder(bytes.NewReader(raw))
	if err := jsonValue(tokens, 0); err != nil {
		return err
	}
	if _, err := tokens.Token(); err != io.EOF {
		return fmt.Errorf("trailing JSON or malformed input: %v", err)
	}
	var object any
	if err := json.Unmarshal(raw, &object); err != nil {
		return err
	}
	if err := exactFields(object, reflect.TypeOf(into)); err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(into); err != nil {
		return err
	}
	return nil
}
func exactFields(value any, typ reflect.Type) error {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	switch typ.Kind() {
	case reflect.Struct:
		object, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("JSON object required")
		}
		fields := make(map[string]reflect.Type)
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			tag := field.Tag.Get("json")
			name, _, _ := strings.Cut(tag, ",")
			if name != "" && name != "-" {
				fields[name] = field.Type
			}
		}
		for name, child := range object {
			field, ok := fields[name]
			if !ok {
				return fmt.Errorf("unknown JSON field %q", name)
			}
			if err := exactFields(child, field); err != nil {
				return err
			}
		}
	case reflect.Slice:
		if array, ok := value.([]any); ok {
			for _, child := range array {
				if err := exactFields(child, typ.Elem()); err != nil {
					return err
				}
			}
		}
	case reflect.Map:
		if object, ok := value.(map[string]any); ok {
			for _, child := range object {
				if err := exactFields(child, typ.Elem()); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
func jsonValue(dec *json.Decoder, depth int) error {
	if depth > 64 {
		return fmt.Errorf("JSON nesting exceeds bound")
	}
	token, err := dec.Token()
	if err != nil {
		return err
	}
	if token == nil {
		return fmt.Errorf("null is not an execution input")
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		keys := make(map[string]bool)
		for dec.More() {
			key, err := dec.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || keys[name] {
				return fmt.Errorf("duplicate or invalid JSON key %q", name)
			}
			keys[name] = true
			if err := jsonValue(dec, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for dec.More() {
			if err := jsonValue(dec, depth+1); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter")
	}
	_, err = dec.Token()
	return err
}

// Reject aliases rather than silently following symlinks. OpenRoot below also
// confines reads if a parent is exchanged after the initial path check.
func canonicalPath(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return fmt.Errorf("clean absolute path required")
	}
	actual, err := filepath.EvalSymlinks(path)
	if err != nil {
		return err
	}
	if actual != path {
		return fmt.Errorf("symlink path forbidden: %s", path)
	}
	return nil
}
func readFile(path string, limit int64) ([]byte, error) {
	if sensitivePath(filepath.ToSlash(path)) {
		return nil, fmt.Errorf("sensitive input path forbidden")
	}
	return readRegularFile(path, limit)
}

// Only readFile and the manifest-qualified public gRPC source reader call this
// primitive. It does not grant a sensitive-path exception on its own.
func readRegularFile(path string, limit int64) ([]byte, error) {
	file, _, err := openRegularFile(path, limit)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > limit {
		return nil, fmt.Errorf("file exceeds bound")
	}
	return raw, nil
}

// Return a pinned descriptor and its fstat, never a later stat of its pathname.
func openRegularFile(path string, limit int64) (*os.File, os.FileInfo, error) {
	if err := canonicalPath(path); err != nil {
		return nil, nil, err
	}
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return nil, nil, err
	}
	defer root.Close()
	before, err := root.Lstat(filepath.Base(path))
	if err != nil {
		return nil, nil, err
	}
	if !before.Mode().IsRegular() || before.Size() > limit {
		return nil, nil, fmt.Errorf("bounded non-symlink regular file required")
	}
	file, err := root.Open(filepath.Base(path))
	if err != nil {
		return nil, nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}
	if !os.SameFile(before, stat) || !stat.Mode().IsRegular() || stat.Size() > limit {
		_ = file.Close()
		return nil, nil, fmt.Errorf("bounded regular file required: %s", path)
	}
	return file, stat, nil
}
func DecodeProfile(raw []byte) (ExecutionProfile, error) {
	var p ExecutionProfile
	if err := decode(raw, &p); err != nil {
		return p, err
	}
	return p, validateProfile(p)
}
func DecodeProvenance(raw []byte) (ProvenanceInputs, error) {
	var p ProvenanceInputs
	if err := decode(raw, &p); err != nil {
		return p, err
	}
	return p, validateProvenanceShape(p)
}
