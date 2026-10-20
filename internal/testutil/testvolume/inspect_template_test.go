package main

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"text/template"
)

const oldTmpfsExpression = `{{json .HostConfig.Tmpfs}}`
const safeTmpfsExpression = `{{json (index .HostConfig "Tmpfs")}}`

func productionInspectFormat(t *testing.T) string {
	t.Helper()
	path := filepath.Join("..", "..", "..", "scripts", "test-production-volume-linux.py")
	source, err := boundedRead(path)
	if err != nil {
		t.Fatal(err)
	}
	_, body, found := strings.Cut(source, "CONTAINER_FORMAT = (")
	if !found {
		t.Fatal("production selected inspect formatter missing")
	}
	body, _, found = strings.Cut(body, "\nIMAGE_FORMAT = ")
	if !found {
		t.Fatal("production selected inspect formatter boundary missing")
	}
	// Read only this constant's adjacent unescaped literals, never execute Python.
	// A different source representation fails closed instead of guessing its value.
	var result strings.Builder
	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		line = strings.TrimSuffix(strings.TrimSpace(line), ")")
		if len(line) < 2 || line[0] != '\'' || line[len(line)-1] != '\'' {
			t.Fatal("selected inspect formatter must use adjacent quoted literals")
		}
		literal := line[1 : len(line)-1]
		if strings.ContainsAny(literal, "'\\") {
			t.Fatal("escaped selected inspect formatter literal requires explicit test update")
		}
		result.WriteString(literal)
	}
	return result.String()
}

func inspectTemplateJSON(value any) (string, error) {
	raw, err := json.Marshal(value)
	return string(raw), err
}

func executeInspectTemplate(format string, object map[string]any) ([]byte, error) {
	funcs := template.FuncMap{"json": inspectTemplateJSON}
	renderer, err := template.New("inspect").Option("missingkey=error").Funcs(funcs).Parse(format)
	if err != nil {
		return nil, err
	}
	var result bytes.Buffer
	err = renderer.Execute(&result, object)
	return result.Bytes(), err
}

func inspectTemplateFixture(format string) map[string]any {
	host := make(map[string]any)
	for _, field := range regexp.MustCompile(`\.HostConfig\.(\w+)`).FindAllStringSubmatch(format, -1) {
		host[field[1]] = nil
	}
	delete(host, "Tmpfs") // Docker28.0.4 omits this unconfigured optional raw field.
	restart := map[string]any{"Name": "no"}
	host["RestartPolicy"] = restart
	config := make(map[string]any)
	for _, field := range regexp.MustCompile(`\.Config\.(\w+)`).FindAllStringSubmatch(format, -1) {
		config[field[1]] = nil
	}
	labels := map[string]any{"weir.owner": "known-fixture"}
	config["Labels"] = labels
	network := map[string]any{"Ports": nil}
	object := map[string]any{
		"Id": "known-id", "Name": "known-name", "Image": "known-image",
		"State": nil, "Mounts": nil, "Config": config, "HostConfig": host,
		"NetworkSettings": network,
	}
	return object
}

func oldInspectFormat(format string) string {
	return strings.Replace(format, safeTmpfsExpression, oldTmpfsExpression, 1)
}

func TestInspectTmpfsTemplateActualOldRed(t *testing.T) {
	format := oldInspectFormat(productionInspectFormat(t))
	object := inspectTemplateFixture(format)
	_, err := executeInspectTemplate(format, object)
	// Actual CI39343925588 Docker28.0.4 error action and column, with a named template.
	actual := `:1:952: executing "inspect" at <.HostConfig.Tmpfs>: map has no entry for key "Tmpfs"`
	if err == nil || !strings.Contains(err.Error(), actual) {
		t.Fatalf("expected actual old missing-Tmpfs error, got %v", err)
	}
	t.Logf("OLD RED: %v", err)
}

func TestInspectTmpfsTemplateProductionSelectedValues(t *testing.T) {
	format := productionInspectFormat(t)
	if strings.Count(format, safeTmpfsExpression) != 1 || strings.Contains(format, oldTmpfsExpression) {
		t.Fatal("production selected Tmpfs expression is not the safe map index")
	}
	empty := map[string]string{}
	nonempty := map[string]string{"/tmp": "rw,size=1"}
	list := []string{}
	cases := []struct {
		name    string
		present bool
		value   any
		want    string
	}{
		{name: "omitted", want: "null"},
		{name: "null", present: true, want: "null"},
		{name: "empty map", present: true, value: empty, want: "{}"},
		{name: "nonempty map", present: true, value: nonempty, want: `{"/tmp":"rw,size=1"}`},
		{name: "false", present: true, value: false, want: "false"},
		{name: "zero", present: true, value: 0, want: "0"},
		{name: "empty string", present: true, value: "", want: `""`},
		{name: "list", present: true, value: list, want: "[]"},
	}
	for _, item := range cases {
		t.Run(item.name, func(t *testing.T) {
			object := inspectTemplateFixture(oldInspectFormat(format))
			host := object["HostConfig"].(map[string]any)
			if item.present {
				host["Tmpfs"] = item.value
			}
			raw, err := executeInspectTemplate(format, object)
			if err != nil {
				t.Fatal(err)
			}
			var selected map[string]json.RawMessage
			if err = json.Unmarshal(raw, &selected); err != nil {
				t.Fatal(err)
			}
			value, exists := selected["Tmpfs"]
			if !exists || string(value) != item.want {
				t.Fatalf("selected Tmpfs missing or changed: exists=%v value=%s", exists, value)
			}
		})
	}
	t.Log("NEW GREEN: actual production full format emits required Tmpfs:null for the same omitted raw key; other values remain unmodified for strict controller validation")
}

func TestInspectTmpfsTemplateOtherMissingFieldsRemainErrors(t *testing.T) {
	format := productionInspectFormat(t)
	for _, field := range []string{"HostConfig", "Mounts", "SecurityOpt"} {
		t.Run(field, func(t *testing.T) {
			object := inspectTemplateFixture(oldInspectFormat(format))
			if field == "SecurityOpt" {
				delete(object["HostConfig"].(map[string]any), field)
			} else {
				delete(object, field)
			}
			if _, err := executeInspectTemplate(format, object); err == nil {
				t.Fatal("unrelated missing selected field was silently defaulted")
			}
		})
	}
}
