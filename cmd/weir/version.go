package main

import (
	"encoding/json"
	"io"
	"runtime"
	"runtime/debug"
)

// Set only by the clean-commit packaging entry point. Ordinary builds stay dev.
var sourceRevision string

func printVersion(output io.Writer) error {
	identity := struct {
		Product  string `json:"product"`
		Version  string `json:"version"`
		Revision string `json:"revision"`
		Go       string `json:"go"`
		Target   string `json:"target"`
		State    string `json:"state"`
		Dirty    string `json:"dirty"`
	}{
		"weir",
		"dev",
		"unknown",
		runtime.Version(),
		runtime.GOOS + "/" + runtime.GOARCH,
		"dev",
		"unknown",
	}

	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				identity.Revision = setting.Value
			case "vcs.modified":
				identity.Dirty = setting.Value
			}
		}
	}

	if sourceRevision != "" {
		identity.Revision = sourceRevision
		identity.Version = "local-" + sourceRevision
		identity.State = "clean-commit"
		identity.Dirty = "false"
	}

	return json.NewEncoder(output).Encode(identity)
}
