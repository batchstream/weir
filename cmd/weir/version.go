package main

import (
	"encoding/json"
	"io"
	"runtime"
	"runtime/debug"
)

// revision is the source commit. Image and release builds set it with
// -X main.revision; other builds report the revision go build records.
var revision string

type versionIdentity struct {
	Product  string `json:"product"`
	Version  string `json:"version"`
	Revision string `json:"revision,omitempty"`
	Go       string `json:"go"`
	Target   string `json:"target"`
}

func printVersion(output io.Writer) error {
	identity := versionIdentity{
		Product:  "weir",
		Version:  "dev",
		Revision: revision,
		Go:       runtime.Version(),
		Target:   runtime.GOOS + "/" + runtime.GOARCH,
	}

	if info, ok := debug.ReadBuildInfo(); ok {
		// go build records module and VCS details when available; an injected revision wins.
		if info.Main.Version != "" && info.Main.Version != "(devel)" {
			identity.Version = info.Main.Version
		}
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" && identity.Revision == "" {
				identity.Revision = setting.Value
			}
		}
	}

	return json.NewEncoder(output).Encode(identity)
}
