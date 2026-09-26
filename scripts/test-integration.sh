#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
# failCommand is server-global even when scoped by appName: serialize packages.
WEIR_INTEGRATION=1 go test -p 1 -tags integration "$@" ./...
