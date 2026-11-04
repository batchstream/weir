# Contributing to Weir

Focused bug fixes, documentation improvements, tests and feature proposals are
welcome. For changes to the public protocol or to batching and write semantics,
open an [issue](https://github.com/batchstream/weir/issues/new/choose) first so the server,
[protocol](https://github.com/batchstream/weir-protocol), [Go SDK](https://github.com/batchstream/weir-go),
[Helm chart](https://github.com/batchstream/weir-charts) and [system tests](https://github.com/batchstream/weir-tests)
can change together.

## Report a problem

Use the issue forms and include the output of `weir version`, the backend and its
version, the SDK version, the expected result and a minimal reproduction. Include
relevant logs and configuration with credentials and private data removed. For
capacity reports, describe the workload, replica count and resource limits.

For vulnerabilities, follow [SECURITY.md](SECURITY.md).

## Work locally

Install the Go version declared in [go.mod](go.mod).

```sh
git clone https://github.com/batchstream/weir.git
cd weir
go build -o bin/weir ./cmd/weir
go test ./...
go test -race ./...
go vet ./...
go vet -tags=integration ./...
```

Default tests need no backends.
[Integration, load and fault tests](docs/development.md#integration-tests) use Docker and the
local fixtures under `scripts/`.

## Prepare a pull request

- Keep the change focused and explain the observed problem and resulting behavior.
- Add a regression test when fixing behavior.
- Update the documentation and the [reference configuration](config/weir.reference.yaml) when configuration or APIs change.
- Change public schemas in [weir-protocol](https://github.com/batchstream/weir-protocol), and add blackbox and cross-component tests to [weir-tests](https://github.com/batchstream/weir-tests).
