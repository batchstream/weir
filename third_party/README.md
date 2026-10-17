# Local dependency patches

Weir keeps two pinned source patches for allocation ownership. Both execute inside
the normal server process and preserve the upstream wire/language implementation.

| Module | Purpose | Details |
| --- | --- | --- |
| `google.golang.org/grpc v1.83.2` | Reserve ingress and transport buffer memory before allocation; release at native ownership completion | [Receive patch](grpc/WEIR_PATCH.md) |
| `github.com/iceisfun/golua v1.1.1` | Reserve parser, compiler, VM and allowed library allocations on the Node execution account | [Lua patch](golua/WEIR_PATCH.md) |

Each directory preserves upstream licensing, `WEIR_UPSTREAM.json` file hashes and
`WEIR_PATCH.diff`. The root `go.mod` replaces only these two pinned dependencies.
`scripts/check_dependencies.py` validates that boundary; packaging verifies source
identity and includes patch/license metadata.

Focused offline regression checks run from each module directory:

```sh
# third_party/grpc
go test -race -count=1 -run '^TestReceive' ./ ./mem ./internal/transport

# third_party/golua
go test -race -count=1 ./budget ./lexer ./parser ./compiler ./vm ./stdlib
```

CI prepares the test dependencies for all three modules before disabling downloads.
Preserve these focused checks when updating either patch. Do not edit the module
cache or substitute an unrecorded upstream version.
