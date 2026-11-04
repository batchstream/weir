# Security policy

## Report a vulnerability

Use GitHub's private
[Report a vulnerability](https://github.com/batchstream/weir/security/advisories/new)
form. Include the affected Weir version or commit, reproduction steps, impact,
and relevant deployment settings with credentials and private data removed.
Please keep exploit details out of public issues while a report is being
investigated.

## Versions

Weir is pre-1.0. Check whether an issue is still present on the latest commit of
`main`. Older versions do not have a guaranteed security backport schedule. Report
suspected vulnerabilities even if you cannot reproduce them on `main`.

## Deployment guidance

The application listener uses plaintext gRPC and belongs on an isolated network.
Backend authentication stays local to Weir. Review
[Configuration](docs/configuration.md) and [Operations](docs/operations.md) for
dependency access, resource limits and failure handling.
