# Independent PR review, 2026-10-16

A separate agent reviewed PR #74 at `a3a5ac046518b1a7d196e17ca6663078de721e67`.
It found one P2 regression: a listable MongoDB namespace without write permission
could reject a native bulk containing an otherwise authorized namespace. The
[original reproduction](review-auth-isolation-before.txt) preserves the failing
output and complete injected offline test source. MongoDB checks every bulk
privilege [before executing its writes](https://github.com/mongodb/mongo/blob/r8.0.0/src/mongo/db/commands/bulk_write.cpp#L1382-L1389).

The correction isolates namespaces only after a validated initial command-level
`Unauthorized` rejection: no cursor, write-concern uncertainty, execution counts
or unrecognized fields may be present. Zero or absent counts are accepted.
Active operations are then grouped by namespace, preserving their caller
positions and rechecking cancellation. Successful cross-namespace batches stay
combined. Transport loss, getMore rejection, per-item errors and ambiguous
responses cannot trigger this isolation path.

The same reviewer independently reran its original reproduction against the
correction under race detection: the allowed namespace succeeds and the denied
namespace keeps its own permission failure. The [after-fix output](review-auth-isolation-after.txt)
also covers malformed evidence, cancellation and no-replay boundaries. The
[focused review tests](review-focused-tests.txt) cover scheduler caches, ownership,
Search coalescing and MongoDB batching. No blocking findings remained after
reviewing the correction.

The [real TLS/SCRAM regression](review-auth-secure-integration.txt) verifies that
two authorized writes persist once, the denied namespace remains empty, and
the wire contains one rejected combined command followed by one command per
namespace. The fixture released its connections and stopped its owned process.
The complete MongoDB package also passes race detection.

The 25-case Store matrix and the 84-stage real database matrix retain their
original version-VII inputs and checksums. The real performance matrix was not
rerun after this authorization error-path correction; it is not a measurement
of the post-review binary. `post-review-server.receipt.json` and
`post-review-build-audit.json` identify the delivered source inputs separately.
Only MongoDB `batch.go` and two authorization regression tests differ from the
formatting checkpoint; Store runtime and Search sources are unchanged. Review
did not exercise sharded failover or production saturation.
