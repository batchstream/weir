# Batching evidence

Latest Store evidence is **VII**, including private per-ticket plan ownership.
The complete VI matrix and frozen source snapshots remain in
[`iteration6`](iteration6/). `iteration7-control-*` captures adjacent VI/VII
measurements showing unchanged allocations in three representative cases;
`baseline-plan-regression.txt` records the reproduced original accounting/race
failure.

Read the [main report](../batching-performance-2026-10-16.md) for methods,
limitations, iteration decisions and representative results. The separate
[real-database report](real-experiments.md) covers actual MongoDB/Search
commands, CPU and multiprocess throughput.

Accepted comparison inputs are `paired-baseline-runtime.txt` versus
`final-runtime.txt`, and `paired-baseline-selector.txt` versus
`final-selector.txt`. Each case has three measurements. `summary.csv` and
`summary.json` combine all 21 runtime cases and four selector cases.
`manifest.json` records hashes, source snapshots, commands and exclusions.
Earlier `baseline-*`, `iteration*` and ABBA files preserve rejected candidates,
the original timing period and the control for host/timer drift.

Recompute either pair from the repository root:

```sh
python3 docs/batching-results-2026-10-16/compare.py \
  docs/batching-results-2026-10-16/paired-baseline-runtime.txt \
  docs/batching-results-2026-10-16/final-runtime.txt > runtime-comparison.json
python3 docs/batching-results-2026-10-16/compare.py \
  docs/batching-results-2026-10-16/paired-baseline-runtime.txt \
  docs/batching-results-2026-10-16/final-runtime.txt --format csv > runtime-comparison.csv
```

Replace `runtime` with `selector` to recompute the other pair. The comparison
tool requires successful complete output and identical case sets; interrupted
and partial files are deliberately rejected.

These Store benchmarks use an instrumented fake adapter. They measure Store
scheduling, lifecycle overhead and aggregation. Their adapter-call counters
are not actual database wire commands or database CPU measurements.
