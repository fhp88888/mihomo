# Alpha into Beta — 2026-10-07

## Scope and resolution

Merge Alpha `c3b2cb5fdf868964c1fddd0ba58e1e070f19bf28` into Beta
`4c1b9f2e06d07ed12bc8b44a3cee0ca7bd4bf64d`. Their common ancestor is
`87d7a4365ac4f65474f573ba68dce1ee47e22e6d`.

The user requested that Smart retain Beta's implementation and that other
components incorporate as many Alpha updates as possible.

- Preserve Beta's Smart group, TCP/UDP routing, discovery coordinator, TTFB
  scoring and priors, exploration/risk accounting, route-table persistence,
  aggregation, memory/cache implementation, TCP statistics, and health recovery.
- Accept Alpha's protocol, DNS, sniffing, domain matching, network stack,
  configuration, dependency, and general connection-wrapper updates.
- Keep Alpha's `adapter/adapter.go` updates. Its new `StatusProbe`/`ExitProbe`
  methods require response and exit parsing helpers; retain `response.go` and
  extract the independent exit probe helpers into `exit_probe.go`. These helpers
  do not initialize an exit watcher or affect Beta's Smart candidate selection.
- Initially restore `atomic.TypedValue.Update` and `lru.ResetLRU` for Beta's
  retained Smart code. The subsequent adaptation below removes both shims and
  uses Alpha's common libraries verbatim.
- Keep Alpha's load-balance hash-key tests. Exclude four tests embedded in the
  same file that exercise Alpha's unadopted Smart exit-watcher implementation.
- Keep Beta's two-second dial timeout and raw TCP connect timer, alongside
  Alpha's listener improvements.

At the initial merge, the original ten principal Smart implementation files
were byte-identical to the pre-merge Beta version. File hashes and binary fingerprints are recorded in the
validation provenance artifact.

## Verification

Passed:

```sh
go test ./component/smart/... ./adapter/outboundgroup ./adapter/provider \
  ./component/dialer ./common/callback ./adapter \
  ./component/profile/cachefile ./hub/route
go test -tags with_gvisor ./...
go test -race ./component/smart ./adapter/outboundgroup ./adapter/provider
```

Both benchmark binaries use identical build flags:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -tags with_gvisor -trimpath \
  -o <binary> .
```

## System performance comparison

Scenario: `mihomo-benchmark/configs/advanced-web-loading-bench.yaml`.
Both cores run Smart with the same seed (`20261001`), full 10-client/32-page
HTTP/2 workload, original kernel first-hop topology, and 1200-second deadline.
Each run starts a fresh isolated environment. No link parameters, scenario
resource sizes, routes, or Smart settings are changed.

Three matched pairs alternate merged-first and baseline-first. Builds and Go
tests finish before these measurements. An earlier baseline run, made while
compilation/tests were active, is retained as a pilot and excluded from results.

The existing complete-workload validator checks every run's 320 pages, 2880
requests, deterministic request IDs, page membership, response/route assertions,
verified unshaped client gateway, environment, and harness fingerprints. It
allows different core fingerprints for this cross-core comparison. Both runs
use Smart; the comparison is **not** an Oracle/OPT ratio.

Primary metric: sum of all page durations, with relative change defined as
`100 * (merged / baseline - 1)`; negative means faster after merging. Also record
per-run median, P90 and P95 page durations. Repeated full runs measure observed
variation; they cannot establish literally zero regression across all workloads.

All six formal runs passed: 960 pages and 8640 requests per core (1920 pages,
17280 requests total). Every run recorded 520 Smart establishments; exploration
outcomes were present, with the exact per-run counts in `smart-validation.json`.

| Pair | Baseline page total (s) | Merged page total (s) | Change |
| --- | ---: | ---: | ---: |
| 1 | 2502.559 | 2569.272 | +2.67% |
| 2 | 3453.059 | 2441.163 | -29.30% |
| 3 | 2492.677 | 2489.402 | -0.13% |

Aggregate page time: baseline 8448.295s versus
merged 7499.836s, a
-11.23% change. This is the sum of page durations,
not wall-clock runtime.

Interpretation: these runs show no consistent performance regression. Pair 1
was 2.67% slower, pair 3 was effectively unchanged, and pair 2 was substantially
faster because its baseline run was unusually slow. The aggregate improvement
must not be interpreted as a stable 11.23% speedup. A sensitivity calculation
using pairs 1 and 3 gives +1.27%; all three pairs remain in the official
aggregate. With three pairs and large run-level variation, this does not prove
statistical equivalence or guarantee zero regression in every workload.

One reference-proxy startup failure occurred before any requests in pair 1's
baseline. Its JSON and log are preserved; only this zero-request startup failure
was retried, following the harness's existing policy. There were no failures
within the six completed formal workloads.


Local artifacts are under
`../mihomo-benchmark/results/alpha-beta-merge-20261007/`:
`run_core.py`, `run_pairs.py`, `provenance.json`, `summary.json`, per-run JSON/CSV,
and progress logs. Go test logs and binaries are under
`bin/alpha-merge-validation/`.

## Follow-up: adapt Smart to Alpha common libraries

At the user's request, `common/atomic/value.go` and `common/lru/lrucache.go` now
match Alpha exactly; no `TypedValue.Update` or `ResetLRU` compatibility shim
remains. Smart routing and scoring policies remain Beta's implementation.

- Smart serializes queue read-modify-write operations with a local mutex and
  publishes immutable slices using Alpha's `TypedValue.Load`/`Store`. Append,
  drain, filtering, initialization, and failed-write re-enqueue share this lock.
  Atomic readers do not need to take the lock. Update callbacks execute once,
  avoiding the side effects of retries in the previous CAS callback.
- The six Smart caches use Alpha's `SetMaxSize` in place. Their pointers and
  existing expiration times remain stable; shrinking immediately evicts excess
  least-recently-used entries. Initialization still assigns 300-second TTLs
  (1800 seconds for the unwrap cache).
- Strengthen the concurrent persistence test to verify all 200 submitted
  records and their contents in bbolt, as well as an empty final queue.

Follow-up checks passed:

```sh
go test -race ./component/smart ./adapter/outboundgroup ./adapter/provider \
  ./common/atomic ./common/lru
go test -tags with_gvisor ./...
```

One supplemental full `advanced-web-loading-bench` run passed all 320 pages,
2880 requests and complete-workload/route validation against each of the six
historical references. Build flags and workload settings are unchanged.
Page duration total was 2538.882592414 seconds, within the previous merged
range (2441.162982241–2569.271535588 seconds), +1.988% versus its historical
median and +1.451% versus the pre-merge historical median. This is a single
supplemental run, not a new paired experiment or proof of zero regression.
Results: `mihomo-benchmark/results/alpha-beta-merge-20261007/alpha-common.json`
and `alpha-common-validation.json` (ignored benchmark artifacts).
