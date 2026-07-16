# Per-tag fair scheduling for XrdClCurl

## Motivation

A single unresponsive upstream origin — one that accepts TCP connections
but never sends bytes — can hold every worker's slot in
`XrdClCurl::HandlerQueue::Consume` -> `CurlWorker::Run` until each curl
handle hits the header timeout (order of a minute). While that plays
out, the queue happily accepts new operations, most of which stall
behind the bad origin's operations. The cache appears unresponsive to
everyone.

This document sketches a per-tag fair scheduler for XrdClCurl that
bounds each origin's share of the worker pool so a single bad origin
cannot monopolise it.

## Goals

- One misbehaving origin cannot hold more than a bounded share of the
  worker pool.
- Starving (no first byte yet) and active (bytes flowing) transfers
  are counted separately, with a tighter cap on starving to keep the
  pathology out of the pool.
- Overflow is shed with a 429-equivalent error to the caller rather
  than queued indefinitely.
- Fair share across tags via a weighted probabilistic draw using an
  EMA of recent per-tag usage.
- Tag key is opaque; the initial implementation uses `URL::GetHostName()`
  and composes additional identifiers (username, path prefix) later.
- Failover behaviour: tag by first attempt, do NOT retag mid-flight.
  Small accounting leak bounded by the operation's total lifetime;
  retagging in-flight adds complexity for no operator-visible benefit.

## Constraint: minimise upstream drift

Most XrdClCurl code was upstreamed; the remaining local patches should
stay small. The scheduler therefore lives in new files
(`XrdClCurlTagScheduler.{hh,cc}`) with only small hooks into the
existing files:

- `CurlOperation` grows two `std::function<void()>` members
  (`m_on_first_byte`, `m_on_done`) plus an atomic guard so each fires
  exactly once.
- The response-body write callback fires `m_on_first_byte`.
- `CurlWorker::Run` fires `m_on_done` via a scope guard when the op
  is popped from `HandlerQueue::Consume`.
- `HandlerQueue` gains an optional `TagScheduler *`; when non-null,
  `Produce`/`Consume` delegate to it.
- `Factory::Init` constructs the scheduler from env vars and hands
  it to the `HandlerQueue`. `Factory::Monitor` ticks it periodically
  so the scheduler doesn't need its own thread just for EMA updates.

Everything else — FIFOs, per-tag counters, EMA state, weighted draw,
config knobs — is in the new file pair. See
`src/XrdClCurl/XrdClCurlTagScheduler.hh` for the class interface.

## Wiring the scheduler into HandlerQueue

Two options were considered:

**Wrap.** Leave `HandlerQueue` untouched and put the scheduler in
front of `Produce`, feeding the queue from an internal producer
thread. Zero upstream diff, but two hops (scheduler FIFO → shared
queue → worker) and two bounded buffers to reason about.

**Inject (chosen).** Give `HandlerQueue` an optional `TagScheduler *`;
when non-null, `Produce` calls `Admit` and `Consume` calls
`scheduler->Consume(timeout)`. When null, behaviour is unchanged.
One hop, one locking flow, and the two small edits to `HandlerQueue`
are cheaper than running a separate scheduler-to-queue producer
thread.

## Rejection propagation

`Admit` returning `false` synthesises a caller-visible error. Every
`Produce` call site constructs an operation with a `ResponseHandler *`;
on rejection the queue invokes
`handler->HandleResponse(new XRootDStatus(stError, errRetry, ...))`
and drops the op without producing. The XrdCl→HTTP layer maps
`errRetry` to a 429/503-shaped response so clients see a
retry-friendly signal.

## Config

Env vars:

| Env var                        | Type   | Default | Meaning                                                                                 |
| ------------------------------ | ------ | ------- | --------------------------------------------------------------------------------------- |
| `XRDCLCURL_STARVING_PERCENT`   | int    | 25      | Per-tag cap on transfers still waiting for first byte, as a percent of the worker pool. |
| `XRDCLCURL_ACTIVE_PERCENT`     | int    | 90      | Per-tag cap on total in-flight transfers, as a percent of the worker pool.              |
| `XRDCLCURL_PENDING_BUFFER`     | int    | 200     | Global admissions FIFO cap; 0 disables the scheduler entirely.                          |
| `XRDCLCURL_PENDING_PER_ORIGIN` | int    | 50      | Per-tag admissions FIFO cap.                                                            |
| `XRDCLCURL_EMA_SECONDS`        | int    | 30      | Time constant for the per-tag active-worker EMA used to weight the fairness draw.       |

Setting `XRDCLCURL_PENDING_BUFFER=0` disables the scheduler entirely.
Reading these lives in `Factory::Init` alongside the existing
`CurlMaxPendingOps` handling.

## Contention and scaling

The scheduler protects `m_tags`, `m_total_pending`, the EMA snapshot,
and the shutdown flag with a single `std::mutex`.

**Critical section length.** Each op under the lock does a hash-map
lookup, a `std::deque` op, and a couple of integer counter updates —
a handful of microseconds. `Consume`'s dispatch decision iterates
`m_tags` (bounded by the number of upstream origins the cache is
talking to *right now* — realistically 10–30 for a busy cache, not
thousands), does a weighted random draw, and pops one FIFO. Still
low single-digit microseconds. No I/O, curl calls, or allocations
larger than a `shared_ptr` reference under the lock.

**Contention rate.** For a 100-worker pool with ~10 tags each op
costs about 4 lock acquisitions (`Admit`, `OnFirstByte`, `Consume`,
`OnDone`). At 10 000 ops/sec that's 40 k lock acquisitions/sec —
well inside a single `std::mutex`. `HandlerQueue` already uses a
similar shape (one mutex, one deque, N workers plus M producers)
and isn't a bottleneck.

**Escape hatch: FIFO sharding.** If profiling shows the single mutex
saturating, the fix is to shard tag state by `hash(tag) % K` into
K sub-tables, each with its own mutex and FIFO. `Consume` locks
only the chosen shard; the eligibility scan becomes lock-free over
`std::atomic<int>` per-tag counters. Not worth building until
flame-graph evidence justifies it.

**No lock across a `wait`.** `Consume` uses a condition variable
when the queue is empty; the lock is released while the worker
sleeps, so an idle pool does not serialise.

## Non-goals for v1

- Retagging on failover. A CurlOperation that starts against origin
  A and retries against B keeps A's slot. Small fairness leak,
  bounded by the operation's total lifetime.
- Per-user tagging. Tag is only the URL host for now; the tag key is
  a `std::string` so `hostname + ":" + user` composes cleanly later
  without changing the scheduler.
- Adaptive caps (e.g. shrink the starving cap when the pool is under
  pressure). EMA already gives the input signal if we ever want it.

## Test plan

- Unit tests covering: accept-and-dispatch, global buffer full,
  per-tag pending full, starving cap, first-byte releases starving,
  active cap, cross-tag fairness, concurrent-admit stress.
- Integration test: a tight scheduler in front of a slow-serving
  origin plus a burst of concurrent requests; assert the extras are
  rejected on the wire as non-2xx while the admitted ones complete.
