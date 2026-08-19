# market-maker

A high-frequency market-making engine: a price-time-priority matching engine
and synthetic limit order book in Rust, an Avellaneda-Stoikov optimal
quoting strategy with real-time volatility estimation, VaR/Expected
Shortfall risk limits with a kill switch, and a live web dashboard.

**Scope, stated honestly up front:** this simulates a market and trades
against it — it does not connect to a real exchange. That's a deliberate
choice, not a shortcut: this was built in a sandboxed environment with no
path to a live venue, and the standard way to develop and validate a
market-making strategy is against a simulator *before* it ever sees a real
order book anyway. Everything else — the matching logic, the concurrency
pattern, the quoting math, the risk controls — is built the way you'd build
it for real. The [Limitations](#limitations--what-a-real-system-would-add)
section is specific about exactly where the simplifications are.

## Architecture

```
┌─────────────────────┐        UDP          ┌────────────────┐      lock-free ring buffer      ┌──────────────────────┐
│   Matching Engine    │ ──── (seq'd L3 ───▶ │  Feed Handler   │ ───────────────────────────────▶│   Strategy Thread     │
│  (ground-truth book, │      packets,       │ (gap detection, │      SeqEvent { seq, event }     │ (shadow book rebuilt  │
│   price process,     │    ~0.05% loss      │  resync trigger)│                                   │  purely by replaying  │
│   background flow)   │     injected)       └────────────────┘                                   │  the L3 feed; A-S     │
└──────────┬───────────┘                                                                           │  quoting; risk/VaR;   │
           │        ◀──────────────────── lock-free ring buffer ─────────────────────────────────  │  kill switch)         │
           │             StrategyCommand { Quote | CancelAll }                                      └───────────┬───────────┘
           │                                                                                                     │
           │  periodic full-book resync (Mutex, sequence-gated so it can only move forward)                     │
           └─────────────────────────────────────────────────────────────────────────────────────────────────────┘
                                                                                                                   │
                                                                                                    Arc<Mutex<Snapshot>>
                                                                                                                   ▼
                                                                                                         ┌──────────────────┐
                                                                                                         │   IPC / Monitor   │
                                                                                                         │  TCP :7878, JSON  │
                                                                                                         │  lines + KILL cmd │
                                                                                                         └────────┬──────────┘
                                                                                                                  │ TCP
                                                                                                         ┌────────▼──────────┐
                                                                                                         │  Python relay.py   │
                                                                                                         │  WS :8765           │
                                                                                                         └────────┬──────────┘
                                                                                                                  │ WebSocket
                                                                                                         ┌────────▼──────────┐
                                                                                                         │ dashboard.html      │
                                                                                                         │ (price ladder, PnL, │
                                                                                                         │  risk, kill button) │
                                                                                                         └─────────────────────┘
```

Four threads in the Rust engine, each doing one job:

- **Matching engine** (`market_sim.rs`) — owns the one ground-truth
  `OrderBook`. Evolves a synthetic fundamental price (arithmetic Brownian
  motion), maintains background liquidity from imaginary other participants,
  fires noise-trader market orders, and applies our own strategy's quotes.
  Every mutation — background or ours — goes through
  `OrderBook::submit_limit_order`, which checks whether the incoming order
  *crosses* the book first and matches it immediately if so, exactly like a
  real exchange treats a marketable limit order. It never just lets two
  orders rest at overlapping prices.
- **Feed handler** (`udp_feed.rs`) — receives the UDP feed, tracks sequence
  numbers, and counts gaps (dropped/reordered datagrams) — the same job a
  real feed handler does against an exchange's multicast feed.
- **Strategy** (`main.rs`) — reconstructs its *own* order book purely by
  replaying the Level-3 event stream it receives (`OrderBook` is reused here
  in "replay" mode via `insert_resting_order`, never re-deriving matches).
  It never touches the matching engine's book directly — this is exactly how
  a real strategy engine is isolated from the exchange's internal state.
  Runs Avellaneda-Stoikov quoting, updates a real-time volatility estimate,
  computes VaR/ES, and evaluates the kill switch every ~10ms.
- **IPC/monitor** (`ipc.rs`) — a plain blocking TCP server, deliberately
  *not* async and deliberately isolated on its own thread. This is the
  monitoring sidecar; production systems keep this off the hot path so a
  JSON serialization call or a socket write is never sitting between the
  strategy and its next quoting decision.

### The lock-free ring buffer (`ringbuffer.rs`)

A hand-rolled single-producer/single-consumer ring buffer — the core idea
behind the LMAX Disruptor: a pre-allocated fixed array, cache-line-padded
atomic head/tail counters (to avoid false sharing between the producer and
consumer), and the minimum memory ordering that's actually correct
(`Acquire`/`Release`, not `SeqCst` — there's only one producer-consumer edge
here, so the stronger total-order guarantee `SeqCst` buys isn't needed).
`T: Copy` is enforced at the type level: hot-path messages are plain
fixed-size data, so push/pop never touches the allocator. Verified with a
concurrent stress test (100k messages, producer and consumer on separate
threads, checked for strict FIFO order and completeness) in addition to
single-threaded unit tests.

### Order-book reconciliation: the interesting bug

The most instructive bug in this project wasn't in the matching logic or the
concurrency primitive — it was in how the strategy's replayed book recovers
from lost UDP packets. Three iterations, in order:

1. **Blind periodic snapshot overwrite** (every ~1s): silently *wrong*. At
   ~1-2k events/sec, a resync snapshot is stale within milliseconds, so
   applying one unconditionally discards far more legitimately-applied
   recent state than the rare ghost order it's meant to clean up — a worse
   trade than the problem it's solving.
2. **Gap-triggered only** (resync only when the feed handler detects new
   loss): better, but the snapshot-publish rate (100ms) was slower than the
   strategy's own drain rate (~10ms), so by the time a resync was checked,
   the strategy had almost always already drained *past* that snapshot's
   point in time — the safety check correctly refused to apply it, so it
   never fired in practice.
3. **Gap-triggered *and* sequence-gated, with the matching engine publishing
   a fresh snapshot on every loop iteration (~5ms):** every `MarketEvent`
   is tagged with the matching engine's outbound sequence number as it flows
   through the ring buffer (`SeqEvent`); a resync is only ever applied if
   its `seq_at_snapshot` is strictly newer than the highest sequence the
   strategy has already incorporated. This is what makes it safe to apply
   *and* actually able to fire when needed.

Verified by asserting the ground-truth book is never crossed
(`MM_ASSERT_INVARIANTS=1`, 0 violations over a 60s run) and then checking the
shadow book's *reported* top-of-book against that same invariant from the
outside — which is how the first two flawed designs were actually caught.

### Latency benchmark

`./mm_engine --bench <n>` runs the ring buffer in isolation (no UDP, no
simulation, no OS-scheduled sleeps) and reports two numbers, not one, and the
distinction between them is itself worth understanding:

```
cross-thread publish -> consume latency (producer thread A, consumer thread B):
  min:      175398 ns   mean:  283030 ns   p50:  281691 ns
  p99:      364331 ns   p99.9: 425382 ns   max:  712725 ns

single-threaded push+pop round-trip cost (200000 iterations, no second thread involved):
  min:          27 ns   mean:      32 ns   p50:      31 ns
  p99:          34 ns   p99.9:     48 ns   max:   22365 ns
```

(Real output, this sandbox, 5,000,000 messages, ~14.5M msgs/sec throughput.)

The two numbers tell different stories. The single-threaded round-trip
(push immediately followed by pop, same thread, no handoff) isolates the
ring buffer's own mechanical cost — an atomic load, an atomic store, a
small fixed-size copy — and it's genuinely nanosecond-scale, as a lock-free
SPSC queue should be. The cross-thread number is ~9,000x higher, and the
benchmark's own output tells you why: `nproc` in this sandbox returns **1**.
With only one core, two threads that both want to run are fundamentally
taking turns, not running in parallel, and the "latency" mostly measures how
long thread B waits for the OS scheduler to deschedule thread A — not the
ring buffer. The benchmark detects this (`std::thread::available_parallelism()`)
and switches from spin-waiting to yielding accordingly, because spinning
against yourself on a single core is actively counterproductive, not just
imprecise. On genuine multi-core hardware, expect the cross-thread number to
converge toward the single-threaded one (still not implementation-specific
HFT-grade colocated-hardware numbers — see
[Limitations](#limitations--what-a-real-system-would-add) — but a real
measurement of real parallelism instead of an artifact of contending for
one CPU).

## The quoting model

Avellaneda & Stoikov (2008), *"High-frequency trading in a limit order
book"*:

```
reservation price:  r(s, t) = s − q·γ·σ²·(T−t)
optimal spread:      δ_a + δ_b = γ·σ²·(T−t) + (2/γ)·ln(1 + γ/κ)
```

where `s` = mid price, `q` = signed inventory, `γ` = risk aversion, `σ²` =
price variance per unit time, `(T−t)` = time remaining in the horizon, and
`κ` parameterizes the assumed decay of fill-arrival intensity with distance
from mid. Implemented exactly as published, in `quoting.rs`.

Two honest adaptations:

- **γ and κ are calibrated constants**, not fit from historical fill data. A
  real desk estimates κ by regressing log(fill rate) against quoted distance
  from the mid. **Getting this calibration right matters more than it
  sounds**: κ=1.5 (a value that looks reasonable in isolation) produced a
  65-tick half-spread on this $0.01-tick instrument — the bot was never
  competitive with the background book and essentially never traded.
  Recalibrating to κ=100 (implying fill intensity decays meaningfully over
  about one tick, appropriate for a tightly-quoted synthetic instrument)
  brought the half-spread to ~1.2 ticks and produced real, healthy trading
  activity.
- **Rolling horizon.** The original paper assumes a single trading session
  with a hard terminal time. Crypto doesn't have a close of trading, so
  `(T−t)` here resets every `HORIZON_SECS` — a common practical adaptation
  for continuous markets.

Volatility (`σ`) is a live EWMA of realized variance from the strategy's own
mid-price series (`quoting::VolEstimator`) — not a fixed assumption, and
deliberately estimated from the strategy's *own observed* book, not from the
simulator's "true" underlying process (a real strategy has no access to
that).

## Risk management

`risk.rs` computes parametric VaR and Expected Shortfall on the open
inventory every strategy tick:

```
VaR_95  = z_0.95 · |q| · σ · √h
ES_95   = φ(z_0.95)/0.05 · |q| · σ · √h
```

The `KillSwitch` latches (doesn't silently un-trip) on the first breach of
inventory, drawdown, or VaR limits, or a manual trigger — and is exercised
from both directions: automatically when the risk state say so, and manually
from the dashboard's kill button, which is relayed as a plain `KILL\n` line
over the same TCP connection the snapshots stream over.

## Running it

**Requires:** Rust/cargo (`apt install rustc cargo` if not already present),
Python 3 with `pip install -r monitor/requirements.txt`.

```bash
# 1. build
cd engine && cargo build --release

# 2. run the engine (optionally: --duration <secs> to auto-stop)
./target/release/mm_engine

# 3. in another terminal, start the relay
cd ../monitor && python3 relay.py

# 4. open monitor/dashboard.html directly in a browser (File > Open, or
#    double-click) -- it connects to ws://127.0.0.1:8765 automatically
```

Or use `./run.sh` from the project root to start the engine and relay
together.

`cargo test` runs 25 unit tests, including the concurrent ring-buffer stress
test and the order-book matching/crossing tests. Set `MM_ASSERT_INVARIANTS=1`
when running the engine to enable a permanent, cheap runtime check that the
ground-truth book is never crossed. Run `./target/release/mm_engine --bench
1000000` for the ring-buffer latency/throughput benchmark described above.

## Limitations — what a real system would add

This is a portfolio-grade simulation, not a production HFT system, and the
gap between them is real and worth naming directly:

- **No live venue.** No colocation, no kernel-bypass NIC (Solarflare
  OpenOnload, DPDK), no FPGA tick-to-trade path, no exchange membership, and
  no real L3 data feed (which would come from the exchange directly or a
  paid vendor). This project's UDP layer *demonstrates the pattern*, not
  wire-speed performance — it's JSON over loopback UDP, not a binary
  encoding like SBE.
- **The benchmark measures this sandbox, not tuned hardware — and this
  sandbox turned out to have exactly one CPU core.** That's not a caveat
  buried in fine print; it's the actual reason the cross-thread latency
  number is microseconds instead of nanoseconds (see the benchmark section
  above for the full explanation and the single-threaded number that isolates
  the ring buffer's real cost from that artifact). On real multi-core,
  colocated, tuned hardware, expect the cross-thread number to approach the
  single-threaded one, not match some external "HFT-grade" figure — that
  additionally requires kernel bypass, CPU pinning/isolation, and NIC-level
  timestamping this project doesn't attempt.
- **Simplified fill model.** Background liquidity and noise-trader flow are
  calibrated stochastic processes, not a queue-position model. Real LOB
  queue-position modeling — estimating your probability of fill *given where
  in the queue at a price level you are*, not just whether you're at the
  best price — is a genuinely hard, still-researched problem on its own.
- **No transaction costs, fees, or multi-instrument/cross-asset risk.**
- **No historical backtest harness.** This validates the strategy's
  *mechanics* (the quotes react correctly to inventory, volatility, and
  time; risk limits and the kill switch fire correctly) via unit tests and
  live stability runs, not its expected edge against real historical data.
- **Order-book reconciliation is "good enough for a demo," not exchange-grade.**
  The sequence-gated resync (above) fixes the systemic version of this
  problem; a genuinely brief (bounded to roughly one resync-check interval,
  ~20ms) reconciliation window can still occasionally show a transient
  crossed state in the shadow book immediately after a dropped packet,
  self-correcting on the next check. A production book-builder would use a
  proper "request snapshot at sequence N, buffer live events, splice them
  together" recovery protocol instead of a periodic Mutex-guarded snapshot.

## Project layout

```
engine/           Rust workspace
  src/
    types.rs       shared types: Side, Order, Fill, MarketEvent (L3), SeqEvent, StrategyCommand
    orderbook.rs    price-time-priority book + matching (ground truth AND shadow-book replay)
    ringbuffer.rs   lock-free SPSC ring buffer (Disruptor pattern)
    bench.rs        isolated latency/throughput benchmark for the ring buffer
    market_sim.rs   matching engine thread: price process, background liquidity, UDP feed sender
    udp_feed.rs     feed handler thread: UDP receiver, sequence-gap detection
    quoting.rs      Avellaneda-Stoikov model + real-time volatility estimator
    risk.rs         VaR/ES + kill switch
    ipc.rs          TCP monitoring server (JSON snapshots + KILL command)
    main.rs         orchestration + strategy thread (L3 replay, quoting, risk loop)
monitor/
  relay.py          WebSocket bridge: engine's TCP stream <-> browser
  dashboard.html    live price ladder, PnL chart, risk panel, kill switch
  requirements.txt
run.sh              convenience script: builds (if needed) and starts engine + relay
```
