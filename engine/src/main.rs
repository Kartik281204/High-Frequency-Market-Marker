mod ipc;
mod market_sim;
mod orderbook;
mod quoting;
mod ringbuffer;
mod risk;
mod types;
mod udp_feed;

use market_sim::MarketSimConfig;
use orderbook::OrderBook;
use quoting::{AvellanedaStoikov, VolEstimator};
use risk::{compute_risk, KillSwitch, RiskLimits};
use types::{now_nanos, BookResync, MarketEvent, Price, SeqEvent, Side, StrategyCommand};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const TICK_SIZE: f64 = 0.01;
const OUR_LOT_SIZE: u64 = 5;
const HORIZON_SECS: f64 = 45.0; // rolling/receding horizon -- see quoting.rs
const GAMMA: f64 = 0.1; // risk aversion
const KAPPA: f64 = 100.0; // assumed order-flow intensity decay, calibrated for a $0.01 tick size
const VOL_EWMA_LAMBDA: f64 = 0.97;
const FEED_ADDR: &str = "127.0.0.1:9001";
const MONITOR_ADDR: &str = "127.0.0.1:7878";

fn price_to_ticks(p: f64) -> Price {
    (p / TICK_SIZE).round() as Price
}
fn ticks_to_price(t: Price) -> f64 {
    t as f64 * TICK_SIZE
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let duration_secs: Option<u64> = args
        .iter()
        .position(|a| a == "--duration")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok());

    let shutdown = Arc::new(AtomicBool::new(false));
    let manual_kill = Arc::new(AtomicBool::new(false));
    let snapshot: Arc<Mutex<ipc::Snapshot>> = Arc::new(Mutex::new(ipc::Snapshot::default()));
    let feed_health = Arc::new(udp_feed::FeedHealth::new());
    let resync: Arc<Mutex<BookResync>> = Arc::new(Mutex::new(BookResync::default()));

    // Feed-handler -> strategy: genuine L3 events, decoupled via a lock-free queue.
    let (md_tx, md_rx) = ringbuffer::channel::<SeqEvent>(16384);
    // Strategy -> matching engine: outbound order-entry style commands.
    let (cmd_tx, cmd_rx) = ringbuffer::channel::<StrategyCommand>(1024);

    println!("=== market-maker engine starting ===");
    println!("feed:      matching-engine --UDP--> {FEED_ADDR} --ring buffer--> strategy");
    println!("orders:    strategy --ring buffer--> matching-engine");
    println!("monitor:   TCP {MONITOR_ADDR} (newline-delimited JSON snapshots + KILL command)");

    let sim_cfg = MarketSimConfig {
        feed_addr: FEED_ADDR.to_string(),
        ..MarketSimConfig::default()
    };
    let sim_shutdown = shutdown.clone();
    let sim_resync = resync.clone();
    let sim_handle = thread::spawn(move || {
        market_sim::run(sim_cfg, cmd_rx, sim_resync, sim_shutdown);
    });

    let feed_shutdown = shutdown.clone();
    let feed_health_h = feed_health.clone();
    let feed_addr = FEED_ADDR.to_string();
    let feed_handle = thread::spawn(move || {
        udp_feed::run(&feed_addr, md_tx, feed_health_h, feed_shutdown);
    });

    let strat_shutdown = shutdown.clone();
    let strat_manual_kill = manual_kill.clone();
    let strat_snapshot = snapshot.clone();
    let strat_feed_health = feed_health.clone();
    let strat_resync = resync.clone();
    let strat_handle = thread::spawn(move || {
        run_strategy(md_rx, cmd_tx, strat_shutdown, strat_manual_kill, strat_snapshot, strat_feed_health, strat_resync);
    });

    let ipc_shutdown = shutdown.clone();
    let ipc_snapshot = snapshot.clone();
    let ipc_kill = manual_kill.clone();
    let ipc_handle = thread::spawn(move || {
        ipc::run_server(MONITOR_ADDR, ipc_snapshot, ipc_kill, ipc_shutdown, Duration::from_millis(50));
    });

    if let Some(secs) = duration_secs {
        thread::sleep(Duration::from_secs(secs));
        println!("=== --duration elapsed, shutting down ===");
        shutdown.store(true, Ordering::Relaxed);
    }

    sim_handle.join().ok();
    feed_handle.join().ok();
    strat_handle.join().ok();
    ipc_handle.join().ok();
}

#[allow(clippy::too_many_arguments)]
fn run_strategy(
    event_rx: ringbuffer::Consumer<SeqEvent>,
    cmd_tx: ringbuffer::Producer<StrategyCommand>,
    shutdown: Arc<AtomicBool>,
    manual_kill: Arc<AtomicBool>,
    snapshot: Arc<Mutex<ipc::Snapshot>>,
    feed_health: Arc<udp_feed::FeedHealth>,
    resync: Arc<Mutex<BookResync>>,
) {
    // The strategy's view of the world is built *entirely* by replaying the
    // L3 feed below. It never touches the matching engine's book directly.
    let mut shadow_book = OrderBook::new();
    let mut inventory: i64 = 0;
    let mut cash: f64 = 0.0;
    let mut trades_count: u64 = 0;

    let mut vol_est = VolEstimator::new(0.05, VOL_EWMA_LAMBDA);
    let model = AvellanedaStoikov { gamma: GAMMA, kappa: KAPPA };
    let mut kill_switch = KillSwitch::new(RiskLimits {
        max_abs_inventory: 60,
        max_drawdown: 8.0,
        max_var_95: 4.0,
    });

    let start = Instant::now();
    let mut our_bid_price: Price = 0;
    let mut our_ask_price: Price = 0;
    let mut have_quotes = false;
    let mut last_mid: f64 = 100.0;
    let mut last_resync_pull = Instant::now();
    let resync_pull_interval = Duration::from_millis(20);
    let mut last_known_lost_events: u64 = 0;
    let mut highest_seq_applied: u64 = 0;

    while !shutdown.load(Ordering::Relaxed) {
        // Drain everything currently available on the L3 feed and replay it
        // into the shadow book -- exactly what a real strategy engine does.
        while let Some(seq_event) = event_rx.try_pop() {
            highest_seq_applied = highest_seq_applied.max(seq_event.seq);
            match seq_event.event {
                MarketEvent::Add { order_id, side, price, qty, ts_nanos, is_ours } => {
                    shadow_book.insert_resting_order(order_id, side, price, qty, ts_nanos, is_ours);
                    if is_ours {
                        match side {
                            Side::Bid => our_bid_price = price,
                            Side::Ask => our_ask_price = price,
                        }
                        have_quotes = true;
                    }
                }
                MarketEvent::Cancel { order_id, side, price, .. } => {
                    shadow_book.cancel_order(side, price, order_id);
                }
                MarketEvent::Trade { resting_order_id, resting_is_ours, side_of_resting_order, price, qty, .. } => {
                    shadow_book.apply_trade(side_of_resting_order, price, resting_order_id, qty);
                    if resting_is_ours {
                        trades_count += 1;
                        let fill_price = ticks_to_price(price);
                        match side_of_resting_order {
                            Side::Bid => {
                                inventory += qty as i64; // our bid was hit: we bought
                                cash -= qty as f64 * fill_price;
                            }
                            Side::Ask => {
                                inventory -= qty as i64; // our ask was lifted: we sold
                                cash += qty as f64 * fill_price;
                            }
                        }
                    }
                }
            }
        }

        // Only pay the cost of a full resync when the feed handler has
        // actually detected new loss since we last checked (a cheap
        // pre-filter -- most of the time there's nothing to fix). The
        // sequence check just below is the actual safety gate: a snapshot is
        // only ever applied if it is strictly newer than everything the
        // shadow book has already incorporated. Without that check, this
        // mechanism would be actively harmful -- at ~1-2k events/sec, any
        // snapshot is stale within milliseconds, and blindly applying one
        // would silently roll back legitimately-applied recent state (which
        // is exactly what produced transient crossed-book artifacts before
        // this check was added).
        if last_resync_pull.elapsed() >= resync_pull_interval {
            let current_lost = feed_health.events_lost_to_gaps.load(Ordering::Relaxed);
            if current_lost > last_known_lost_events {
                let snap = resync.lock().unwrap().clone();
                let is_actually_newer = snap.seq_at_snapshot > highest_seq_applied;
                if is_actually_newer && (!snap.bids.is_empty() || !snap.asks.is_empty()) {
                    shadow_book.rebuild_from_resync(&snap, now_nanos());
                    highest_seq_applied = snap.seq_at_snapshot;
                    if let Some(&(_, p, _, _)) = snap.bids.iter().find(|&&(_, _, _, is_ours)| is_ours) {
                        our_bid_price = p;
                    }
                    if let Some(&(_, p, _, _)) = snap.asks.iter().find(|&&(_, _, _, is_ours)| is_ours) {
                        our_ask_price = p;
                    }
                }
                last_known_lost_events = current_lost;
            }
            last_resync_pull = Instant::now();
        }

        let mid = shadow_book.mid_ticks().map(|t| t * TICK_SIZE).unwrap_or(last_mid);
        last_mid = mid;
        let sigma = vol_est.update(mid, now_nanos());

        let elapsed = start.elapsed().as_secs_f64();
        let time_in_cycle = elapsed % HORIZON_SECS;
        let time_remaining = HORIZON_SECS - time_in_cycle;

        let risk_metrics = compute_risk(inventory, mid, sigma, 1.0, cash);
        let kill = kill_switch.evaluate(inventory, &risk_metrics, manual_kill.load(Ordering::Relaxed));

        let mut snap = ipc::Snapshot {
            ts_ms: (now_nanos() / 1_000_000) as u64,
            mid,
            our_bid: None,
            our_ask: None,
            inventory,
            cash,
            unrealized_pnl: risk_metrics.unrealized_pnl,
            sigma,
            var_95: risk_metrics.var_95,
            es_95: risk_metrics.es_95,
            killed: kill.is_some(),
            kill_reason: kill.map(|k| k.describe().to_string()),
            bid_depth: depth_as_prices(&shadow_book, true),
            ask_depth: depth_as_prices(&shadow_book, false),
            trades_count,
            feed_packets_received: feed_health.packets_received.load(Ordering::Relaxed),
            feed_gaps_detected: feed_health.gaps_detected.load(Ordering::Relaxed),
            feed_events_lost: feed_health.events_lost_to_gaps.load(Ordering::Relaxed),
            ring_dropped: event_rx.dropped_count(),
        };

        if kill.is_some() {
            cmd_tx.try_push(StrategyCommand::CancelAll).ok();
            have_quotes = false;
        } else {
            let quote = model.quote(mid, inventory, sigma, time_remaining);
            let bid_ticks = price_to_ticks(quote.bid);
            let ask_ticks = price_to_ticks(quote.ask);

            let should_requote = !have_quotes || our_bid_price != bid_ticks || our_ask_price != ask_ticks;
            if should_requote {
                cmd_tx
                    .try_push(StrategyCommand::Quote {
                        bid_price: bid_ticks,
                        bid_qty: OUR_LOT_SIZE,
                        ask_price: ask_ticks,
                        ask_qty: OUR_LOT_SIZE,
                    })
                    .ok();
            }
            snap.our_bid = Some(ticks_to_price(bid_ticks));
            snap.our_ask = Some(ticks_to_price(ask_ticks));
        }

        *snapshot.lock().unwrap() = snap;
        thread::sleep(Duration::from_millis(10));
    }
}

fn depth_as_prices(book: &OrderBook, bids: bool) -> Vec<(f64, u64)> {
    let (b, a) = book.depth_snapshot(5);
    if bids {
        b.into_iter().map(|(p, q)| (ticks_to_price(p), q)).collect()
    } else {
        a.into_iter().map(|(p, q)| (ticks_to_price(p), q)).collect()
    }
}
