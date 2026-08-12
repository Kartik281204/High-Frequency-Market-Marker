// The "exchange" thread. It owns the single ground-truth order book, evolves
// a synthetic fundamental price via a simple stochastic process, keeps the
// book populated with plausible background liquidity from imaginary other
// participants, fires random "noise trader" market orders against it, and
// applies our own strategy's quotes -- while publishing every individual
// mutation as an L3 MarketEvent over UDP (as a real exchange would publish a
// multicast feed) and draining inbound StrategyCommands from the strategy
// thread over an in-process queue (representing the separate order-entry
// path).
//
// Note on realism: noise-trader market orders are matched through the same
// `execute_market_order` price-time-priority logic as everything else, so
// whichever side currently has the best price gets hit -- including our own
// quote, whenever we're tighter than the background crowd. That's what
// produces "quote tighter, get filled more often" as an emergent property of
// a fuller book simulation, rather than hand-coding the Avellaneda-Stoikov
// paper's assumed arrival-intensity formula directly onto our own fills. The
// A-S formula still supplies the *quoting policy*; here we're evaluating
// that policy in a more general simulated market than the one its derivation
// assumes, which is itself a useful, honest thing to see.

use crate::orderbook::OrderBook;
use crate::ringbuffer::Consumer;
use crate::types::{now_nanos, BookResync, FeedPacket, MarketEvent, Price, Qty, Side, StrategyCommand};
use rand::Rng;
use rand_distr::{Distribution, Normal};
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

pub struct MarketSimConfig {
    pub start_price: f64,
    pub tick_size: f64,
    pub true_sigma_per_sqrt_sec: f64,
    pub step_interval: Duration,
    pub background_levels: usize,
    pub background_level_qty: Qty,
    pub background_refresh_prob: f64,
    pub noise_order_prob_per_step: f64,
    pub noise_order_qty: Qty,
    pub feed_addr: String,
    /// Fraction of outbound feed packets intentionally NOT sent, purely to
    /// give the feed handler's gap-detection logic something real to catch
    /// and prove it works -- a real exchange doesn't do this on purpose,
    /// packet loss there comes from actual network conditions.
    pub artificial_drop_prob: f64,
}

impl Default for MarketSimConfig {
    fn default() -> Self {
        Self {
            start_price: 100.00,
            tick_size: 0.01,
            true_sigma_per_sqrt_sec: 0.06,
            step_interval: Duration::from_millis(4),
            background_levels: 5,
            background_level_qty: 20,
            background_refresh_prob: 0.35,
            noise_order_prob_per_step: 0.45,
            noise_order_qty: 3,
            feed_addr: "127.0.0.1:9001".to_string(),
            artificial_drop_prob: 0.0005,
        }
    }
}

fn to_ticks(price: f64, tick_size: f64) -> Price {
    (price / tick_size).round() as Price
}

struct BackgroundSlots {
    bid: Vec<Option<(u64, Price)>>,
    ask: Vec<Option<(u64, Price)>>,
}

struct FeedSender {
    socket: UdpSocket,
    target: String,
    seq: u64,
    drop_prob: f64,
}

impl FeedSender {
    fn emit(&mut self, event: MarketEvent, rng: &mut impl Rng) {
        let packet = FeedPacket { seq: self.seq, event };
        self.seq += 1;
        if rng.gen::<f64>() < self.drop_prob {
            return; // simulated packet loss -- seq still advances, so the gap is real
        }
        if let Ok(bytes) = serde_json::to_vec(&packet) {
            let _ = self.socket.send_to(&bytes, &self.target);
        }
    }

    /// The sequence number that will be assigned to the *next* emitted
    /// event, i.e. "how many events have been emitted so far". Used to tag
    /// resync snapshots so the strategy can tell whether one is actually
    /// newer than what it has already incorporated.
    fn next_seq(&self) -> u64 {
        self.seq
    }
}

fn fill_to_trade_event(fill: &crate::types::Fill) -> MarketEvent {
    MarketEvent::Trade {
        resting_order_id: fill.resting_order_id,
        resting_is_ours: fill.resting_is_ours,
        side_of_resting_order: fill.side_of_resting_order,
        price: fill.price,
        qty: fill.qty,
        ts_nanos: fill.ts_nanos,
    }
}

pub fn run(
    cfg: MarketSimConfig,
    cmd_rx: Consumer<StrategyCommand>,
    resync: Arc<Mutex<BookResync>>,
    shutdown: Arc<AtomicBool>,
) {
    let socket = UdpSocket::bind("127.0.0.1:0").expect("failed to bind UDP sender socket");
    let mut feed = FeedSender {
        socket,
        target: cfg.feed_addr.clone(),
        seq: 0,
        drop_prob: cfg.artificial_drop_prob,
    };

    let mut book = OrderBook::new();
    let mut id_gen: u64 = 0;
    let mut rng = rand::thread_rng();
    let step_secs = cfg.step_interval.as_secs_f64();
    let normal = Normal::new(0.0f64, 1.0f64).unwrap();

    let mut mid: f64 = cfg.start_price;
    let mut slots = BackgroundSlots {
        bid: vec![None; cfg.background_levels],
        ask: vec![None; cfg.background_levels],
    };

    let mut our_bid_id: Option<u64> = None;
    let mut our_ask_id: Option<u64> = None;
    let mut our_bid_price: Price = 0;
    let mut our_ask_price: Price = 0;

    for lvl in 0..cfg.background_levels {
        place_background(&mut book, &mut slots, Side::Bid, lvl, mid, &cfg, &mut id_gen, &mut feed, &mut rng);
        place_background(&mut book, &mut slots, Side::Ask, lvl, mid, &cfg, &mut id_gen, &mut feed, &mut rng);
    }
    *resync.lock().unwrap() = BookResync {
        seq_at_snapshot: feed.next_seq(),
        ..book.export_resync()
    };
    let mut last_resync_publish = Instant::now();
    let resync_interval = Duration::from_millis(5);

    while !shutdown.load(Ordering::Relaxed) {
        // 1. evolve the synthetic fundamental price (arithmetic Brownian motion)
        let dw = normal.sample(&mut rng);
        mid += cfg.true_sigma_per_sqrt_sec * step_secs.sqrt() * dw;
        if mid < cfg.tick_size * 10.0 {
            mid = cfg.tick_size * 10.0; // guard against a pathological random walk
        }

        // 2. background liquidity: each level independently may refresh,
        //    modelling other participants continuously re-quoting
        for lvl in 0..cfg.background_levels {
            if rng.gen::<f64>() < cfg.background_refresh_prob {
                place_background(&mut book, &mut slots, Side::Bid, lvl, mid, &cfg, &mut id_gen, &mut feed, &mut rng);
            }
            if rng.gen::<f64>() < cfg.background_refresh_prob {
                place_background(&mut book, &mut slots, Side::Ask, lvl, mid, &cfg, &mut id_gen, &mut feed, &mut rng);
            }
        }

        // 3. apply pending commands from the strategy thread
        while let Some(cmd) = cmd_rx.try_pop() {
            match cmd {
                StrategyCommand::Quote { bid_price, bid_qty, ask_price, ask_qty } => {
                    if let Some(id) = our_bid_id.take() {
                        if book.cancel_order(Side::Bid, our_bid_price, id) {
                            feed.emit(MarketEvent::Cancel { order_id: id, side: Side::Bid, price: our_bid_price, ts_nanos: now_nanos() }, &mut rng);
                        }
                    }
                    if let Some(id) = our_ask_id.take() {
                        if book.cancel_order(Side::Ask, our_ask_price, id) {
                            feed.emit(MarketEvent::Cancel { order_id: id, side: Side::Ask, price: our_ask_price, ts_nanos: now_nanos() }, &mut rng);
                        }
                    }

                    id_gen += 1;
                    let new_bid_id = id_gen;
                    let bid_fills = book.submit_limit_order(new_bid_id, Side::Bid, bid_price, bid_qty, now_nanos(), true);
                    let bid_filled: u64 = bid_fills.iter().map(|f| f.qty).sum();
                    for f in &bid_fills {
                        feed.emit(fill_to_trade_event(f), &mut rng);
                    }
                    if bid_filled < bid_qty {
                        let resting_qty = bid_qty - bid_filled;
                        feed.emit(MarketEvent::Add { order_id: new_bid_id, side: Side::Bid, price: bid_price, qty: resting_qty, ts_nanos: now_nanos(), is_ours: true }, &mut rng);
                        our_bid_id = Some(new_bid_id);
                        our_bid_price = bid_price;
                    }

                    // Guard against our own two orders crossing each other:
                    // if the quoted spread has collapsed to zero after tick
                    // rounding, nudge the ask up rather than let it match
                    // against the bid we just placed (a self-trade a real
                    // exchange's self-trade prevention would also block).
                    let safe_ask_price = ask_price.max(bid_price + 1);

                    id_gen += 1;
                    let new_ask_id = id_gen;
                    let ask_fills = book.submit_limit_order(new_ask_id, Side::Ask, safe_ask_price, ask_qty, now_nanos(), true);
                    let ask_filled: u64 = ask_fills.iter().map(|f| f.qty).sum();
                    for f in &ask_fills {
                        feed.emit(fill_to_trade_event(f), &mut rng);
                    }
                    if ask_filled < ask_qty {
                        let resting_qty = ask_qty - ask_filled;
                        feed.emit(MarketEvent::Add { order_id: new_ask_id, side: Side::Ask, price: safe_ask_price, qty: resting_qty, ts_nanos: now_nanos(), is_ours: true }, &mut rng);
                        our_ask_id = Some(new_ask_id);
                        our_ask_price = safe_ask_price;
                    }
                }
                StrategyCommand::CancelAll => {
                    if let Some(id) = our_bid_id.take() {
                        if book.cancel_order(Side::Bid, our_bid_price, id) {
                            feed.emit(MarketEvent::Cancel { order_id: id, side: Side::Bid, price: our_bid_price, ts_nanos: now_nanos() }, &mut rng);
                        }
                    }
                    if let Some(id) = our_ask_id.take() {
                        if book.cancel_order(Side::Ask, our_ask_price, id) {
                            feed.emit(MarketEvent::Cancel { order_id: id, side: Side::Ask, price: our_ask_price, ts_nanos: now_nanos() }, &mut rng);
                        }
                    }
                }
            }
        }

        // 4. noise-trader market orders hit whichever side is currently best
        if rng.gen::<f64>() < cfg.noise_order_prob_per_step {
            let side = if rng.gen::<bool>() { Side::Bid } else { Side::Ask };
            let fills = book.execute_market_order(side, cfg.noise_order_qty, now_nanos());
            for fill in &fills {
                feed.emit(fill_to_trade_event(fill), &mut rng);
            }
        }

        // 5. periodically publish an authoritative full-book snapshot so the
        //    strategy's shadow book can resync and discard any drift from
        //    lost feed packets, instead of letting it silently compound
        if last_resync_publish.elapsed() >= resync_interval {
            *resync.lock().unwrap() = BookResync {
                seq_at_snapshot: feed.next_seq(),
                ..book.export_resync()
            };
            last_resync_publish = Instant::now();
        }

        // Optional runtime sanity check, off by default: the ground-truth
        // book must never be crossed (best_bid >= best_ask) -- if this ever
        // fires, `submit_limit_order`'s crossing logic has a bug. Left in as
        // a permanent, cheap invariant check rather than debugging scaffolding.
        if std::env::var("MM_ASSERT_INVARIANTS").is_ok() {
            if let (Some(bb), Some(ba)) = (book.best_bid(), book.best_ask()) {
                debug_assert!(bb < ba, "ground truth book crossed: best_bid={bb} best_ask={ba}");
                if bb >= ba {
                    eprintln!("[INVARIANT VIOLATION] ground truth crossed: best_bid={bb} best_ask={ba}");
                }
            }
        }

        thread::sleep(cfg.step_interval);
    }
}

#[allow(clippy::too_many_arguments)]
fn place_background(
    book: &mut OrderBook,
    slots: &mut BackgroundSlots,
    side: Side,
    lvl_idx: usize,
    mid: f64,
    cfg: &MarketSimConfig,
    id_gen: &mut u64,
    feed: &mut FeedSender,
    rng: &mut impl Rng,
) {
    let existing = match side {
        Side::Bid => slots.bid[lvl_idx].take(),
        Side::Ask => slots.ask[lvl_idx].take(),
    };
    if let Some((old_id, old_price)) = existing {
        if book.cancel_order(side, old_price, old_id) {
            feed.emit(MarketEvent::Cancel { order_id: old_id, side, price: old_price, ts_nanos: now_nanos() }, rng);
        }
    }
    let mid_ticks = to_ticks(mid, cfg.tick_size);
    let depth = (lvl_idx as Price) + 1;
    let price = match side {
        Side::Bid => mid_ticks - depth,
        Side::Ask => mid_ticks + depth,
    };
    *id_gen += 1;
    let id = *id_gen;
    let fills = book.submit_limit_order(id, side, price, cfg.background_level_qty, now_nanos(), false);
    let filled: Qty = fills.iter().map(|f| f.qty).sum();
    for f in &fills {
        feed.emit(fill_to_trade_event(f), rng);
    }
    if filled < cfg.background_level_qty {
        let resting_qty = cfg.background_level_qty - filled;
        feed.emit(MarketEvent::Add { order_id: id, side, price, qty: resting_qty, ts_nanos: now_nanos(), is_ours: false }, rng);
        match side {
            Side::Bid => slots.bid[lvl_idx] = Some((id, price)),
            Side::Ask => slots.ask[lvl_idx] = Some((id, price)),
        }
    }
    // if fully filled immediately, the slot is intentionally left as None --
    // the next refresh cycle will place a fresh order there
}
