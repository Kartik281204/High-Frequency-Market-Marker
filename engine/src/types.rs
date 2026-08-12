// Shared types used across every thread in the engine.
//
// Price is represented in integer ticks, not floats. This is deliberate:
// real matching engines never compare floating point prices for equality or
// ordering (rounding makes that a real source of bugs), so BTreeMap<Price, _>
// keys are integers here and we convert to dollars only at the boundary
// (when reporting to the strategy's risk/PnL layer or the monitoring UI).

use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

pub type Price = i64;
pub type Qty = u64;
pub type OrderId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Bid,
    Ask,
}

impl Side {
    pub fn opposite(self) -> Side {
        match self {
            Side::Bid => Side::Ask,
            Side::Ask => Side::Bid,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Order {
    pub id: OrderId,
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
    pub ts_nanos: u64,
    pub is_ours: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct Fill {
    pub resting_order_id: OrderId,
    pub resting_is_ours: bool,
    pub side_of_resting_order: Side,
    pub price: Price,
    pub qty: Qty,
    pub ts_nanos: u64,
}

/// Genuine Level-3 (order-by-order) market data: every individual add,
/// cancel, and trade -- not aggregated price levels (L2) and not just
/// top-of-book (L1). This is exactly the granularity a real feed handler
/// parses off an exchange's ITCH/FIX-style multicast feed. The strategy
/// thread never sees the matching engine's book directly; it only ever sees
/// this stream, over UDP, through a feed handler, through a lock-free queue.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum MarketEvent {
    Add {
        order_id: OrderId,
        side: Side,
        price: Price,
        qty: Qty,
        ts_nanos: u64,
        is_ours: bool,
    },
    Cancel {
        order_id: OrderId,
        side: Side,
        price: Price,
        ts_nanos: u64,
    },
    Trade {
        resting_order_id: OrderId,
        resting_is_ours: bool,
        side_of_resting_order: Side,
        price: Price,
        qty: Qty,
        ts_nanos: u64,
    },
}

/// A UDP wire packet: a sequence number plus one event. The feed handler
/// uses `seq` to detect gaps (dropped or reordered datagrams) the same way a
/// real ITCH/multicast feed handler detects gaps and requests a snapshot
/// recovery.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct FeedPacket {
    pub seq: u64,
    pub event: MarketEvent,
}

/// Outbound order-entry style commands, strategy -> matching engine. This
/// mirrors a simplified FIX/OUCH "New Order" / "Cancel" message and travels
/// over its own in-process lock-free queue (order entry and market data are
/// different protocols with different guarantees in real venues too).
#[derive(Debug, Clone, Copy)]
pub enum StrategyCommand {
    Quote {
        bid_price: Price,
        bid_qty: Qty,
        ask_price: Price,
        ask_qty: Qty,
    },
    CancelAll,
}

/// A full authoritative resync of the book: every currently-resting order,
/// straight from the matching engine's ground truth. Published periodically
/// on a side channel (an occasional Mutex lock is fine here -- this is a
/// cold, infrequent operation, not the hot path) so the strategy's shadow
/// book can discard any drift accumulated from lost feed packets instead of
/// letting it silently compound forever. This is a simplified stand-in for
/// what a real feed handler's "request snapshot, replay from sequence N"
/// recovery flow does.
///
/// `seq_at_snapshot` is the matching engine's outbound sequence counter at
/// the moment the snapshot was captured. Without this, a resync is actually
/// dangerous, not just occasionally wasteful: applying a snapshot that is
/// older than events the shadow book has *already processed* silently rolls
/// back legitimate state, which is worse than the drift it's meant to fix.
/// The strategy only applies a resync when its sequence is newer than the
/// highest sequence it has already incorporated.
#[derive(Debug, Clone, Default)]
pub struct BookResync {
    pub bids: Vec<(OrderId, Price, Qty, bool)>,
    pub asks: Vec<(OrderId, Price, Qty, bool)>,
    pub seq_at_snapshot: u64,
}

/// A MarketEvent tagged with the matching engine's outbound sequence number,
/// carried through the ring buffer so the strategy can track how much of the
/// feed it has actually incorporated (used to gate resync application; see
/// `BookResync`).
#[derive(Debug, Clone, Copy)]
pub struct SeqEvent {
    pub seq: u64,
    pub event: MarketEvent,
}

pub fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}
