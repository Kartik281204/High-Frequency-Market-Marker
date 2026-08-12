// A price-time-priority limit order book.
//
// This one struct plays two roles in the project:
//   1. The *ground truth* book inside the matching-engine thread, where every
//      mutation is authoritative and generates a MarketEvent.
//   2. A *shadow* book inside the strategy thread, rebuilt purely by
//      replaying the L3 MarketEvent stream it receives -- exactly how a real
//      strategy engine reconstructs book state from a market data feed,
//      never touching the exchange's true internal state directly.

use crate::types::{BookResync, Fill, Order, OrderId, Price, Qty, Side};
use std::collections::{BTreeMap, VecDeque};

#[derive(Default)]
pub struct OrderBook {
    pub bids: BTreeMap<Price, VecDeque<Order>>, // ascending; best bid = max key
    pub asks: BTreeMap<Price, VecDeque<Order>>, // ascending; best ask = min key
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    pub fn best_bid(&self) -> Option<Price> {
        self.bids.keys().next_back().copied()
    }

    pub fn best_ask(&self) -> Option<Price> {
        self.asks.keys().next().copied()
    }

    pub fn mid_ticks(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(b), Some(a)) => Some((b + a) as f64 / 2.0),
            _ => None,
        }
    }

    fn book_mut(&mut self, side: Side) -> &mut BTreeMap<Price, VecDeque<Order>> {
        match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        }
    }

    /// Unconditionally rests an order in the book, with no crossing check.
    /// This is the right primitive for *replaying* book state exactly as it
    /// was reported (the shadow book reconstructing from an `Add` event, or
    /// rebuilding from a resync snapshot) -- in both cases the crossing
    /// decision was already made by the ground-truth matching engine, and
    /// blindly re-deriving it here would be redundant with the `Trade`
    /// events already in the stream, and could double-count fills.
    pub fn insert_resting_order(
        &mut self,
        id: OrderId,
        side: Side,
        price: Price,
        qty: Qty,
        ts_nanos: u64,
        is_ours: bool,
    ) {
        let order = Order {
            id,
            side,
            price,
            qty,
            ts_nanos,
            is_ours,
        };
        self.book_mut(side)
            .entry(price)
            .or_insert_with(VecDeque::new)
            .push_back(order);
    }

    /// Submits a limit order the way a real exchange does: if it crosses the
    /// opposite side (a bid at or above the best ask, or an ask at or below
    /// the best bid), it is a *marketable* limit order and must match
    /// immediately against the opposite book -- price-time priority, walking
    /// levels only as far as the limit price allows -- before any unfilled
    /// remainder rests. Only the ground-truth book should call this; the
    /// shadow book replays the fills this produces via separate `Trade`
    /// events instead of re-deriving them.
    pub fn submit_limit_order(
        &mut self,
        id: OrderId,
        side: Side,
        price: Price,
        qty: Qty,
        ts_nanos: u64,
        is_ours: bool,
    ) -> Vec<Fill> {
        let mut remaining = qty;
        let mut fills = Vec::new();
        let resting_side = side.opposite();

        loop {
            if remaining == 0 {
                break;
            }
            let book = self.book_mut(resting_side);
            let best = match side {
                Side::Bid => book.keys().next().copied(),     // lowest resting ask
                Side::Ask => book.keys().next_back().copied(), // highest resting bid
            };
            let level_price = match best {
                Some(p) => p,
                None => break,
            };
            let crosses = match side {
                Side::Bid => price >= level_price,
                Side::Ask => price <= level_price,
            };
            if !crosses {
                break;
            }

            let mut emptied = false;
            if let Some(queue) = book.get_mut(&level_price) {
                while remaining > 0 && !queue.is_empty() {
                    let front = queue.front_mut().unwrap();
                    let traded = front.qty.min(remaining);
                    fills.push(Fill {
                        resting_order_id: front.id,
                        resting_is_ours: front.is_ours,
                        side_of_resting_order: resting_side,
                        price: level_price,
                        qty: traded,
                        ts_nanos,
                    });
                    front.qty -= traded;
                    remaining -= traded;
                    let done = front.qty == 0;
                    if done {
                        queue.pop_front();
                    }
                }
                emptied = queue.is_empty();
            }
            if emptied {
                book.remove(&level_price);
            }
        }

        if remaining > 0 {
            self.insert_resting_order(id, side, price, remaining, ts_nanos, is_ours);
        }
        fills
    }

    pub fn cancel_order(&mut self, side: Side, price: Price, id: OrderId) -> bool {
        let book = self.book_mut(side);
        if let Some(queue) = book.get_mut(&price) {
            if let Some(pos) = queue.iter().position(|o| o.id == id) {
                queue.remove(pos);
                if queue.is_empty() {
                    book.remove(&price);
                }
                return true;
            }
        }
        false
    }

    /// Matches an incoming market order (an "aggressor") against resting
    /// limit orders on the opposite side, respecting price-then-time
    /// priority. Returns every partial/full fill generated, in the order
    /// they were executed.
    pub fn execute_market_order(&mut self, aggressor_side: Side, qty: Qty, ts_nanos: u64) -> Vec<Fill> {
        let mut remaining = qty;
        let mut fills = Vec::new();
        let resting_side = aggressor_side.opposite();
        let book = self.book_mut(resting_side);

        // Collect the price levels to walk in priority order before taking
        // any mutable borrow level-by-level.
        let price_levels: Vec<Price> = match aggressor_side {
            Side::Bid => book.keys().copied().collect(), // buy hits asks, cheapest first
            Side::Ask => book.keys().rev().copied().collect(), // sell hits bids, richest first
        };

        for price in price_levels {
            if remaining == 0 {
                break;
            }
            let mut emptied = false;
            if let Some(queue) = book.get_mut(&price) {
                while remaining > 0 && !queue.is_empty() {
                    let front = queue.front_mut().unwrap();
                    let traded = front.qty.min(remaining);
                    let fill = Fill {
                        resting_order_id: front.id,
                        resting_is_ours: front.is_ours,
                        side_of_resting_order: resting_side,
                        price,
                        qty: traded,
                        ts_nanos,
                    };
                    front.qty -= traded;
                    remaining -= traded;
                    let front_done = front.qty == 0;
                    fills.push(fill);
                    if front_done {
                        queue.pop_front();
                    }
                }
                emptied = queue.is_empty();
            }
            if emptied {
                book.remove(&price);
            }
        }
        fills
    }

    /// Applies the effect of an already-executed trade to this book. Used by
    /// a book that is built purely by *replaying* an L3 feed (the strategy's
    /// shadow book), where trades arrive as reports rather than being
    /// generated locally via `execute_market_order`.
    pub fn apply_trade(&mut self, side: Side, price: Price, id: OrderId, traded_qty: Qty) {
        let book = self.book_mut(side);
        let mut now_empty = false;
        if let Some(queue) = book.get_mut(&price) {
            if let Some(pos) = queue.iter().position(|o| o.id == id) {
                if traded_qty >= queue[pos].qty {
                    queue.remove(pos);
                } else {
                    queue[pos].qty -= traded_qty;
                }
            }
            now_empty = queue.is_empty();
        }
        if now_empty {
            book.remove(&price);
        }
    }

    /// Top `n_levels` on each side, aggregated by price -- the L2 view
    /// derived from the L3 book, which is what the depth-of-market chart
    /// shows on the dashboard.
    pub fn depth_snapshot(&self, n_levels: usize) -> (Vec<(Price, Qty)>, Vec<(Price, Qty)>) {
        let bid_levels = self
            .bids
            .iter()
            .rev()
            .take(n_levels)
            .map(|(p, q)| (*p, q.iter().map(|o| o.qty).sum()))
            .collect();
        let ask_levels = self
            .asks
            .iter()
            .take(n_levels)
            .map(|(p, q)| (*p, q.iter().map(|o| o.qty).sum()))
            .collect();
        (bid_levels, ask_levels)
    }

    /// Exports every currently-resting order as a flat authoritative
    /// snapshot -- used by the matching engine to periodically publish a
    /// resync point for the strategy's shadow book.
    pub fn export_resync(&self) -> BookResync {
        let bids = self
            .bids
            .values()
            .flat_map(|q| q.iter())
            .map(|o| (o.id, o.price, o.qty, o.is_ours))
            .collect();
        let asks = self
            .asks
            .values()
            .flat_map(|q| q.iter())
            .map(|o| (o.id, o.price, o.qty, o.is_ours))
            .collect();
        BookResync {
            bids,
            asks,
            seq_at_snapshot: 0, // the caller (which knows about the feed's sequence counter) overrides this
        }
    }

    /// Discards all current state and rebuilds from an authoritative
    /// snapshot -- used by the shadow book to recover from drift caused by
    /// feed gaps, rather than letting a dropped Cancel leave a permanent
    /// "ghost" order that silently compounds over the life of the process.
    pub fn rebuild_from_resync(&mut self, resync: &BookResync, ts_nanos: u64) {
        self.bids.clear();
        self.asks.clear();
        for &(id, price, qty, is_ours) in &resync.bids {
            self.insert_resting_order(id, Side::Bid, price, qty, ts_nanos, is_ours);
        }
        for &(id, price, qty, is_ours) in &resync.asks {
            self.insert_resting_order(id, Side::Ask, price, qty, ts_nanos, is_ours);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resting_orders_are_visible_at_best_bid_ask() {
        let mut book = OrderBook::new();
        book.insert_resting_order(1, Side::Bid, 9998, 10, 0, false);
        book.insert_resting_order(2, Side::Ask, 10002, 10, 0, false);
        assert_eq!(book.best_bid(), Some(9998));
        assert_eq!(book.best_ask(), Some(10002));
        assert_eq!(book.mid_ticks(), Some(10000.0));
    }

    #[test]
    fn market_order_matches_best_price_first() {
        let mut book = OrderBook::new();
        book.insert_resting_order(1, Side::Ask, 10005, 5, 0, false);
        book.insert_resting_order(2, Side::Ask, 10001, 5, 0, false); // cheaper ask, added second
        let fills = book.execute_market_order(Side::Bid, 5, 0);
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].price, 10001); // must hit the cheaper level first
        assert_eq!(fills[0].resting_order_id, 2);
        assert_eq!(book.best_ask(), Some(10005));
    }

    #[test]
    fn market_order_respects_time_priority_within_a_level() {
        let mut book = OrderBook::new();
        book.insert_resting_order(1, Side::Bid, 9999, 5, 0, false); // first in queue
        book.insert_resting_order(2, Side::Bid, 9999, 5, 0, false); // second in queue
        let fills = book.execute_market_order(Side::Ask, 5, 0);
        assert_eq!(fills[0].resting_order_id, 1); // FIFO: earlier order fills first
    }

    #[test]
    fn market_order_walks_multiple_levels_when_size_exceeds_top_level() {
        let mut book = OrderBook::new();
        book.insert_resting_order(1, Side::Ask, 10001, 3, 0, false);
        book.insert_resting_order(2, Side::Ask, 10002, 10, 0, false);
        let fills = book.execute_market_order(Side::Bid, 5, 0);
        assert_eq!(fills.len(), 2);
        assert_eq!((fills[0].price, fills[0].qty), (10001, 3));
        assert_eq!((fills[1].price, fills[1].qty), (10002, 2));
    }

    #[test]
    fn cancel_removes_only_the_targeted_order() {
        let mut book = OrderBook::new();
        book.insert_resting_order(1, Side::Bid, 9999, 5, 0, false);
        book.insert_resting_order(2, Side::Bid, 9999, 5, 0, false);
        assert!(book.cancel_order(Side::Bid, 9999, 1));
        let fills = book.execute_market_order(Side::Ask, 5, 0);
        assert_eq!(fills[0].resting_order_id, 2); // order 1 is gone, order 2 remains
    }

    #[test]
    fn depth_snapshot_aggregates_quantity_per_price_level() {
        let mut book = OrderBook::new();
        book.insert_resting_order(1, Side::Bid, 9999, 4, 0, false);
        book.insert_resting_order(2, Side::Bid, 9999, 6, 0, false);
        let (bids, _asks) = book.depth_snapshot(5);
        assert_eq!(bids[0], (9999, 10));
    }

    #[test]
    fn apply_trade_mirrors_a_partial_then_full_fill_reported_externally() {
        let mut book = OrderBook::new();
        book.insert_resting_order(1, Side::Bid, 9999, 10, 0, false);
        book.apply_trade(Side::Bid, 9999, 1, 4);
        assert_eq!(book.depth_snapshot(1).0, vec![(9999, 6)]);
        book.apply_trade(Side::Bid, 9999, 1, 6);
        assert_eq!(book.best_bid(), None); // fully consumed, level removed
    }

    #[test]
    fn resync_rebuild_discards_stale_ghost_orders() {
        let mut ground_truth = OrderBook::new();
        ground_truth.insert_resting_order(1, Side::Bid, 9999, 10, 0, false);
        ground_truth.insert_resting_order(2, Side::Ask, 10001, 10, 0, false);

        // simulate a shadow book that missed a Cancel event and now has a
        // "ghost" order (id 99) that ground truth no longer has
        let mut shadow = OrderBook::new();
        shadow.insert_resting_order(1, Side::Bid, 9999, 10, 0, false);
        shadow.insert_resting_order(99, Side::Bid, 10050, 5, 0, false); // ghost: above where a bid should be

        let resync = ground_truth.export_resync();
        shadow.rebuild_from_resync(&resync, 0);

        assert_eq!(shadow.best_bid(), Some(9999)); // ghost at 10050 is gone
        assert_eq!(shadow.best_ask(), Some(10001));
        assert_eq!(shadow.depth_snapshot(5).0.len(), 1);
    }

    #[test]
    fn submit_limit_order_matches_immediately_when_it_crosses_the_book() {
        // This is the actual bug fix: a naive order book lets a new bid rest
        // at or above the current best ask, which real exchanges never
        // allow -- such an order is marketable and must match immediately.
        let mut book = OrderBook::new();
        book.insert_resting_order(1, Side::Ask, 10000, 5, 0, false);
        let fills = book.submit_limit_order(2, Side::Bid, 10000, 5, 0, false);
        assert_eq!(fills.len(), 1);
        assert_eq!((fills[0].price, fills[0].qty), (10000, 5));
        assert_eq!(book.best_ask(), None); // fully matched away, nothing rests
        assert_eq!(book.best_bid(), None); // and the crossing bid didn't rest either
    }

    #[test]
    fn submit_limit_order_partially_fills_then_rests_the_remainder() {
        let mut book = OrderBook::new();
        book.insert_resting_order(1, Side::Ask, 10000, 3, 0, false);
        let fills = book.submit_limit_order(2, Side::Bid, 10000, 10, 0, false);
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].qty, 3);
        assert_eq!(book.best_ask(), None); // ask fully consumed
        assert_eq!(book.best_bid(), Some(10000)); // remaining 7 rests as a bid
        assert_eq!(book.depth_snapshot(1).0, vec![(10000, 7)]);
    }

    #[test]
    fn submit_limit_order_walks_multiple_crossing_levels_but_stops_at_the_limit_price() {
        let mut book = OrderBook::new();
        book.insert_resting_order(1, Side::Ask, 9999, 5, 0, false);
        book.insert_resting_order(2, Side::Ask, 10000, 5, 0, false);
        book.insert_resting_order(3, Side::Ask, 10001, 5, 0, false); // outside our limit price
        let fills = book.submit_limit_order(4, Side::Bid, 10000, 20, 0, false);
        assert_eq!(fills.len(), 2); // matches 9999 and 10000, but not 10001
        assert_eq!(fills.iter().map(|f| f.qty).sum::<u64>(), 10);
        assert_eq!(book.best_ask(), Some(10001)); // untouched, outside the limit price
        assert_eq!(book.best_bid(), Some(10000)); // remaining 10 rests at our limit price
    }

    #[test]
    fn submit_limit_order_rests_normally_when_it_does_not_cross() {
        let mut book = OrderBook::new();
        book.insert_resting_order(1, Side::Ask, 10005, 5, 0, false);
        let fills = book.submit_limit_order(2, Side::Bid, 9995, 5, 0, false);
        assert!(fills.is_empty());
        assert_eq!(book.best_bid(), Some(9995));
        assert_eq!(book.best_ask(), Some(10005));
    }
}
