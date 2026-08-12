// The Avellaneda-Stoikov (2008) "High-frequency trading in a limit order
// book" optimal market-making formulas, plus a streaming realized-volatility
// estimator so the quoted spread reacts to *current* conditions instead of a
// fixed assumption.
//
//   reservation price:  r(s, t) = s - q * gamma * sigma^2 * (T - t)
//   optimal spread:     delta_a + delta_b = gamma * sigma^2 * (T - t)
//                                            + (2 / gamma) * ln(1 + gamma / kappa)
//
// where s = mid price, q = signed inventory, gamma = risk aversion,
// sigma^2 = variance of the price process per unit time, (T - t) = time
// remaining in the horizon, and kappa parameterises the assumed decay of
// order-arrival intensity with distance from the mid (arrival rate
// lambda(delta) = A * exp(-kappa * delta) in the original paper).
//
// Two honest simplifications, both called out in the README:
//   - gamma and kappa are treated as calibrated constants here rather than
//     fit from historical fill data (a real desk estimates kappa by
//     regressing log(fill rate) against quoted distance from mid).
//   - the original paper assumes a single trading session with a hard
//     terminal time T. Crypto markets don't have a close of trading, so this
//     implementation uses a rolling/receding horizon: (T - t) resets every
//     `horizon_secs`, a common practical adaptation for continuous markets.

pub struct VolEstimator {
    ewma_var: f64,
    lambda: f64, // EWMA decay, RiskMetrics-style (e.g. 0.94-0.97)
    last_mid: Option<f64>,
    last_ts_nanos: Option<u64>,
}

impl VolEstimator {
    pub fn new(initial_sigma: f64, lambda: f64) -> Self {
        Self {
            ewma_var: initial_sigma * initial_sigma,
            lambda,
            last_mid: None,
            last_ts_nanos: None,
        }
    }

    /// Feed a new mid-price observation; returns the updated sigma estimate,
    /// in price units per sqrt(second).
    pub fn update(&mut self, mid: f64, ts_nanos: u64) -> f64 {
        if let (Some(last_mid), Some(last_ts)) = (self.last_mid, self.last_ts_nanos) {
            let dt = ts_nanos.saturating_sub(last_ts) as f64 / 1e9;
            if dt > 1e-6 {
                let ret = mid - last_mid;
                let instant_var_per_sec = (ret * ret) / dt;
                self.ewma_var =
                    self.lambda * self.ewma_var + (1.0 - self.lambda) * instant_var_per_sec;
            }
        }
        self.last_mid = Some(mid);
        self.last_ts_nanos = Some(ts_nanos);
        self.sigma()
    }

    pub fn sigma(&self) -> f64 {
        self.ewma_var.sqrt()
    }
}

pub struct AvellanedaStoikov {
    pub gamma: f64,
    pub kappa: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct Quote {
    pub bid: f64,
    pub ask: f64,
    pub reservation_price: f64,
    pub half_spread: f64,
}

impl AvellanedaStoikov {
    pub fn quote(&self, mid: f64, inventory: i64, sigma: f64, time_remaining_secs: f64) -> Quote {
        let t = time_remaining_secs.max(0.0);
        let sigma2 = sigma * sigma;
        let reservation_price = mid - (inventory as f64) * self.gamma * sigma2 * t;
        let spread =
            self.gamma * sigma2 * t + (2.0 / self.gamma) * (1.0 + self.gamma / self.kappa).ln();
        let half_spread = (spread / 2.0).max(0.0);
        Quote {
            bid: reservation_price - half_spread,
            ask: reservation_price + half_spread,
            reservation_price,
            half_spread,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn model() -> AvellanedaStoikov {
        AvellanedaStoikov {
            gamma: 0.1,
            kappa: 1.5,
        }
    }

    #[test]
    fn zero_inventory_quotes_symmetrically_around_mid() {
        let q = model().quote(100.0, 0, 0.05, 30.0);
        assert!((q.reservation_price - 100.0).abs() < 1e-9);
        assert!(q.bid < 100.0 && q.ask > 100.0);
        assert!((100.0 - q.bid - (q.ask - 100.0)).abs() < 1e-9);
    }

    #[test]
    fn long_inventory_skews_reservation_price_down_to_encourage_selling() {
        let q_flat = model().quote(100.0, 0, 0.05, 30.0);
        let q_long = model().quote(100.0, 10, 0.05, 30.0);
        assert!(q_long.reservation_price < q_flat.reservation_price);
        assert!(q_long.bid < q_flat.bid && q_long.ask < q_flat.ask);
    }

    #[test]
    fn short_inventory_skews_reservation_price_up_to_encourage_buying() {
        let q_flat = model().quote(100.0, 0, 0.05, 30.0);
        let q_short = model().quote(100.0, -10, 0.05, 30.0);
        assert!(q_short.reservation_price > q_flat.reservation_price);
    }

    #[test]
    fn higher_volatility_widens_the_spread() {
        let calm = model().quote(100.0, 0, 0.02, 30.0);
        let turbulent = model().quote(100.0, 0, 0.10, 30.0);
        assert!(turbulent.half_spread > calm.half_spread);
    }

    #[test]
    fn more_time_remaining_widens_the_inventory_driven_component() {
        let near_horizon_end = model().quote(100.0, 5, 0.05, 1.0);
        let far_from_horizon_end = model().quote(100.0, 5, 0.05, 40.0);
        assert!(
            (far_from_horizon_end.reservation_price - 100.0).abs()
                > (near_horizon_end.reservation_price - 100.0).abs()
        );
    }

    #[test]
    fn vol_estimator_adapts_upward_given_consistently_large_moves() {
        let mut est = VolEstimator::new(0.02, 0.9);
        let mut ts = 0u64;
        let mut mid = 100.0;
        for _ in 0..200 {
            ts += 4_000_000; // 4ms steps
            mid += 0.5; // far bigger than a 0.02-sigma process would produce
            est.update(mid, ts);
        }
        assert!(est.sigma() > 0.02);
    }
}
