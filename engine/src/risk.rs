// Real-time position risk: parametric Value-at-Risk and Expected Shortfall
// on the open inventory, plus the kill-switch policy that watches them.

use std::f64::consts::PI;

pub struct RiskLimits {
    pub max_abs_inventory: i64,
    pub max_drawdown: f64,
    pub max_var_95: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct RiskMetrics {
    pub position_value: f64,
    pub unrealized_pnl: f64,
    pub var_95: f64,
    pub es_95: f64,
}

fn std_normal_pdf(z: f64) -> f64 {
    (-(z * z) / 2.0).exp() / (2.0 * PI).sqrt()
}

/// 95th-percentile z-score, i.e. Phi^-1(0.95).
const Z_95: f64 = 1.6448536269514722;
const TAIL_MASS_95: f64 = 0.05;

/// Parametric (variance-covariance) VaR/ES of the open inventory over the
/// given horizon, assuming approximately normal mark-to-market P&L changes.
/// `position_dollar_vol = |inventory| * sigma * sqrt(horizon)` is the
/// standard deviation of the position's dollar P&L over that horizon; VaR
/// and ES are then just that scaled by the appropriate normal-tail factor.
pub fn compute_risk(inventory: i64, mid: f64, sigma: f64, horizon_secs: f64, cash: f64) -> RiskMetrics {
    let position_value = inventory as f64 * mid;
    let unrealized_pnl = cash + position_value;
    let position_dollar_vol = (inventory as f64).abs() * sigma * horizon_secs.max(0.0).sqrt();
    let var_95 = Z_95 * position_dollar_vol;
    let es_95 = (std_normal_pdf(Z_95) / TAIL_MASS_95) * position_dollar_vol;
    RiskMetrics {
        position_value,
        unrealized_pnl,
        var_95,
        es_95,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KillReason {
    InventoryLimit,
    Drawdown,
    ValueAtRisk,
    Manual,
}

impl KillReason {
    pub fn describe(&self) -> &'static str {
        match self {
            KillReason::InventoryLimit => "inventory limit breached",
            KillReason::Drawdown => "drawdown limit breached",
            KillReason::ValueAtRisk => "VaR limit breached",
            KillReason::Manual => "manual kill switch triggered",
        }
    }
}

pub struct KillSwitch {
    pub limits: RiskLimits,
    peak_pnl: f64,
    triggered: Option<KillReason>,
}

impl KillSwitch {
    pub fn new(limits: RiskLimits) -> Self {
        Self {
            limits,
            peak_pnl: f64::MIN,
            triggered: None,
        }
    }

    pub fn is_triggered(&self) -> Option<KillReason> {
        self.triggered
    }

    /// Evaluate current state; latches the first reason found. A kill switch
    /// that silently un-trips when conditions later look fine isn't a kill
    /// switch -- call `reset()` explicitly to re-arm it.
    pub fn evaluate(&mut self, inventory: i64, metrics: &RiskMetrics, manual_kill: bool) -> Option<KillReason> {
        if self.triggered.is_some() {
            return self.triggered;
        }
        self.peak_pnl = self.peak_pnl.max(metrics.unrealized_pnl);
        let drawdown = self.peak_pnl - metrics.unrealized_pnl;

        let reason = if manual_kill {
            Some(KillReason::Manual)
        } else if inventory.abs() > self.limits.max_abs_inventory {
            Some(KillReason::InventoryLimit)
        } else if drawdown > self.limits.max_drawdown {
            Some(KillReason::Drawdown)
        } else if metrics.var_95 > self.limits.max_var_95 {
            Some(KillReason::ValueAtRisk)
        } else {
            None
        };
        self.triggered = reason;
        reason
    }

    #[allow(dead_code)]
    pub fn reset(&mut self) {
        self.triggered = None;
        self.peak_pnl = f64::MIN;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn var_and_es_scale_with_position_size_and_es_exceeds_var() {
        let small = compute_risk(1, 100.0, 0.05, 1.0, 0.0);
        let large = compute_risk(10, 100.0, 0.05, 1.0, 0.0);
        assert!(large.var_95 > small.var_95);
        assert!(large.es_95 > small.es_95);
        assert!(large.es_95 > large.var_95, "ES must exceed VaR at the same confidence level");
    }

    #[test]
    fn flat_inventory_has_zero_risk() {
        let flat = compute_risk(0, 100.0, 0.05, 1.0, 0.0);
        assert_eq!(flat.var_95, 0.0);
        assert_eq!(flat.es_95, 0.0);
    }

    #[test]
    fn kill_switch_trips_on_inventory_breach_and_latches() {
        let mut ks = KillSwitch::new(RiskLimits {
            max_abs_inventory: 5,
            max_drawdown: 1e9,
            max_var_95: 1e9,
        });
        let ok = compute_risk(3, 100.0, 0.05, 1.0, 0.0);
        assert_eq!(ks.evaluate(3, &ok, false), None);
        let over = compute_risk(6, 100.0, 0.05, 1.0, 0.0);
        assert_eq!(ks.evaluate(6, &over, false), Some(KillReason::InventoryLimit));
        // even once inventory looks fine again, a latched kill switch stays tripped
        let back_to_flat = compute_risk(0, 100.0, 0.05, 1.0, 0.0);
        assert_eq!(ks.evaluate(0, &back_to_flat, false), Some(KillReason::InventoryLimit));
    }

    #[test]
    fn manual_kill_overrides_everything() {
        let mut ks = KillSwitch::new(RiskLimits {
            max_abs_inventory: 1000,
            max_drawdown: 1e9,
            max_var_95: 1e9,
        });
        let metrics = compute_risk(0, 100.0, 0.05, 1.0, 0.0);
        assert_eq!(ks.evaluate(0, &metrics, true), Some(KillReason::Manual));
    }
}
