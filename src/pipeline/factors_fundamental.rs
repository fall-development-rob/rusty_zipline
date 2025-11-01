//! Fundamental analysis factors
//!
//! This module provides fundamental metrics for stock valuation

/// PE Ratio - Price to Earnings Ratio
#[derive(Debug, Clone)]
pub struct PERatio;

impl PERatio {
    /// Calculate P/E ratio from price and earnings per share
    pub fn calculate(price: f64, eps: f64) -> Option<f64> {
        if eps.abs() < f64::EPSILON {
            None // Avoid division by zero
        } else {
            Some(price / eps)
        }
    }

    /// Calculate trailing P/E from price and trailing 12-month EPS
    pub fn trailing(price: f64, trailing_eps: f64) -> Option<f64> {
        Self::calculate(price, trailing_eps)
    }

    /// Calculate forward P/E from price and estimated future EPS
    pub fn forward(price: f64, forward_eps: f64) -> Option<f64> {
        Self::calculate(price, forward_eps)
    }
}

/// PB Ratio - Price to Book Ratio
#[derive(Debug, Clone)]
pub struct PBRatio;

impl PBRatio {
    /// Calculate P/B ratio from price and book value per share
    pub fn calculate(price: f64, book_value_per_share: f64) -> Option<f64> {
        if book_value_per_share.abs() < f64::EPSILON {
            None
        } else {
            Some(price / book_value_per_share)
        }
    }
}

/// PS Ratio - Price to Sales Ratio
#[derive(Debug, Clone)]
pub struct PSRatio;

impl PSRatio {
    /// Calculate P/S ratio from price and sales per share
    pub fn calculate(price: f64, sales_per_share: f64) -> Option<f64> {
        if sales_per_share.abs() < f64::EPSILON {
            None
        } else {
            Some(price / sales_per_share)
        }
    }
}

/// ROE - Return on Equity
#[derive(Debug, Clone)]
pub struct ROE;

impl ROE {
    /// Calculate ROE from net income and shareholder equity
    pub fn calculate(net_income: f64, shareholder_equity: f64) -> Option<f64> {
        if shareholder_equity.abs() < f64::EPSILON {
            None
        } else {
            Some((net_income / shareholder_equity) * 100.0)
        }
    }
}

/// ROA - Return on Assets
#[derive(Debug, Clone)]
pub struct ROA;

impl ROA {
    /// Calculate ROA from net income and total assets
    pub fn calculate(net_income: f64, total_assets: f64) -> Option<f64> {
        if total_assets.abs() < f64::EPSILON {
            None
        } else {
            Some((net_income / total_assets) * 100.0)
        }
    }
}

/// ROIC - Return on Invested Capital
#[derive(Debug, Clone)]
pub struct ROIC;

impl ROIC {
    /// Calculate ROIC from NOPAT and invested capital
    pub fn calculate(nopat: f64, invested_capital: f64) -> Option<f64> {
        if invested_capital.abs() < f64::EPSILON {
            None
        } else {
            Some((nopat / invested_capital) * 100.0)
        }
    }
}

/// Dividend Yield
#[derive(Debug, Clone)]
pub struct DividendYield;

impl DividendYield {
    /// Calculate dividend yield from annual dividend and price
    pub fn calculate(annual_dividend: f64, price: f64) -> Option<f64> {
        if price.abs() < f64::EPSILON {
            None
        } else {
            Some((annual_dividend / price) * 100.0)
        }
    }
}

/// EV/EBITDA - Enterprise Value to EBITDA
#[derive(Debug, Clone)]
pub struct EVToEBITDA;

impl EVToEBITDA {
    /// Calculate EV/EBITDA ratio
    pub fn calculate(enterprise_value: f64, ebitda: f64) -> Option<f64> {
        if ebitda.abs() < f64::EPSILON {
            None
        } else {
            Some(enterprise_value / ebitda)
        }
    }
}

/// Debt to Equity Ratio
#[derive(Debug, Clone)]
pub struct DebtToEquity;

impl DebtToEquity {
    /// Calculate D/E ratio from total debt and total equity
    pub fn calculate(total_debt: f64, total_equity: f64) -> Option<f64> {
        if total_equity.abs() < f64::EPSILON {
            None
        } else {
            Some(total_debt / total_equity)
        }
    }
}

/// Current Ratio
#[derive(Debug, Clone)]
pub struct CurrentRatio;

impl CurrentRatio {
    /// Calculate current ratio from current assets and current liabilities
    pub fn calculate(current_assets: f64, current_liabilities: f64) -> Option<f64> {
        if current_liabilities.abs() < f64::EPSILON {
            None
        } else {
            Some(current_assets / current_liabilities)
        }
    }
}

/// Quick Ratio (Acid-Test Ratio)
#[derive(Debug, Clone)]
pub struct QuickRatio;

impl QuickRatio {
    /// Calculate quick ratio
    pub fn calculate(
        current_assets: f64,
        inventory: f64,
        current_liabilities: f64,
    ) -> Option<f64> {
        if current_liabilities.abs() < f64::EPSILON {
            None
        } else {
            Some((current_assets - inventory) / current_liabilities)
        }
    }
}

/// Earnings Yield
#[derive(Debug, Clone)]
pub struct EarningsYield;

impl EarningsYield {
    /// Calculate earnings yield (inverse of P/E)
    pub fn calculate(eps: f64, price: f64) -> Option<f64> {
        if price.abs() < f64::EPSILON {
            None
        } else {
            Some((eps / price) * 100.0)
        }
    }
}

/// Payout Ratio
#[derive(Debug, Clone)]
pub struct PayoutRatio;

impl PayoutRatio {
    /// Calculate dividend payout ratio
    pub fn calculate(dividends: f64, net_income: f64) -> Option<f64> {
        if net_income.abs() < f64::EPSILON {
            None
        } else {
            Some((dividends / net_income) * 100.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;

    #[test]
    fn test_pe_ratio() {
        let pe = PERatio::calculate(100.0, 5.0);
        assert_eq!(pe, Some(20.0));

        let pe_zero = PERatio::calculate(100.0, 0.0);
        assert_eq!(pe_zero, None);
    }

    #[test]
    fn test_pb_ratio() {
        let pb = PBRatio::calculate(50.0, 25.0);
        assert_eq!(pb, Some(2.0));
    }

    #[test]
    fn test_ps_ratio() {
        let ps = PSRatio::calculate(80.0, 40.0);
        assert_eq!(ps, Some(2.0));
    }

    #[test]
    fn test_roe() {
        let roe = ROE::calculate(10_000.0, 100_000.0);
        assert_relative_eq!(roe.unwrap(), 10.0, epsilon = 0.01);
    }

    #[test]
    fn test_roa() {
        let roa = ROA::calculate(5_000.0, 100_000.0);
        assert_relative_eq!(roa.unwrap(), 5.0, epsilon = 0.01);
    }

    #[test]
    fn test_roic() {
        let roic = ROIC::calculate(8_000.0, 100_000.0);
        assert_relative_eq!(roic.unwrap(), 8.0, epsilon = 0.01);
    }

    #[test]
    fn test_dividend_yield() {
        let dy = DividendYield::calculate(5.0, 100.0);
        assert_relative_eq!(dy.unwrap(), 5.0, epsilon = 0.01);
    }

    #[test]
    fn test_ev_to_ebitda() {
        let ev_ebitda = EVToEBITDA::calculate(1_000_000.0, 100_000.0);
        assert_eq!(ev_ebitda, Some(10.0));
    }

    #[test]
    fn test_debt_to_equity() {
        let de = DebtToEquity::calculate(50_000.0, 100_000.0);
        assert_eq!(de, Some(0.5));
    }

    #[test]
    fn test_current_ratio() {
        let cr = CurrentRatio::calculate(200_000.0, 100_000.0);
        assert_eq!(cr, Some(2.0));
    }

    #[test]
    fn test_quick_ratio() {
        let qr = QuickRatio::calculate(200_000.0, 50_000.0, 100_000.0);
        assert_eq!(qr, Some(1.5)); // (200k - 50k) / 100k
    }

    #[test]
    fn test_earnings_yield() {
        let ey = EarningsYield::calculate(5.0, 100.0);
        assert_eq!(ey, Some(5.0));
    }

    #[test]
    fn test_payout_ratio() {
        let pr = PayoutRatio::calculate(30_000.0, 100_000.0);
        assert_eq!(pr, Some(30.0));
    }

    #[test]
    fn test_division_by_zero() {
        assert_eq!(PERatio::calculate(100.0, 0.0), None);
        assert_eq!(PBRatio::calculate(100.0, 0.0), None);
        assert_eq!(DividendYield::calculate(5.0, 0.0), None);
    }
}
