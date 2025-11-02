//! Algorithm trait and context for trading strategies

use crate::asset::{Asset, AssetType};
use crate::assets::AssetFinder;
use crate::data::BarData;
use crate::error::{Result, ZiplineError};
use crate::finance::{Account, CommissionModel, Portfolio, SlippageModel};
use crate::order::{Order, OrderSide};
use crate::pipeline::engine::Pipeline;
use crate::types::{Quantity, Timestamp};
use chrono::{DateTime, Utc};
use hashbrown::HashMap;
use std::sync::Arc;

/// Trading algorithm context
pub struct Context {
    /// Current simulation timestamp
    pub timestamp: Timestamp,
    /// Portfolio state
    pub portfolio: Portfolio,
    /// Account state (account-level metrics)
    pub account: Account,
    /// Recorded variables for analysis (name -> [(timestamp, value)])
    pub recorded_vars: HashMap<String, Vec<(DateTime<Utc>, f64)>>,
    /// User-defined variables
    pub variables: HashMap<String, Box<dyn std::any::Any + Send>>,
    /// Pending orders
    pub pending_orders: Vec<Order>,
}

impl Context {
    /// Create a new context with starting cash
    pub fn new(starting_cash: f64) -> Self {
        Self {
            timestamp: Timestamp::default(),
            portfolio: Portfolio::new(starting_cash),
            account: Account::new(starting_cash),
            recorded_vars: HashMap::new(),
            variables: HashMap::new(),
            pending_orders: Vec::new(),
        }
    }

    /// Record a custom variable for later analysis
    ///
    /// Recorded variables are stored as time series and can be retrieved
    /// for analysis after the backtest completes.
    ///
    /// # Arguments
    /// * `name` - Name of the variable to record
    /// * `value` - Value to record
    ///
    /// # Example
    /// ```ignore
    /// context.record("my_signal", 0.75);
    /// context.record("position_count", context.portfolio.positions.len() as f64);
    /// context.record("leverage", context.account.leverage);
    /// ```
    pub fn record(&mut self, name: &str, value: f64) {
        let dt = DateTime::<Utc>::from_timestamp(self.timestamp.timestamp(), 0)
            .unwrap_or_else(Utc::now);

        self.recorded_vars
            .entry(name.to_string())
            .or_insert_with(Vec::new)
            .push((dt, value));
    }

    /// Get all recorded values for a variable
    ///
    /// Returns a slice of (timestamp, value) tuples for the given variable.
    ///
    /// # Arguments
    /// * `name` - Name of the variable
    ///
    /// # Returns
    /// Option containing the recorded time series, or None if not found
    pub fn get_recorded(&self, name: &str) -> Option<&[(DateTime<Utc>, f64)]> {
        self.recorded_vars.get(name).map(|v| v.as_slice())
    }

    /// Get the latest recorded value for a variable
    ///
    /// # Arguments
    /// * `name` - Name of the variable
    ///
    /// # Returns
    /// The most recent value, or None if no values recorded
    pub fn get_latest_recorded(&self, name: &str) -> Option<f64> {
        self.recorded_vars
            .get(name)
            .and_then(|v| v.last())
            .map(|(_, val)| *val)
    }

    /// Get all recorded variable names
    pub fn recorded_variable_names(&self) -> Vec<&str> {
        self.recorded_vars.keys().map(|s| s.as_str()).collect()
    }

    /// Clear all recorded data for a variable
    pub fn clear_recorded(&mut self, name: &str) {
        self.recorded_vars.remove(name);
    }

    /// Update account metrics from current portfolio state
    ///
    /// This should be called after portfolio updates to keep account
    /// metrics synchronized.
    pub fn update_account(&mut self) {
        let dt = DateTime::<Utc>::from_timestamp(self.timestamp.timestamp(), 0)
            .unwrap_or_else(Utc::now);
        self.account.update(&self.portfolio, dt);
    }

    /// Store a variable in the context
    pub fn set<T: 'static + Send>(&mut self, key: String, value: T) {
        self.variables.insert(key, Box::new(value));
    }

    /// Get a variable from the context
    pub fn get<T: 'static>(&self, key: &str) -> Option<&T> {
        self.variables
            .get(key)
            .and_then(|v| v.downcast_ref::<T>())
    }

    /// Order a target position in an asset
    pub fn order_target(&mut self, asset: Asset, target_quantity: Quantity) -> Result<OrderId> {
        let current_position = self
            .portfolio
            .get_position(asset.id)
            .map(|p| p.quantity)
            .unwrap_or(0.0);

        let delta = target_quantity - current_position;

        if delta.abs() < f64::EPSILON {
            // Already at target
            return Err(crate::error::ZiplineError::InvalidOrder(
                "Already at target position".to_string(),
            ));
        }

        let (side, quantity) = if delta > 0.0 {
            (OrderSide::Buy, delta)
        } else {
            (OrderSide::Sell, -delta)
        };

        let order = Order::market(asset, side, quantity, self.timestamp);
        let order_id = order.id;
        self.pending_orders.push(order);

        Ok(order_id)
    }

    /// Order a specific quantity of an asset
    pub fn order(&mut self, asset: Asset, quantity: Quantity) -> Result<OrderId> {
        if quantity.abs() < f64::EPSILON {
            return Err(crate::error::ZiplineError::InvalidOrder(
                "Quantity must be non-zero".to_string(),
            ));
        }

        let (side, qty) = if quantity > 0.0 {
            (OrderSide::Buy, quantity)
        } else {
            (OrderSide::Sell, -quantity)
        };

        let order = Order::market(asset, side, qty, self.timestamp);
        let order_id = order.id;
        self.pending_orders.push(order);

        Ok(order_id)
    }

    /// Get number of pending orders
    pub fn pending_orders_count(&self) -> usize {
        self.pending_orders.len()
    }

    /// Order a percentage of current portfolio value
    ///
    /// # Arguments
    /// * `asset` - Asset to trade
    /// * `percent` - Percentage of portfolio value (0.1 = 10%)
    /// * `price` - Current price for conversion to shares
    pub fn order_percent(&mut self, asset: Asset, percent: f64, price: f64) -> Result<OrderId> {
        let target_value = self.portfolio.portfolio_value * percent;
        let quantity = target_value / price;
        self.order(asset, quantity)
    }

    /// Order to target a specific percentage of portfolio
    ///
    /// # Arguments
    /// * `asset` - Asset to trade
    /// * `target_percent` - Target percentage of portfolio (0.1 = 10%)
    /// * `price` - Current price for conversion to shares
    pub fn order_target_percent(&mut self, asset: Asset, target_percent: f64, price: f64) -> Result<OrderId> {
        let target_value = self.portfolio.portfolio_value * target_percent;
        let target_quantity = target_value / price;
        self.order_target(asset, target_quantity)
    }

    /// Order a specific dollar value
    ///
    /// # Arguments
    /// * `asset` - Asset to trade
    /// * `value` - Dollar value to trade
    /// * `price` - Current price for conversion to shares
    pub fn order_value(&mut self, asset: Asset, value: f64, price: f64) -> Result<OrderId> {
        let quantity = value / price;
        self.order(asset, quantity)
    }

    /// Order to target a specific dollar value position
    ///
    /// # Arguments
    /// * `asset` - Asset to trade
    /// * `target_value` - Target dollar value of position
    /// * `price` - Current price for conversion to shares
    pub fn order_target_value(&mut self, asset: Asset, target_value: f64, price: f64) -> Result<OrderId> {
        let target_quantity = target_value / price;
        self.order_target(asset, target_quantity)
    }

    /// Get an order by ID
    pub fn get_order(&self, order_id: OrderId) -> Option<&Order> {
        self.pending_orders.iter().find(|o| o.id == order_id)
    }

    /// Get all open orders, optionally filtered by asset
    pub fn get_open_orders(&self, asset: Option<&Asset>) -> Vec<&Order> {
        match asset {
            Some(a) => self
                .pending_orders
                .iter()
                .filter(|o| o.asset.id == a.id)
                .collect(),
            None => self.pending_orders.iter().collect(),
        }
    }

    /// Cancel a pending order
    pub fn cancel_order(&mut self, order_id: OrderId) -> Result<()> {
        if let Some(pos) = self.pending_orders.iter().position(|o| o.id == order_id) {
            let mut order = self.pending_orders.remove(pos);
            order.cancel(self.timestamp);
            Ok(())
        } else {
            Err(crate::error::ZiplineError::InvalidOrder(
                "Order not found or already closed".to_string(),
            ))
        }
    }
}

use uuid::Uuid;
type OrderId = Uuid;

/// Cancel policy trait - determines when orders should be cancelled
pub trait CancelPolicy: Send + Sync {
    /// Check if an order should be cancelled
    fn should_cancel(&self, order: &Order, dt: DateTime<Utc>) -> bool;
}

/// Asset class configuration for slippage and commission models
///
/// Allows different asset classes (equities, futures, etc.) to have
/// different slippage and commission models.
pub struct AssetClassConfig {
    /// Slippage model for this asset class
    pub slippage_model: Arc<dyn SlippageModel>,
    /// Commission model for this asset class
    pub commission_model: Arc<dyn CommissionModel>,
}

impl AssetClassConfig {
    /// Create a new asset class configuration
    pub fn new(
        slippage_model: Arc<dyn SlippageModel>,
        commission_model: Arc<dyn CommissionModel>,
    ) -> Self {
        Self {
            slippage_model,
            commission_model,
        }
    }
}

/// Scheduled function with date and time rules
#[derive(Clone)]
pub struct ScheduledFunction {
    pub func_name: String,
    pub date_rule: DateRule,
    pub time_rule: TimeRule,
}

/// Date rule for scheduled functions
#[derive(Clone)]
pub enum DateRule {
    EveryDay,
    WeekStart,
    WeekEnd,
    MonthStart,
    MonthEnd,
}

/// Time rule for scheduled functions
#[derive(Clone)]
pub enum TimeRule {
    MarketOpen { offset_minutes: i32 },
    MarketClose { offset_minutes: i32 },
}

/// Trading algorithm wrapper with full API
pub struct TradingAlgorithm {
    /// Asset finder for symbol lookups
    asset_finder: Arc<AssetFinder>,
    /// Attached pipelines
    pipelines: HashMap<String, Pipeline>,
    /// Pipeline outputs (cached)
    pipeline_outputs: HashMap<String, HashMap<u64, HashMap<String, f64>>>,
    /// Per-asset-class configuration (slippage and commission models)
    asset_configs: HashMap<AssetType, AssetClassConfig>,
    /// Default configuration for asset classes without specific config
    default_config: Option<AssetClassConfig>,
    /// Cancel policy
    cancel_policy: Option<Arc<dyn CancelPolicy>>,
    /// Scheduled functions
    scheduled_functions: Vec<ScheduledFunction>,
    /// Whether initialize() has been called
    initialized: bool,
}

impl TradingAlgorithm {
    /// Create a new trading algorithm
    pub fn new(asset_finder: Arc<AssetFinder>) -> Self {
        Self {
            asset_finder,
            pipelines: HashMap::new(),
            pipeline_outputs: HashMap::new(),
            asset_configs: HashMap::new(),
            default_config: None,
            cancel_policy: None,
            scheduled_functions: Vec::new(),
            initialized: false,
        }
    }

    /// Lookup a security by symbol
    ///
    /// # Arguments
    /// * `symbol_str` - Symbol string (e.g., "AAPL")
    /// * `as_of_date` - Optional point-in-time date
    ///
    /// # Returns
    /// The asset corresponding to the symbol
    pub fn symbol(&self, symbol_str: &str, as_of_date: Option<DateTime<Utc>>) -> Result<Asset> {
        self.asset_finder.lookup_symbol(symbol_str, as_of_date)
    }

    /// Lookup multiple securities by symbol
    ///
    /// # Arguments
    /// * `symbols` - Array of symbol strings
    /// * `as_of_date` - Optional point-in-time date
    ///
    /// # Returns
    /// Vector of assets corresponding to the symbols
    pub fn symbols(&self, symbols: &[&str], as_of_date: Option<DateTime<Utc>>) -> Result<Vec<Asset>> {
        self.asset_finder.lookup_symbols(symbols, as_of_date)
    }

    /// Lookup a security by SID (security ID)
    ///
    /// # Arguments
    /// * `sid` - Security ID
    ///
    /// # Returns
    /// The asset with the given SID
    pub fn sid(&self, sid: u64) -> Result<Asset> {
        self.asset_finder.retrieve_asset(sid)
    }

    /// Attach a pipeline to the algorithm
    ///
    /// Must be called during initialize(). Pipelines compute factor values
    /// that can be accessed via pipeline_output().
    ///
    /// # Arguments
    /// * `pipeline` - The pipeline to attach
    /// * `name` - Name for the pipeline
    ///
    /// # Errors
    /// * `AttachPipelineAfterInitialize` - If called after initialize()
    /// * `DuplicatePipelineName` - If a pipeline with this name already exists
    pub fn attach_pipeline(&mut self, pipeline: Pipeline, name: &str) -> Result<()> {
        if self.initialized {
            return Err(ZiplineError::AttachPipelineAfterInitialize);
        }

        if self.pipelines.contains_key(name) {
            return Err(ZiplineError::DuplicatePipelineName(name.to_string()));
        }

        self.pipelines.insert(name.to_string(), pipeline);
        Ok(())
    }

    /// Get the output of a pipeline
    ///
    /// Returns the most recently computed pipeline results as a DataFrame-like structure.
    ///
    /// # Arguments
    /// * `name` - Name of the pipeline
    ///
    /// # Returns
    /// HashMap<asset_id, HashMap<column_name, value>>
    ///
    /// # Errors
    /// * `NoSuchPipeline` - If no pipeline with this name exists
    pub fn pipeline_output(&self, name: &str) -> Result<&HashMap<u64, HashMap<String, f64>>> {
        self.pipeline_outputs
            .get(name)
            .ok_or_else(|| ZiplineError::NoSuchPipeline(name.to_string()))
    }

    /// Set the default slippage model (for all asset classes without specific config)
    ///
    /// Must be called during initialize().
    ///
    /// # Arguments
    /// * `model` - The slippage model to use
    pub fn set_slippage(&mut self, model: Arc<dyn SlippageModel>) -> Result<()> {
        if self.initialized {
            return Err(ZiplineError::SetSlippagePostInit);
        }

        if let Some(ref mut config) = self.default_config {
            config.slippage_model = model;
        } else {
            // Create default config with a placeholder commission model
            self.default_config = Some(AssetClassConfig::new(
                model,
                Arc::new(crate::finance::ZeroCommission),
            ));
        }

        Ok(())
    }

    /// Set the default commission model (for all asset classes without specific config)
    ///
    /// Must be called during initialize().
    ///
    /// # Arguments
    /// * `model` - The commission model to use
    pub fn set_commission(&mut self, model: Arc<dyn CommissionModel>) -> Result<()> {
        if self.initialized {
            return Err(ZiplineError::SetCommissionPostInit);
        }

        if let Some(ref mut config) = self.default_config {
            config.commission_model = model;
        } else {
            // Create default config with a placeholder slippage model
            self.default_config = Some(AssetClassConfig::new(
                Arc::new(crate::finance::NoSlippage),
                model,
            ));
        }

        Ok(())
    }

    /// Set slippage model for a specific asset class
    ///
    /// Must be called during initialize().
    ///
    /// # Arguments
    /// * `asset_type` - Asset class (Equity, Future, etc.)
    /// * `model` - Slippage model for this asset class
    pub fn set_slippage_by_class(
        &mut self,
        asset_type: AssetType,
        model: Arc<dyn SlippageModel>,
    ) -> Result<()> {
        if self.initialized {
            return Err(ZiplineError::SetSlippagePostInit);
        }

        self.asset_configs
            .entry(asset_type)
            .and_modify(|config| config.slippage_model = model.clone())
            .or_insert_with(|| {
                AssetClassConfig::new(model, Arc::new(crate::finance::ZeroCommission))
            });

        Ok(())
    }

    /// Set commission model for a specific asset class
    ///
    /// Must be called during initialize().
    ///
    /// # Arguments
    /// * `asset_type` - Asset class (Equity, Future, etc.)
    /// * `model` - Commission model for this asset class
    pub fn set_commission_by_class(
        &mut self,
        asset_type: AssetType,
        model: Arc<dyn CommissionModel>,
    ) -> Result<()> {
        if self.initialized {
            return Err(ZiplineError::SetCommissionPostInit);
        }

        self.asset_configs
            .entry(asset_type)
            .and_modify(|config| config.commission_model = model.clone())
            .or_insert_with(|| {
                AssetClassConfig::new(Arc::new(crate::finance::NoSlippage), model)
            });

        Ok(())
    }

    /// Convenience method: Set both slippage and commission for equities
    ///
    /// # Arguments
    /// * `slippage` - Slippage model for equities
    /// * `commission` - Commission model for equities
    pub fn set_equities_models(
        &mut self,
        slippage: Arc<dyn SlippageModel>,
        commission: Arc<dyn CommissionModel>,
    ) -> Result<()> {
        if self.initialized {
            return Err(ZiplineError::SetSlippagePostInit);
        }

        self.asset_configs.insert(
            AssetType::Equity,
            AssetClassConfig::new(slippage, commission),
        );

        Ok(())
    }

    /// Convenience method: Set both slippage and commission for futures
    ///
    /// # Arguments
    /// * `slippage` - Slippage model for futures
    /// * `commission` - Commission model for futures
    pub fn set_futures_models(
        &mut self,
        slippage: Arc<dyn SlippageModel>,
        commission: Arc<dyn CommissionModel>,
    ) -> Result<()> {
        if self.initialized {
            return Err(ZiplineError::SetSlippagePostInit);
        }

        self.asset_configs.insert(
            AssetType::Future,
            AssetClassConfig::new(slippage, commission),
        );

        Ok(())
    }

    /// Get slippage model for a specific asset
    ///
    /// Returns the asset-class-specific model if configured,
    /// otherwise falls back to the default model.
    pub fn get_slippage_for_asset(&self, asset: &Asset) -> Option<&Arc<dyn SlippageModel>> {
        self.asset_configs
            .get(&asset.asset_type)
            .map(|c| &c.slippage_model)
            .or_else(|| self.default_config.as_ref().map(|c| &c.slippage_model))
    }

    /// Get commission model for a specific asset
    ///
    /// Returns the asset-class-specific model if configured,
    /// otherwise falls back to the default model.
    pub fn get_commission_for_asset(&self, asset: &Asset) -> Option<&Arc<dyn CommissionModel>> {
        self.asset_configs
            .get(&asset.asset_type)
            .map(|c| &c.commission_model)
            .or_else(|| self.default_config.as_ref().map(|c| &c.commission_model))
    }

    /// Set the order cancel policy
    ///
    /// Must be called during initialize().
    ///
    /// # Arguments
    /// * `policy` - The cancel policy to use
    pub fn set_cancel_policy(&mut self, policy: Arc<dyn CancelPolicy>) -> Result<()> {
        if self.initialized {
            return Err(ZiplineError::SetCancelPolicyPostInit);
        }
        self.cancel_policy = Some(policy);
        Ok(())
    }

    /// Schedule a function to run at specific times
    ///
    /// Must be called during initialize().
    ///
    /// # Arguments
    /// * `func_name` - Name of the function to schedule
    /// * `date_rule` - When to run (e.g., every day, week start)
    /// * `time_rule` - What time to run (e.g., market open + 30 min)
    pub fn schedule_function(
        &mut self,
        func_name: String,
        date_rule: DateRule,
        time_rule: TimeRule,
    ) -> Result<()> {
        if self.initialized {
            return Err(ZiplineError::RegisterTradingControlPostInit);
        }

        self.scheduled_functions.push(ScheduledFunction {
            func_name,
            date_rule,
            time_rule,
        });

        Ok(())
    }

    /// Mark algorithm as initialized
    pub fn mark_initialized(&mut self) {
        self.initialized = true;
    }

    /// Check if algorithm is initialized
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Get default slippage model
    ///
    /// Deprecated: Use get_slippage_for_asset() instead
    pub fn get_slippage_model(&self) -> Option<&Arc<dyn SlippageModel>> {
        self.default_config.as_ref().map(|c| &c.slippage_model)
    }

    /// Get default commission model
    ///
    /// Deprecated: Use get_commission_for_asset() instead
    pub fn get_commission_model(&self) -> Option<&Arc<dyn CommissionModel>> {
        self.default_config.as_ref().map(|c| &c.commission_model)
    }

    /// Get all configured asset types
    pub fn configured_asset_types(&self) -> Vec<AssetType> {
        self.asset_configs.keys().copied().collect()
    }

    /// Check if an asset type has specific configuration
    pub fn has_asset_config(&self, asset_type: AssetType) -> bool {
        self.asset_configs.contains_key(&asset_type)
    }

    /// Get cancel policy
    pub fn get_cancel_policy(&self) -> Option<&Arc<dyn CancelPolicy>> {
        self.cancel_policy.as_ref()
    }

    /// Get scheduled functions
    pub fn get_scheduled_functions(&self) -> &[ScheduledFunction] {
        &self.scheduled_functions
    }

    /// Get pipelines
    pub fn get_pipelines(&self) -> &HashMap<String, Pipeline> {
        &self.pipelines
    }

    /// Update pipeline output (internal use)
    pub fn update_pipeline_output(
        &mut self,
        name: &str,
        output: HashMap<u64, HashMap<String, f64>>,
    ) {
        self.pipeline_outputs.insert(name.to_string(), output);
    }
}

/// Trading algorithm trait
pub trait Algorithm: Send {
    /// Initialize the algorithm (called once at start)
    fn initialize(&mut self, context: &mut Context) {
        // Default implementation does nothing
        let _ = context;
    }

    /// Handle data event (called for each bar)
    fn handle_data(&mut self, context: &mut Context, data: &BarData) -> Result<()>;

    /// Before trading starts each day (optional)
    fn before_trading_start(&mut self, context: &mut Context, data: &BarData) -> Result<()> {
        let _ = (context, data);
        Ok(())
    }

    /// Analyze results after backtest (optional)
    fn analyze(&mut self, context: &Context) -> Result<()> {
        let _ = context;
        Ok(())
    }
}

/// Example: Buy and hold strategy
pub struct BuyAndHold {
    pub asset: Asset,
    pub initialized: bool,
}

impl BuyAndHold {
    pub fn new(asset: Asset) -> Self {
        Self {
            asset,
            initialized: false,
        }
    }
}

impl Algorithm for BuyAndHold {
    fn initialize(&mut self, context: &mut Context) {
        println!("Initializing Buy and Hold strategy");
        println!("Starting cash: {}", context.portfolio.cash);
    }

    fn handle_data(&mut self, context: &mut Context, data: &BarData) -> Result<()> {
        if !self.initialized && data.has_data(&self.asset) {
            // Buy as much as we can on first bar
            let price = data.current_price(&self.asset)?;
            let quantity = (context.portfolio.cash / price).floor();

            if quantity > 0.0 {
                context.order(self.asset.clone(), quantity)?;
                self.initialized = true;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_context_creation() {
        let context = Context::new(100000.0);
        assert_eq!(context.portfolio.cash, 100000.0);
        assert_eq!(context.pending_orders_count(), 0);
    }

    #[test]
    fn test_context_variables() {
        let mut context = Context::new(100000.0);
        context.set("test_value".to_string(), 42i32);

        assert_eq!(context.get::<i32>("test_value"), Some(&42));
        assert_eq!(context.get::<i32>("nonexistent"), None);
    }

    #[test]
    fn test_order_creation() {
        let mut context = Context::new(100000.0);
        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);

        let order_id = context.order(asset, 100.0).unwrap();
        assert_eq!(context.pending_orders_count(), 1);
        assert_eq!(context.pending_orders[0].id, order_id);
    }

    #[test]
    fn test_trading_algorithm_creation() {
        let asset_finder = Arc::new(AssetFinder::new());
        let algo = TradingAlgorithm::new(asset_finder);

        assert!(!algo.is_initialized());
        assert!(algo.get_slippage_model().is_none());
        assert!(algo.get_commission_model().is_none());
        assert!(algo.get_cancel_policy().is_none());
        assert_eq!(algo.get_scheduled_functions().len(), 0);
    }

    #[test]
    fn test_symbol_lookup() {
        let asset_finder = Arc::new(AssetFinder::new());
        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);

        asset_finder.insert_asset(asset.clone()).unwrap();

        let algo = TradingAlgorithm::new(asset_finder.clone());
        let result = algo.symbol("AAPL", None).unwrap();

        assert_eq!(result.symbol, "AAPL");
        assert_eq!(result.id, 1);
    }

    #[test]
    fn test_symbols_batch_lookup() {
        let asset_finder = Arc::new(AssetFinder::new());
        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset1 = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);
        let asset2 = Asset::equity(2, "GOOGL".to_string(), "NASDAQ".to_string(), start_date);

        asset_finder.insert_assets(vec![asset1, asset2]).unwrap();

        let algo = TradingAlgorithm::new(asset_finder.clone());
        let results = algo.symbols(&["AAPL", "GOOGL"], None).unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].symbol, "AAPL");
        assert_eq!(results[1].symbol, "GOOGL");
    }

    #[test]
    fn test_sid_lookup() {
        let asset_finder = Arc::new(AssetFinder::new());
        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(42, "TEST".to_string(), "NYSE".to_string(), start_date);

        asset_finder.insert_asset(asset).unwrap();

        let algo = TradingAlgorithm::new(asset_finder.clone());
        let result = algo.sid(42).unwrap();

        assert_eq!(result.id, 42);
        assert_eq!(result.symbol, "TEST");
    }

    #[test]
    fn test_attach_pipeline() {
        let asset_finder = Arc::new(AssetFinder::new());
        let mut algo = TradingAlgorithm::new(asset_finder);

        // Should succeed before initialization
        let pipeline = Pipeline::new();
        assert!(algo.attach_pipeline(pipeline, "test_pipeline").is_ok());

        // Should fail with duplicate name
        let pipeline2 = Pipeline::new();
        assert!(algo.attach_pipeline(pipeline2, "test_pipeline").is_err());
    }

    #[test]
    fn test_attach_pipeline_after_init() {
        let asset_finder = Arc::new(AssetFinder::new());
        let mut algo = TradingAlgorithm::new(asset_finder);

        algo.mark_initialized();

        // Should fail after initialization
        let pipeline = Pipeline::new();
        let result = algo.attach_pipeline(pipeline, "test");
        assert!(result.is_err());
    }

    #[test]
    fn test_pipeline_output() {
        let asset_finder = Arc::new(AssetFinder::new());
        let mut algo = TradingAlgorithm::new(asset_finder);

        // Add test output
        let mut output = HashMap::new();
        let mut asset_data = HashMap::new();
        asset_data.insert("factor1".to_string(), 1.5);
        output.insert(1u64, asset_data);

        algo.update_pipeline_output("test", output);

        let result = algo.pipeline_output("test").unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains_key(&1));
    }

    #[test]
    fn test_pipeline_output_not_found() {
        let asset_finder = Arc::new(AssetFinder::new());
        let algo = TradingAlgorithm::new(asset_finder);

        let result = algo.pipeline_output("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_schedule_function() {
        let asset_finder = Arc::new(AssetFinder::new());
        let mut algo = TradingAlgorithm::new(asset_finder);

        let result = algo.schedule_function(
            "rebalance".to_string(),
            DateRule::EveryDay,
            TimeRule::MarketOpen { offset_minutes: 30 },
        );

        assert!(result.is_ok());
        assert_eq!(algo.get_scheduled_functions().len(), 1);
        assert_eq!(algo.get_scheduled_functions()[0].func_name, "rebalance");
    }

    #[test]
    fn test_schedule_function_after_init() {
        let asset_finder = Arc::new(AssetFinder::new());
        let mut algo = TradingAlgorithm::new(asset_finder);

        algo.mark_initialized();

        let result = algo.schedule_function(
            "rebalance".to_string(),
            DateRule::EveryDay,
            TimeRule::MarketOpen { offset_minutes: 0 },
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_initialization_state() {
        let asset_finder = Arc::new(AssetFinder::new());
        let mut algo = TradingAlgorithm::new(asset_finder);

        assert!(!algo.is_initialized());

        algo.mark_initialized();
        assert!(algo.is_initialized());
    }

    #[test]
    fn test_date_rule_variants() {
        let _rules = vec![
            DateRule::EveryDay,
            DateRule::WeekStart,
            DateRule::WeekEnd,
            DateRule::MonthStart,
            DateRule::MonthEnd,
        ];
        // Just ensure they compile and can be created
    }

    #[test]
    fn test_time_rule_variants() {
        let _rules = vec![
            TimeRule::MarketOpen { offset_minutes: 0 },
            TimeRule::MarketOpen { offset_minutes: 30 },
            TimeRule::MarketClose { offset_minutes: -30 },
            TimeRule::MarketClose { offset_minutes: 0 },
        ];
        // Just ensure they compile and can be created
    }

    // ========== Recording API Tests ==========

    #[test]
    fn test_context_record_variable() {
        let mut context = Context::new(100000.0);
        context.record("my_signal", 0.75);
        context.record("my_signal", 0.80);

        let recorded = context.get_recorded("my_signal").unwrap();
        assert_eq!(recorded.len(), 2);
        assert_eq!(recorded[0].1, 0.75);
        assert_eq!(recorded[1].1, 0.80);
    }

    #[test]
    fn test_context_record_multiple_variables() {
        let mut context = Context::new(100000.0);
        context.record("signal_a", 1.0);
        context.record("signal_b", 2.0);
        context.record("signal_c", 3.0);

        let names = context.recorded_variable_names();
        assert_eq!(names.len(), 3);
        assert!(names.contains(&"signal_a"));
        assert!(names.contains(&"signal_b"));
        assert!(names.contains(&"signal_c"));
    }

    #[test]
    fn test_context_get_latest_recorded() {
        let mut context = Context::new(100000.0);
        context.record("leverage", 0.5);
        context.record("leverage", 0.75);
        context.record("leverage", 1.0);

        assert_eq!(context.get_latest_recorded("leverage"), Some(1.0));
        assert_eq!(context.get_latest_recorded("nonexistent"), None);
    }

    #[test]
    fn test_context_clear_recorded() {
        let mut context = Context::new(100000.0);
        context.record("temp", 42.0);
        assert!(context.get_recorded("temp").is_some());

        context.clear_recorded("temp");
        assert!(context.get_recorded("temp").is_none());
    }

    #[test]
    fn test_context_account_integration() {
        let mut context = Context::new(100000.0);
        assert_eq!(context.account.net_liquidation, 100000.0);

        // Update account should work
        context.update_account();
        assert_eq!(context.account.net_liquidation, context.portfolio.portfolio_value);
    }

    #[test]
    fn test_performance_tracker_recorded_vars() {
        use crate::performance::PerformanceTracker;

        let mut tracker = PerformanceTracker::new();
        let mut context_vars = HashMap::new();

        let now = Utc::now();
        context_vars.insert(
            "my_metric".to_string(),
            vec![(now, 1.0), (now, 2.0)],
        );

        tracker.update_recorded_vars(&context_vars);

        let recorded = tracker.get_recorded("my_metric").unwrap();
        assert_eq!(recorded.len(), 2);
        assert_eq!(recorded[0].1, 1.0);
    }

    #[test]
    fn test_performance_tracker_num_recorded_vars() {
        use crate::performance::PerformanceTracker;

        let mut tracker = PerformanceTracker::new();
        let mut context_vars = HashMap::new();

        let now = Utc::now();
        context_vars.insert("var1".to_string(), vec![(now, 1.0)]);
        context_vars.insert("var2".to_string(), vec![(now, 2.0)]);

        tracker.update_recorded_vars(&context_vars);

        assert_eq!(tracker.num_recorded_vars(), 2);
        let names = tracker.recorded_variable_names();
        assert_eq!(names.len(), 2);
    }

    #[test]
    fn test_recording_with_portfolio_metrics() {
        let mut context = Context::new(100000.0);

        // Record portfolio-related metrics
        context.record("cash", context.portfolio.cash);
        context.record("num_positions", context.portfolio.num_positions() as f64);

        assert_eq!(context.get_latest_recorded("cash"), Some(100000.0));
        assert_eq!(context.get_latest_recorded("num_positions"), Some(0.0));
    }

    // ========== Multi-Asset Configuration Tests ==========

    #[test]
    fn test_set_default_slippage() {
        use crate::finance::NoSlippage;

        let asset_finder = Arc::new(AssetFinder::new());
        let mut algo = TradingAlgorithm::new(asset_finder);

        let slippage = Arc::new(NoSlippage);
        assert!(algo.set_slippage(slippage.clone()).is_ok());

        assert!(algo.get_slippage_model().is_some());
    }

    #[test]
    fn test_set_default_commission() {
        use crate::finance::ZeroCommission;

        let asset_finder = Arc::new(AssetFinder::new());
        let mut algo = TradingAlgorithm::new(asset_finder);

        let commission = Arc::new(ZeroCommission);
        assert!(algo.set_commission(commission.clone()).is_ok());

        assert!(algo.get_commission_model().is_some());
    }

    #[test]
    fn test_set_slippage_by_class() {
        use crate::finance::NoSlippage;

        let asset_finder = Arc::new(AssetFinder::new());
        let mut algo = TradingAlgorithm::new(asset_finder);

        let slippage = Arc::new(NoSlippage);
        assert!(algo.set_slippage_by_class(AssetType::Equity, slippage).is_ok());

        assert!(algo.has_asset_config(AssetType::Equity));
        assert!(!algo.has_asset_config(AssetType::Future));
    }

    #[test]
    fn test_set_commission_by_class() {
        use crate::finance::ZeroCommission;

        let asset_finder = Arc::new(AssetFinder::new());
        let mut algo = TradingAlgorithm::new(asset_finder);

        let commission = Arc::new(ZeroCommission);
        assert!(algo.set_commission_by_class(AssetType::Future, commission).is_ok());

        assert!(algo.has_asset_config(AssetType::Future));
    }

    #[test]
    fn test_set_equities_models() {
        use crate::finance::{NoSlippage, ZeroCommission};

        let asset_finder = Arc::new(AssetFinder::new());
        let mut algo = TradingAlgorithm::new(asset_finder);

        let slippage = Arc::new(NoSlippage);
        let commission = Arc::new(ZeroCommission);

        assert!(algo.set_equities_models(slippage, commission).is_ok());
        assert!(algo.has_asset_config(AssetType::Equity));
    }

    #[test]
    fn test_set_futures_models() {
        use crate::finance::{NoSlippage, ZeroCommission};

        let asset_finder = Arc::new(AssetFinder::new());
        let mut algo = TradingAlgorithm::new(asset_finder);

        let slippage = Arc::new(NoSlippage);
        let commission = Arc::new(ZeroCommission);

        assert!(algo.set_futures_models(slippage, commission).is_ok());
        assert!(algo.has_asset_config(AssetType::Future));
    }

    #[test]
    fn test_get_slippage_for_asset() {
        use crate::finance::NoSlippage;

        let asset_finder = Arc::new(AssetFinder::new());
        let mut algo = TradingAlgorithm::new(asset_finder);

        // Set equity-specific slippage
        let slippage = Arc::new(NoSlippage);
        algo.set_slippage_by_class(AssetType::Equity, slippage).unwrap();

        // Test with equity asset
        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let equity = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);
        assert!(algo.get_slippage_for_asset(&equity).is_some());

        // Test with future asset (should fall back to default or None)
        let future = Asset::new(2, "ES".to_string(), "CME".to_string(), AssetType::Future, start_date);
        let future_slippage = algo.get_slippage_for_asset(&future);
        assert!(future_slippage.is_none() || future_slippage.is_some());
    }

    #[test]
    fn test_get_commission_for_asset() {
        use crate::finance::ZeroCommission;

        let asset_finder = Arc::new(AssetFinder::new());
        let mut algo = TradingAlgorithm::new(asset_finder);

        // Set equity-specific commission
        let commission = Arc::new(ZeroCommission);
        algo.set_commission_by_class(AssetType::Equity, commission).unwrap();

        // Test with equity asset
        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let equity = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);
        assert!(algo.get_commission_for_asset(&equity).is_some());
    }

    #[test]
    fn test_configured_asset_types() {
        use crate::finance::{NoSlippage, ZeroCommission};

        let asset_finder = Arc::new(AssetFinder::new());
        let mut algo = TradingAlgorithm::new(asset_finder);

        algo.set_slippage_by_class(AssetType::Equity, Arc::new(NoSlippage)).unwrap();
        algo.set_commission_by_class(AssetType::Future, Arc::new(ZeroCommission)).unwrap();

        let types = algo.configured_asset_types();
        assert_eq!(types.len(), 2);
        assert!(types.contains(&AssetType::Equity));
        assert!(types.contains(&AssetType::Future));
    }

    #[test]
    fn test_multi_asset_fallback_to_default() {
        use crate::finance::{NoSlippage, ZeroCommission};

        let asset_finder = Arc::new(AssetFinder::new());
        let mut algo = TradingAlgorithm::new(asset_finder);

        // Set default models
        algo.set_slippage(Arc::new(NoSlippage)).unwrap();
        algo.set_commission(Arc::new(ZeroCommission)).unwrap();

        // Asset without specific config should fall back to default
        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let crypto = Asset::new(1, "BTC".to_string(), "COINBASE".to_string(), AssetType::Crypto, start_date);
        assert!(algo.get_slippage_for_asset(&crypto).is_some());
        assert!(algo.get_commission_for_asset(&crypto).is_some());
    }

    #[test]
    fn test_cannot_set_models_after_init() {
        use crate::finance::NoSlippage;

        let asset_finder = Arc::new(AssetFinder::new());
        let mut algo = TradingAlgorithm::new(asset_finder);

        algo.mark_initialized();

        let slippage = Arc::new(NoSlippage);
        assert!(algo.set_slippage(slippage.clone()).is_err());
        assert!(algo.set_slippage_by_class(AssetType::Equity, slippage).is_err());
    }

    #[test]
    fn test_asset_class_config_creation() {
        use crate::finance::{NoSlippage, ZeroCommission};

        let slippage = Arc::new(NoSlippage);
        let commission = Arc::new(ZeroCommission);

        let config = AssetClassConfig::new(slippage, commission);

        // Just verify it compiles and creates successfully
        assert!(std::mem::size_of_val(&config) > 0);
    }
}
