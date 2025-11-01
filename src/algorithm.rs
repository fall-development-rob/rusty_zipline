//! Algorithm trait and context for trading strategies

use crate::asset::Asset;
use crate::assets::AssetFinder;
use crate::data::BarData;
use crate::error::{Result, ZiplineError};
use crate::finance::{CommissionModel, Portfolio, SlippageModel};
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
            variables: HashMap::new(),
            pending_orders: Vec::new(),
        }
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
    /// Slippage model
    slippage_model: Option<Arc<dyn SlippageModel>>,
    /// Commission model
    commission_model: Option<Arc<dyn CommissionModel>>,
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
            slippage_model: None,
            commission_model: None,
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
    /// * `PipelineNotFound` - If no pipeline with this name exists
    pub fn pipeline_output(&self, name: &str) -> Result<&HashMap<u64, HashMap<String, f64>>> {
        self.pipeline_outputs
            .get(name)
            .ok_or_else(|| ZiplineError::PipelineNotFound(name.to_string()))
    }

    /// Set the slippage model
    ///
    /// Must be called during initialize().
    ///
    /// # Arguments
    /// * `model` - The slippage model to use
    pub fn set_slippage(&mut self, model: Arc<dyn SlippageModel>) -> Result<()> {
        if self.initialized {
            return Err(ZiplineError::SetSlippagePostInit);
        }
        self.slippage_model = Some(model);
        Ok(())
    }

    /// Set the commission model
    ///
    /// Must be called during initialize().
    ///
    /// # Arguments
    /// * `model` - The commission model to use
    pub fn set_commission(&mut self, model: Arc<dyn CommissionModel>) -> Result<()> {
        if self.initialized {
            return Err(ZiplineError::SetCommissionPostInit);
        }
        self.commission_model = Some(model);
        Ok(())
    }

    /// Set the order cancel policy
    ///
    /// Must be called during initialize().
    ///
    /// # Arguments
    /// * `policy` - The cancel policy to use
    pub fn set_cancel_policy(&mut self, policy: Arc<dyn CancelPolicy>) -> Result<()> {
        if self.initialized {
            return Err(ZiplineError::RegisterTradingControlPostInit(
                "cancel_policy".to_string(),
            ));
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
            return Err(ZiplineError::RegisterTradingControlPostInit(
                "schedule_function".to_string(),
            ));
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

    /// Get slippage model
    pub fn get_slippage_model(&self) -> Option<&Arc<dyn SlippageModel>> {
        self.slippage_model.as_ref()
    }

    /// Get commission model
    pub fn get_commission_model(&self) -> Option<&Arc<dyn CommissionModel>> {
        self.commission_model.as_ref()
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
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string());

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
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string());

        asset_finder.insert_asset(asset.clone()).unwrap();

        let algo = TradingAlgorithm::new(asset_finder.clone());
        let result = algo.symbol("AAPL", None).unwrap();

        assert_eq!(result.symbol, "AAPL");
        assert_eq!(result.id, 1);
    }

    #[test]
    fn test_symbols_batch_lookup() {
        let asset_finder = Arc::new(AssetFinder::new());
        let asset1 = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string());
        let asset2 = Asset::equity(2, "GOOGL".to_string(), "NASDAQ".to_string());

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
        let asset = Asset::equity(42, "TEST".to_string(), "NYSE".to_string());

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
}
