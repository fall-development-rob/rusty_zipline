//! Pipeline Term - Core computational expression system
//!
//! Terms represent computational nodes in the pipeline dependency graph.
//! They form the building blocks for factor calculations.

use crate::error::{Result, ZiplineError};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt;

/// Unique identifier for a term in the computational graph
pub type TermId = u64;

/// Data types that can be computed by terms
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DType {
    /// Boolean values
    Bool,
    /// 32-bit integer
    Int32,
    /// 64-bit integer
    Int64,
    /// 32-bit float
    Float32,
    /// 64-bit float
    Float64,
    /// String values
    String,
    /// DateTime values
    DateTime,
    /// Object (arbitrary type)
    Object,
}

impl DType {
    /// Size in bytes for this data type
    pub fn size_bytes(&self) -> usize {
        match self {
            DType::Bool => 1,
            DType::Int32 => 4,
            DType::Int64 => 8,
            DType::Float32 => 4,
            DType::Float64 => 8,
            DType::String => 8, // pointer size
            DType::DateTime => 12, // timestamp
            DType::Object => 8, // pointer size
        }
    }

    /// Whether this dtype can be compared
    pub fn is_comparable(&self) -> bool {
        matches!(
            self,
            DType::Bool
                | DType::Int32
                | DType::Int64
                | DType::Float32
                | DType::Float64
                | DType::DateTime
        )
    }

    /// Whether this dtype supports arithmetic
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            DType::Int32 | DType::Int64 | DType::Float32 | DType::Float64
        )
    }
}

impl fmt::Display for DType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DType::Bool => write!(f, "bool"),
            DType::Int32 => write!(f, "int32"),
            DType::Int64 => write!(f, "int64"),
            DType::Float32 => write!(f, "float32"),
            DType::Float64 => write!(f, "float64"),
            DType::String => write!(f, "string"),
            DType::DateTime => write!(f, "datetime"),
            DType::Object => write!(f, "object"),
        }
    }
}

/// Dimensionality of term output
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NDim {
    /// Scalar (single value)
    Scalar,
    /// 1D array (per-asset)
    Array1D,
    /// 2D array (per-asset, per-date)
    Array2D,
}

/// Trait for all pipeline terms
pub trait Term: Send + Sync {
    /// Unique identifier for this term
    fn id(&self) -> TermId;

    /// Data type of this term's output
    fn dtype(&self) -> DType;

    /// Dimensionality of output
    fn ndim(&self) -> NDim;

    /// Dependencies - other terms this term depends on
    fn dependencies(&self) -> Vec<TermId>;

    /// Window length required for computation
    fn window_length(&self) -> usize {
        1
    }

    /// Whether this term's output should be cached
    fn cacheable(&self) -> bool {
        true
    }

    /// Human-readable name for debugging
    fn name(&self) -> String;

    /// Type-erased self reference
    fn as_any(&self) -> &dyn Any;
}

/// Base term implementation
#[derive(Debug, Clone)]
pub struct BaseTerm {
    pub id: TermId,
    pub dtype: DType,
    pub ndim: NDim,
    pub dependencies: Vec<TermId>,
    pub window_length: usize,
    pub cacheable: bool,
    pub name: String,
}

impl BaseTerm {
    pub fn new(
        id: TermId,
        dtype: DType,
        ndim: NDim,
        name: impl Into<String>,
    ) -> Self {
        Self {
            id,
            dtype,
            ndim,
            dependencies: Vec::new(),
            window_length: 1,
            cacheable: true,
            name: name.into(),
        }
    }

    pub fn with_dependencies(mut self, deps: Vec<TermId>) -> Self {
        self.dependencies = deps;
        self
    }

    pub fn with_window_length(mut self, length: usize) -> Self {
        self.window_length = length;
        self
    }

    pub fn with_cacheable(mut self, cacheable: bool) -> Self {
        self.cacheable = cacheable;
        self
    }
}

impl Term for BaseTerm {
    fn id(&self) -> TermId {
        self.id
    }

    fn dtype(&self) -> DType {
        self.dtype
    }

    fn ndim(&self) -> NDim {
        self.ndim
    }

    fn dependencies(&self) -> Vec<TermId> {
        self.dependencies.clone()
    }

    fn window_length(&self) -> usize {
        self.window_length
    }

    fn cacheable(&self) -> bool {
        self.cacheable
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Binary operation term
/// Corresponds to Python Zipline's comparison and arithmetic operators:
/// - Add: __add__
/// - Subtract: __sub__
/// - Multiply: __mul__
/// - Divide: __truediv__
/// - Modulo: __mod__
/// - Power: __pow__
/// - Equal: __eq__
/// - NotEqual: __ne__
/// - Less: __lt__
/// - LessEqual: __le__
/// - Greater: __gt__
/// - GreaterEqual: __ge__
/// - And: __and__
/// - Or: __or__
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    /// Addition operator (+)
    Add,
    /// Subtraction operator (-)
    Subtract,
    /// Multiplication operator (*)
    Multiply,
    /// Division operator (/)
    Divide,
    /// Modulo operator (%)
    Modulo,
    /// Power operator (**)
    Power,
    /// Equality comparison (==)
    Equal,
    /// Inequality comparison (!=)
    NotEqual,
    /// Less than comparison (<)
    Less,
    /// Less than or equal comparison (<=)
    LessEqual,
    /// Greater than comparison (>)
    Greater,
    /// Greater than or equal comparison (>=)
    GreaterEqual,
    /// Logical AND
    And,
    /// Logical OR
    Or,
}

impl fmt::Display for BinOp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.python_name())
    }
}

impl BinOp {
    /// Get the Python operator name for this operation
    pub fn python_name(&self) -> &'static str {
        match self {
            BinOp::Add => "__add__",
            BinOp::Subtract => "__sub__",
            BinOp::Multiply => "__mul__",
            BinOp::Divide => "__truediv__",
            BinOp::Modulo => "__mod__",
            BinOp::Power => "__pow__",
            BinOp::Equal => "__eq__",
            BinOp::NotEqual => "__ne__",
            BinOp::Less => "__lt__",
            BinOp::LessEqual => "__le__",
            BinOp::Greater => "__gt__",
            BinOp::GreaterEqual => "__ge__",
            BinOp::And => "__and__",
            BinOp::Or => "__or__",
        }
    }

    /// Check if this is an arithmetic operation
    pub fn is_arithmetic(&self) -> bool {
        matches!(
            self,
            BinOp::Add | BinOp::Subtract | BinOp::Multiply | BinOp::Divide
        )
    }

    /// Check if this is a comparison operation
    pub fn is_comparison(&self) -> bool {
        matches!(
            self,
            BinOp::Equal
                | BinOp::NotEqual
                | BinOp::Less
                | BinOp::LessEqual
                | BinOp::Greater
                | BinOp::GreaterEqual
        )
    }

    /// Check if this is a logical operation
    pub fn is_logical(&self) -> bool {
        matches!(self, BinOp::And | BinOp::Or)
    }

    /// Result dtype for this operation on given input types
    pub fn result_dtype(&self, left: DType, right: DType) -> Result<DType> {
        match self {
            BinOp::Add | BinOp::Subtract | BinOp::Multiply | BinOp::Divide => {
                if !left.is_numeric() || !right.is_numeric() {
                    return Err(ZiplineError::InvalidOperation(format!(
                        "Arithmetic operation requires numeric types, got {} and {}",
                        left, right
                    )));
                }
                // Promote to higher precision
                Ok(match (left, right) {
                    (DType::Float64, _) | (_, DType::Float64) => DType::Float64,
                    (DType::Float32, _) | (_, DType::Float32) => DType::Float32,
                    (DType::Int64, _) | (_, DType::Int64) => DType::Int64,
                    _ => DType::Int32,
                })
            }
            BinOp::Modulo | BinOp::Power => {
                if !left.is_numeric() || !right.is_numeric() {
                    return Err(ZiplineError::InvalidOperation(format!(
                        "Math operation requires numeric types, got {} and {}",
                        left, right
                    )));
                }
                Ok(DType::Float64)
            }
            BinOp::Equal
            | BinOp::NotEqual
            | BinOp::Less
            | BinOp::LessEqual
            | BinOp::Greater
            | BinOp::GreaterEqual => {
                if !left.is_comparable() || !right.is_comparable() {
                    return Err(ZiplineError::InvalidOperation(format!(
                        "Comparison requires comparable types, got {} and {}",
                        left, right
                    )));
                }
                Ok(DType::Bool)
            }
            BinOp::And | BinOp::Or => {
                if left != DType::Bool || right != DType::Bool {
                    return Err(ZiplineError::InvalidOperation(format!(
                        "Logical operation requires bool types, got {} and {}",
                        left, right
                    )));
                }
                Ok(DType::Bool)
            }
        }
    }
}

/// Unary operation term
/// Corresponds to Python Zipline's unary operators:
/// - Negate: __neg__
/// - Not: __invert__ (for bool)
/// - Abs: abs()
/// - Sqrt: sqrt()
/// - Log: log()
/// - Exp: exp()
/// - IsNaN: isnan()
/// - IsNull: isnull()
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    /// Negation operator (-)
    Negate,
    /// Logical NOT operator
    Not,
    /// Absolute value
    Abs,
    /// Square root
    Sqrt,
    /// Natural logarithm
    Log,
    /// Exponential function
    Exp,
    /// Check if value is NaN
    IsNaN,
    /// Check if value is null/missing
    IsNull,
}

impl fmt::Display for UnaryOp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let name = match self {
            UnaryOp::Negate => "__neg__",
            UnaryOp::Not => "__invert__",
            UnaryOp::Abs => "abs",
            UnaryOp::Sqrt => "sqrt",
            UnaryOp::Log => "log",
            UnaryOp::Exp => "exp",
            UnaryOp::IsNaN => "isnan",
            UnaryOp::IsNull => "isnull",
        };
        write!(f, "{}", name)
    }
}

impl UnaryOp {
    /// Result dtype for this operation on given input type
    pub fn result_dtype(&self, input: DType) -> Result<DType> {
        match self {
            UnaryOp::Negate | UnaryOp::Abs => {
                if !input.is_numeric() {
                    return Err(ZiplineError::InvalidOperation(format!(
                        "Negation requires numeric type, got {}",
                        input
                    )));
                }
                Ok(input)
            }
            UnaryOp::Not => {
                if input != DType::Bool {
                    return Err(ZiplineError::InvalidOperation(format!(
                        "Not operation requires bool type, got {}",
                        input
                    )));
                }
                Ok(DType::Bool)
            }
            UnaryOp::Sqrt | UnaryOp::Log | UnaryOp::Exp => {
                if !input.is_numeric() {
                    return Err(ZiplineError::InvalidOperation(format!(
                        "Math operation requires numeric type, got {}",
                        input
                    )));
                }
                Ok(DType::Float64)
            }
            UnaryOp::IsNaN | UnaryOp::IsNull => Ok(DType::Bool),
        }
    }
}

/// Term representing a binary operation
#[derive(Debug, Clone)]
pub struct BinaryOpTerm {
    base: BaseTerm,
    op: BinOp,
    left: TermId,
    right: TermId,
}

impl BinaryOpTerm {
    pub fn new(
        id: TermId,
        op: BinOp,
        left: TermId,
        right: TermId,
        left_dtype: DType,
        right_dtype: DType,
    ) -> Result<Self> {
        let dtype = op.result_dtype(left_dtype, right_dtype)?;
        Ok(Self {
            base: BaseTerm::new(id, dtype, NDim::Array2D, format!("BinOp({})", op))
                .with_dependencies(vec![left, right]),
            op,
            left,
            right,
        })
    }

    pub fn op(&self) -> BinOp {
        self.op
    }

    pub fn left(&self) -> TermId {
        self.left
    }

    pub fn right(&self) -> TermId {
        self.right
    }
}

impl Term for BinaryOpTerm {
    fn id(&self) -> TermId {
        self.base.id()
    }
    fn dtype(&self) -> DType {
        self.base.dtype()
    }
    fn ndim(&self) -> NDim {
        self.base.ndim()
    }
    fn dependencies(&self) -> Vec<TermId> {
        self.base.dependencies()
    }
    fn window_length(&self) -> usize {
        self.base.window_length()
    }
    fn cacheable(&self) -> bool {
        self.base.cacheable()
    }
    fn name(&self) -> String {
        self.base.name()
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Term representing a unary operation
#[derive(Debug, Clone)]
pub struct UnaryOpTerm {
    base: BaseTerm,
    op: UnaryOp,
    input: TermId,
}

impl UnaryOpTerm {
    pub fn new(id: TermId, op: UnaryOp, input: TermId, input_dtype: DType) -> Result<Self> {
        let dtype = op.result_dtype(input_dtype)?;
        Ok(Self {
            base: BaseTerm::new(id, dtype, NDim::Array2D, format!("UnaryOp({})", op))
                .with_dependencies(vec![input]),
            op,
            input,
        })
    }

    pub fn op(&self) -> UnaryOp {
        self.op
    }

    pub fn input(&self) -> TermId {
        self.input
    }
}

impl Term for UnaryOpTerm {
    fn id(&self) -> TermId {
        self.base.id()
    }
    fn dtype(&self) -> DType {
        self.base.dtype()
    }
    fn ndim(&self) -> NDim {
        self.base.ndim()
    }
    fn dependencies(&self) -> Vec<TermId> {
        self.base.dependencies()
    }
    fn window_length(&self) -> usize {
        self.base.window_length()
    }
    fn cacheable(&self) -> bool {
        self.base.cacheable()
    }
    fn name(&self) -> String {
        self.base.name()
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dtype_properties() {
        assert!(DType::Float64.is_numeric());
        assert!(DType::Int32.is_numeric());
        assert!(!DType::Bool.is_numeric());
        assert!(!DType::String.is_numeric());

        assert!(DType::Int64.is_comparable());
        assert!(DType::DateTime.is_comparable());
        assert!(!DType::String.is_comparable());
    }

    #[test]
    fn test_binop_result_dtype() {
        // Arithmetic operations
        let add_result = BinOp::Add.result_dtype(DType::Int32, DType::Float64);
        assert_eq!(add_result.unwrap(), DType::Float64);

        let mul_result = BinOp::Multiply.result_dtype(DType::Float32, DType::Int64);
        assert_eq!(mul_result.unwrap(), DType::Float32);

        // Comparison operations
        let cmp_result = BinOp::Less.result_dtype(DType::Int32, DType::Float64);
        assert_eq!(cmp_result.unwrap(), DType::Bool);

        // Logical operations
        let and_result = BinOp::And.result_dtype(DType::Bool, DType::Bool);
        assert_eq!(and_result.unwrap(), DType::Bool);
    }

    #[test]
    fn test_binop_type_errors() {
        // Can't do arithmetic on non-numeric types
        let err = BinOp::Add.result_dtype(DType::String, DType::Int32);
        assert!(err.is_err());

        // Can't do logical ops on non-bool
        let err = BinOp::And.result_dtype(DType::Int32, DType::Bool);
        assert!(err.is_err());
    }

    #[test]
    fn test_unary_op_result_dtype() {
        let neg_result = UnaryOp::Negate.result_dtype(DType::Float64);
        assert_eq!(neg_result.unwrap(), DType::Float64);

        let sqrt_result = UnaryOp::Sqrt.result_dtype(DType::Int32);
        assert_eq!(sqrt_result.unwrap(), DType::Float64);

        let not_result = UnaryOp::Not.result_dtype(DType::Bool);
        assert_eq!(not_result.unwrap(), DType::Bool);

        let isnan_result = UnaryOp::IsNaN.result_dtype(DType::Float64);
        assert_eq!(isnan_result.unwrap(), DType::Bool);
    }

    #[test]
    fn test_base_term() {
        let term = BaseTerm::new(1, DType::Float64, NDim::Array2D, "test_factor")
            .with_dependencies(vec![2, 3])
            .with_window_length(20)
            .with_cacheable(true);

        assert_eq!(term.id(), 1);
        assert_eq!(term.dtype(), DType::Float64);
        assert_eq!(term.ndim(), NDim::Array2D);
        assert_eq!(term.dependencies(), vec![2, 3]);
        assert_eq!(term.window_length(), 20);
        assert!(term.cacheable());
        assert_eq!(term.name(), "test_factor");
    }

    #[test]
    fn test_binary_op_term() {
        let term = BinaryOpTerm::new(1, BinOp::Add, 2, 3, DType::Float32, DType::Float64).unwrap();

        assert_eq!(term.id(), 1);
        assert_eq!(term.dtype(), DType::Float64); // Promoted to higher precision
        assert_eq!(term.op(), BinOp::Add);
        assert_eq!(term.left(), 2);
        assert_eq!(term.right(), 3);
        assert_eq!(term.dependencies(), vec![2, 3]);
    }

    #[test]
    fn test_unary_op_term() {
        let term = UnaryOpTerm::new(1, UnaryOp::Sqrt, 2, DType::Int32).unwrap();

        assert_eq!(term.id(), 1);
        assert_eq!(term.dtype(), DType::Float64);
        assert_eq!(term.op(), UnaryOp::Sqrt);
        assert_eq!(term.input(), 2);
        assert_eq!(term.dependencies(), vec![2]);
    }

    #[test]
    fn test_binop_python_names() {
        // Test that all comparison operations map to correct Python names
        assert_eq!(BinOp::Equal.python_name(), "__eq__");
        assert_eq!(BinOp::NotEqual.python_name(), "__ne__");
        assert_eq!(BinOp::Less.python_name(), "__lt__");
        assert_eq!(BinOp::LessEqual.python_name(), "__le__");
        assert_eq!(BinOp::Greater.python_name(), "__gt__");
        assert_eq!(BinOp::GreaterEqual.python_name(), "__ge__");

        // Test arithmetic operations
        assert_eq!(BinOp::Add.python_name(), "__add__");
        assert_eq!(BinOp::Subtract.python_name(), "__sub__");
        assert_eq!(BinOp::Multiply.python_name(), "__mul__");
        assert_eq!(BinOp::Divide.python_name(), "__truediv__");
        assert_eq!(BinOp::Modulo.python_name(), "__mod__");
        assert_eq!(BinOp::Power.python_name(), "__pow__");

        // Test logical operations
        assert_eq!(BinOp::And.python_name(), "__and__");
        assert_eq!(BinOp::Or.python_name(), "__or__");
    }

    #[test]
    fn test_binop_display() {
        // Display should show Python operator names
        assert_eq!(format!("{}", BinOp::Equal), "__eq__");
        assert_eq!(format!("{}", BinOp::Multiply), "__mul__");
        assert_eq!(format!("{}", BinOp::Less), "__lt__");
    }

    #[test]
    fn test_binop_categories() {
        // Arithmetic operations
        assert!(BinOp::Add.is_arithmetic());
        assert!(BinOp::Multiply.is_arithmetic());
        assert!(!BinOp::Equal.is_arithmetic());
        assert!(!BinOp::And.is_arithmetic());

        // Comparison operations
        assert!(BinOp::Equal.is_comparison());
        assert!(BinOp::Less.is_comparison());
        assert!(BinOp::GreaterEqual.is_comparison());
        assert!(!BinOp::Add.is_comparison());
        assert!(!BinOp::And.is_comparison());

        // Logical operations
        assert!(BinOp::And.is_logical());
        assert!(BinOp::Or.is_logical());
        assert!(!BinOp::Equal.is_logical());
        assert!(!BinOp::Add.is_logical());
    }

    #[test]
    fn test_unary_op_display() {
        assert_eq!(format!("{}", UnaryOp::Negate), "__neg__");
        assert_eq!(format!("{}", UnaryOp::Not), "__invert__");
        assert_eq!(format!("{}", UnaryOp::Abs), "abs");
        assert_eq!(format!("{}", UnaryOp::Sqrt), "sqrt");
        assert_eq!(format!("{}", UnaryOp::IsNaN), "isnan");
    }

    #[test]
    fn test_multiply_type_compatibility() {
        // Test that multiplication works with compatible numeric types
        let result = BinOp::Multiply.result_dtype(DType::Float64, DType::Float64);
        assert_eq!(result.unwrap(), DType::Float64);

        let result = BinOp::Multiply.result_dtype(DType::Int32, DType::Float64);
        assert_eq!(result.unwrap(), DType::Float64);

        let result = BinOp::Multiply.result_dtype(DType::Float32, DType::Int64);
        assert_eq!(result.unwrap(), DType::Float32);

        // Test that multiplication fails with non-numeric types
        let result = BinOp::Multiply.result_dtype(DType::String, DType::Float64);
        assert!(result.is_err());

        let result = BinOp::Multiply.result_dtype(DType::Bool, DType::Int32);
        assert!(result.is_err());
    }

    #[test]
    fn test_comparison_all_variants() {
        // Ensure all six comparison operators work correctly
        let ops = vec![
            BinOp::Equal,
            BinOp::NotEqual,
            BinOp::Less,
            BinOp::LessEqual,
            BinOp::Greater,
            BinOp::GreaterEqual,
        ];

        for op in ops {
            // All comparison ops should return Bool
            let result = op.result_dtype(DType::Int32, DType::Float64);
            assert_eq!(result.unwrap(), DType::Bool);

            // All should be categorized as comparison
            assert!(op.is_comparison());
            assert!(!op.is_arithmetic());
            assert!(!op.is_logical());
        }
    }

    #[test]
    fn test_term_names_with_operators() {
        // Test that term names include operator display
        let bin_term = BinaryOpTerm::new(1, BinOp::Add, 2, 3, DType::Float32, DType::Float64).unwrap();
        assert!(bin_term.name().contains("__add__"));

        let unary_term = UnaryOpTerm::new(1, UnaryOp::Sqrt, 2, DType::Int32).unwrap();
        assert!(unary_term.name().contains("sqrt"));
    }

    #[test]
    fn test_type_promotion_multiply() {
        // Test type promotion for multiplication matches arithmetic rules

        // Float64 has highest priority
        assert_eq!(
            BinOp::Multiply.result_dtype(DType::Float64, DType::Int32).unwrap(),
            DType::Float64
        );
        assert_eq!(
            BinOp::Multiply.result_dtype(DType::Float32, DType::Float64).unwrap(),
            DType::Float64
        );

        // Float32 beats integers
        assert_eq!(
            BinOp::Multiply.result_dtype(DType::Float32, DType::Int64).unwrap(),
            DType::Float32
        );

        // Int64 beats Int32
        assert_eq!(
            BinOp::Multiply.result_dtype(DType::Int64, DType::Int32).unwrap(),
            DType::Int64
        );

        // Same types stay the same
        assert_eq!(
            BinOp::Multiply.result_dtype(DType::Int32, DType::Int32).unwrap(),
            DType::Int32
        );
    }
}
