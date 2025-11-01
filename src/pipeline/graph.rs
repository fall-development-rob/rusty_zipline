//! Pipeline Graph - Computational dependency graph
//!
//! Manages dependencies between terms and optimizes execution order.

use crate::error::{Result, ZiplineError};
use crate::pipeline::term::{Term, TermId};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

/// Computational dependency graph for pipeline execution
#[derive(Debug, Clone)]
pub struct Graph {
    /// All terms in the graph
    terms: HashMap<TermId, Arc<dyn Term>>,
    /// Dependencies: term_id -> [dependency_ids]
    dependencies: HashMap<TermId, Vec<TermId>>,
    /// Reverse dependencies: term_id -> [dependent_ids]
    dependents: HashMap<TermId, Vec<TermId>>,
    /// Execution order (topologically sorted)
    execution_order: Vec<TermId>,
}

impl Graph {
    /// Create a new empty graph
    pub fn new() -> Self {
        Self {
            terms: HashMap::new(),
            dependencies: HashMap::new(),
            dependents: HashMap::new(),
            execution_order: Vec::new(),
        }
    }

    /// Add a term to the graph
    pub fn add_term(&mut self, term: Arc<dyn Term>) -> Result<()> {
        let term_id = term.id();

        if self.terms.contains_key(&term_id) {
            return Err(ZiplineError::InvalidOperation(format!(
                "Term {} already exists in graph",
                term_id
            )));
        }

        let deps = term.dependencies();

        // Verify all dependencies exist
        for dep_id in &deps {
            if !self.terms.contains_key(dep_id) {
                return Err(ZiplineError::InvalidOperation(format!(
                    "Dependency {} not found for term {}",
                    dep_id, term_id
                )));
            }
        }

        // Add term
        self.terms.insert(term_id, term);

        // Update dependencies
        self.dependencies.insert(term_id, deps.clone());

        // Update reverse dependencies
        for dep_id in deps {
            self.dependents
                .entry(dep_id)
                .or_insert_with(Vec::new)
                .push(term_id);
        }

        // Invalidate execution order (will be recomputed)
        self.execution_order.clear();

        Ok(())
    }

    /// Get a term by ID
    pub fn get_term(&self, id: TermId) -> Option<Arc<dyn Term>> {
        self.terms.get(&id).cloned()
    }

    /// Get dependencies of a term
    pub fn dependencies_of(&self, id: TermId) -> Option<&[TermId]> {
        self.dependencies.get(&id).map(|v| v.as_slice())
    }

    /// Get dependents of a term
    pub fn dependents_of(&self, id: TermId) -> Option<&[TermId]> {
        self.dependents.get(&id).map(|v| v.as_slice())
    }

    /// Check if the graph contains cycles
    pub fn has_cycle(&self) -> bool {
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();

        for term_id in self.terms.keys() {
            if self.has_cycle_util(*term_id, &mut visited, &mut rec_stack) {
                return true;
            }
        }

        false
    }

    fn has_cycle_util(
        &self,
        term_id: TermId,
        visited: &mut HashSet<TermId>,
        rec_stack: &mut HashSet<TermId>,
    ) -> bool {
        if rec_stack.contains(&term_id) {
            return true;
        }

        if visited.contains(&term_id) {
            return false;
        }

        visited.insert(term_id);
        rec_stack.insert(term_id);

        if let Some(deps) = self.dependencies.get(&term_id) {
            for dep_id in deps {
                if self.has_cycle_util(*dep_id, visited, rec_stack) {
                    return true;
                }
            }
        }

        rec_stack.remove(&term_id);
        false
    }

    /// Compute topological sort (execution order)
    pub fn topological_sort(&mut self) -> Result<&[TermId]> {
        if !self.execution_order.is_empty() {
            return Ok(&self.execution_order);
        }

        if self.has_cycle() {
            return Err(ZiplineError::InvalidOperation(
                "Graph contains cycles - cannot compute execution order".to_string(),
            ));
        }

        // Kahn's algorithm for topological sort
        let mut in_degree: HashMap<TermId, usize> = HashMap::new();
        let mut queue: VecDeque<TermId> = VecDeque::new();
        let mut result: Vec<TermId> = Vec::new();

        // Calculate in-degrees
        for term_id in self.terms.keys() {
            in_degree.insert(*term_id, 0);
        }

        for deps in self.dependencies.values() {
            for dep_id in deps {
                *in_degree.get_mut(dep_id).unwrap() += 1;
            }
        }

        // Add all nodes with in-degree 0 to queue
        for (term_id, degree) in &in_degree {
            if *degree == 0 {
                queue.push_back(*term_id);
            }
        }

        // Process queue
        while let Some(term_id) = queue.pop_front() {
            result.push(term_id);

            // Reduce in-degree of dependents
            if let Some(deps) = self.dependencies.get(&term_id) {
                for dep_id in deps {
                    let degree = in_degree.get_mut(dep_id).unwrap();
                    *degree -= 1;
                    if *degree == 0 {
                        queue.push_back(*dep_id);
                    }
                }
            }
        }

        if result.len() != self.terms.len() {
            return Err(ZiplineError::InvalidOperation(
                "Failed to compute topological sort".to_string(),
            ));
        }

        // Reverse to get execution order (dependencies first)
        result.reverse();
        self.execution_order = result;

        Ok(&self.execution_order)
    }

    /// Get execution order
    pub fn execution_order(&mut self) -> Result<&[TermId]> {
        self.topological_sort()
    }

    /// Get the maximum window length required
    pub fn max_window_length(&self) -> usize {
        self.terms
            .values()
            .map(|t| t.window_length())
            .max()
            .unwrap_or(1)
    }

    /// Get all leaf terms (terms with no dependents)
    pub fn leaf_terms(&self) -> Vec<TermId> {
        self.terms
            .keys()
            .filter(|id| {
                self.dependents
                    .get(id)
                    .map(|deps| deps.is_empty())
                    .unwrap_or(true)
            })
            .copied()
            .collect()
    }

    /// Get all root terms (terms with no dependencies)
    pub fn root_terms(&self) -> Vec<TermId> {
        self.terms
            .keys()
            .filter(|id| {
                self.dependencies
                    .get(id)
                    .map(|deps| deps.is_empty())
                    .unwrap_or(true)
            })
            .copied()
            .collect()
    }

    /// Number of terms in the graph
    pub fn len(&self) -> usize {
        self.terms.len()
    }

    /// Check if graph is empty
    pub fn is_empty(&self) -> bool {
        self.terms.is_empty()
    }

    /// Clear the graph
    pub fn clear(&mut self) {
        self.terms.clear();
        self.dependencies.clear();
        self.dependents.clear();
        self.execution_order.clear();
    }

    /// Get all term IDs
    pub fn term_ids(&self) -> Vec<TermId> {
        self.terms.keys().copied().collect()
    }

    /// Compute depth of each term (longest path from root)
    pub fn compute_depths(&self) -> HashMap<TermId, usize> {
        let mut depths: HashMap<TermId, usize> = HashMap::new();

        // Initialize all depths to 0
        for term_id in self.terms.keys() {
            depths.insert(*term_id, 0);
        }

        // Compute depths in topological order
        let mut sorted = self.clone();
        if let Ok(order) = sorted.topological_sort() {
            for term_id in order.iter().rev() {
                if let Some(deps) = self.dependencies.get(term_id) {
                    let max_dep_depth = deps
                        .iter()
                        .map(|dep_id| depths.get(dep_id).copied().unwrap_or(0))
                        .max()
                        .unwrap_or(0);
                    depths.insert(*term_id, max_dep_depth + 1);
                }
            }
        }

        depths
    }

    /// Get terms at a specific depth level
    pub fn terms_at_depth(&self, depth: usize) -> Vec<TermId> {
        let depths = self.compute_depths();
        depths
            .iter()
            .filter(|(_, d)| **d == depth)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get maximum depth of the graph
    pub fn max_depth(&self) -> usize {
        self.compute_depths()
            .values()
            .copied()
            .max()
            .unwrap_or(0)
    }
}

impl Default for Graph {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::term::{BaseTerm, DType, NDim};

    fn create_test_term(id: TermId, deps: Vec<TermId>) -> Arc<dyn Term> {
        Arc::new(
            BaseTerm::new(id, DType::Float64, NDim::Array2D, format!("term_{}", id))
                .with_dependencies(deps),
        )
    }

    #[test]
    fn test_graph_add_term() {
        let mut graph = Graph::new();

        // Add root term
        let term1 = create_test_term(1, vec![]);
        graph.add_term(term1).unwrap();

        assert_eq!(graph.len(), 1);
        assert!(graph.get_term(1).is_some());
    }

    #[test]
    fn test_graph_dependencies() {
        let mut graph = Graph::new();

        let term1 = create_test_term(1, vec![]);
        let term2 = create_test_term(2, vec![]);
        let term3 = create_test_term(3, vec![1, 2]);

        graph.add_term(term1).unwrap();
        graph.add_term(term2).unwrap();
        graph.add_term(term3).unwrap();

        assert_eq!(graph.dependencies_of(3).unwrap(), &[1, 2]);
        assert_eq!(graph.dependents_of(1).unwrap(), &[3]);
        assert_eq!(graph.dependents_of(2).unwrap(), &[3]);
    }

    #[test]
    fn test_missing_dependency() {
        let mut graph = Graph::new();

        let term1 = create_test_term(1, vec![999]); // Missing dependency
        let result = graph.add_term(term1);

        assert!(result.is_err());
    }

    #[test]
    fn test_cycle_detection() {
        let mut graph = Graph::new();

        // Create a simple cycle: 1 -> 2 -> 3 -> 1
        // Note: This can't actually be created with our current API
        // since we verify dependencies exist. This tests the cycle detection logic.

        let term1 = create_test_term(1, vec![]);
        let term2 = create_test_term(2, vec![1]);
        graph.add_term(term1).unwrap();
        graph.add_term(term2).unwrap();

        // Manual cycle creation for testing
        graph.dependencies.insert(1, vec![2]);
        graph.dependents.entry(2).or_insert_with(Vec::new).push(1);

        assert!(graph.has_cycle());
    }

    #[test]
    fn test_topological_sort() {
        let mut graph = Graph::new();

        // Build DAG: 1 <- 3 <- 4
        //            2 <-/
        let term1 = create_test_term(1, vec![]);
        let term2 = create_test_term(2, vec![]);
        let term3 = create_test_term(3, vec![1, 2]);
        let term4 = create_test_term(4, vec![3]);

        graph.add_term(term1).unwrap();
        graph.add_term(term2).unwrap();
        graph.add_term(term3).unwrap();
        graph.add_term(term4).unwrap();

        let order = graph.topological_sort().unwrap();

        // Verify dependencies come before dependents
        let pos: HashMap<TermId, usize> = order
            .iter()
            .enumerate()
            .map(|(i, id)| (*id, i))
            .collect();

        assert!(pos[&1] < pos[&3]);
        assert!(pos[&2] < pos[&3]);
        assert!(pos[&3] < pos[&4]);
    }

    #[test]
    fn test_leaf_and_root_terms() {
        let mut graph = Graph::new();

        let term1 = create_test_term(1, vec![]);
        let term2 = create_test_term(2, vec![]);
        let term3 = create_test_term(3, vec![1, 2]);
        let term4 = create_test_term(4, vec![3]);

        graph.add_term(term1).unwrap();
        graph.add_term(term2).unwrap();
        graph.add_term(term3).unwrap();
        graph.add_term(term4).unwrap();

        let roots = graph.root_terms();
        assert_eq!(roots.len(), 2);
        assert!(roots.contains(&1));
        assert!(roots.contains(&2));

        let leaves = graph.leaf_terms();
        assert_eq!(leaves.len(), 1);
        assert!(leaves.contains(&4));
    }

    #[test]
    fn test_max_window_length() {
        let mut graph = Graph::new();

        let term1 = BaseTerm::new(1, DType::Float64, NDim::Array2D, "term1")
            .with_window_length(10);
        let term2 = BaseTerm::new(2, DType::Float64, NDim::Array2D, "term2")
            .with_window_length(20);
        let term3 = BaseTerm::new(3, DType::Float64, NDim::Array2D, "term3")
            .with_window_length(5);

        graph.add_term(Arc::new(term1)).unwrap();
        graph.add_term(Arc::new(term2)).unwrap();
        graph.add_term(Arc::new(term3)).unwrap();

        assert_eq!(graph.max_window_length(), 20);
    }

    #[test]
    fn test_compute_depths() {
        let mut graph = Graph::new();

        // Build DAG with known depths:
        // 1 (depth 0), 2 (depth 0)
        // 3 (depth 1) depends on 1, 2
        // 4 (depth 2) depends on 3

        let term1 = create_test_term(1, vec![]);
        let term2 = create_test_term(2, vec![]);
        let term3 = create_test_term(3, vec![1, 2]);
        let term4 = create_test_term(4, vec![3]);

        graph.add_term(term1).unwrap();
        graph.add_term(term2).unwrap();
        graph.add_term(term3).unwrap();
        graph.add_term(term4).unwrap();

        let depths = graph.compute_depths();

        assert_eq!(depths[&1], 0);
        assert_eq!(depths[&2], 0);
        assert_eq!(depths[&3], 1);
        assert_eq!(depths[&4], 2);
        assert_eq!(graph.max_depth(), 2);
    }

    #[test]
    fn test_terms_at_depth() {
        let mut graph = Graph::new();

        let term1 = create_test_term(1, vec![]);
        let term2 = create_test_term(2, vec![]);
        let term3 = create_test_term(3, vec![1, 2]);

        graph.add_term(term1).unwrap();
        graph.add_term(term2).unwrap();
        graph.add_term(term3).unwrap();

        let depth_0 = graph.terms_at_depth(0);
        assert_eq!(depth_0.len(), 2);
        assert!(depth_0.contains(&1));
        assert!(depth_0.contains(&2));

        let depth_1 = graph.terms_at_depth(1);
        assert_eq!(depth_1.len(), 1);
        assert!(depth_1.contains(&3));
    }

    #[test]
    fn test_graph_clear() {
        let mut graph = Graph::new();

        let term1 = create_test_term(1, vec![]);
        graph.add_term(term1).unwrap();

        assert_eq!(graph.len(), 1);

        graph.clear();

        assert_eq!(graph.len(), 0);
        assert!(graph.is_empty());
    }
}
