//! Workflow edges: directed transitions between nodes.

use mas_common::error::AppError;
use mas_common::result::Result;
use serde::{Deserialize, Serialize};

/// A directed edge `from → to` between node keys of the same graph.
///
/// `condition` carries an optional guard expression evaluated at runtime
/// (truthy ⇒ traversal). `priority` orders sibling transitions — lower wins —
/// making branching deterministic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowEdge {
    pub from: String,
    pub to: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,
    #[serde(default)]
    pub priority: u32,
}

impl WorkflowEdge {
    pub fn new(from: impl Into<String>, to: impl Into<String>) -> Result<Self> {
        let edge = Self {
            from: from.into(),
            to: to.into(),
            condition: None,
            priority: 0,
        };
        edge.validate()?;
        Ok(edge)
    }

    #[must_use]
    pub fn with_condition(mut self, condition: impl Into<String>) -> Self {
        self.condition = Some(condition.into());
        self
    }

    #[must_use]
    pub fn with_priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }

    /// Structural validation. Existence of endpoints is verified by the
    /// containing graph (an edge cannot check it alone).
    pub fn validate(&self) -> Result<()> {
        for (field, value) in [("from", &self.from), ("to", &self.to)] {
            if value.is_empty() || value.len() > 128 {
                return Err(AppError::invalid_field(
                    field,
                    "invalid_format",
                    "edge endpoints must be non-empty node keys",
                ));
            }
        }
        if self.from == self.to {
            return Err(AppError::invalid_field(
                "edge",
                "self_loop",
                format!("self-loop on node '{}' is not allowed", self.from),
            ));
        }
        if let Some(condition) = &self.condition {
            if condition.trim().is_empty() || condition.len() > 4096 {
                return Err(AppError::invalid_field(
                    "condition",
                    "invalid_format",
                    "edge conditions must be 1..=4096 characters",
                ));
            }
        }
        Ok(())
    }
}
