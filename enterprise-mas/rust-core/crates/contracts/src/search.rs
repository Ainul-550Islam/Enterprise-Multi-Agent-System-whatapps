//! Tenant-scoped command-palette search DTOs.

use mas_common::result::Result;
use mas_common::validation;
use serde::{Deserialize, Serialize};

/// Cross-resource search query.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SearchQuery {
    /// User-entered query string.
    pub q: String,
    /// Optional resource kinds: agents, workflows, executions, connectors.
    pub kinds: Vec<String>,
    /// Maximum number of ranked results.
    pub limit: Option<u16>,
}

impl SearchQuery {
    /// Validates text, kind names and result bounds.
    pub fn validate(&self) -> Result<()> {
        validation::validate_length("q", &self.q, 1, 256)?;
        if self.q.trim().is_empty() {
            return Err(mas_common::error::AppError::invalid_field(
                "q",
                "required",
                "search query must not be blank",
            ));
        }
        if self.kinds.len() > 4 {
            return Err(mas_common::error::AppError::invalid_field(
                "kinds",
                "too_many",
                "at most four resource kinds may be requested",
            ));
        }
        for kind in &self.kinds {
            match kind.as_str() {
                "agents" | "workflows" | "executions" | "connectors" => {},
                other => {
                    return Err(mas_common::error::AppError::invalid_field(
                        "kinds",
                        "invalid_enum_value",
                        format!("unsupported search kind '{other}'"),
                    ));
                },
            }
        }
        if self.limit.is_some_and(|limit| !(1..=100).contains(&limit)) {
            return Err(mas_common::error::AppError::invalid_field(
                "limit",
                "out_of_range",
                "search result limit must be between 1 and 100",
            ));
        }
        Ok(())
    }
}

/// One safe cross-resource search result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResultDto {
    /// Stable resource type.
    pub kind: String,
    /// Resource identifier encoded as a string to keep this union additive.
    pub id: String,
    /// Display title.
    pub title: String,
    /// Matching preview text; must not contain secrets or document content.
    pub subtitle: Option<String>,
    /// Relevance score computed by the service.
    pub score: f64,
    /// Relative API link.
    pub href: String,
}

/// Bounded search response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResponseDto {
    /// Ranked results.
    pub items: Vec<SearchResultDto>,
    /// Whether additional results were omitted due to the result cap.
    pub truncated: bool,
}
