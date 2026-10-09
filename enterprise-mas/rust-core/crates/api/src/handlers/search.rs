//! Bounded tenant-scoped command-palette search endpoint.
//!
//! The durable adapter performs parameterized, limited SQL matching under the
//! request transaction. This handler adds the canonical authorization decision
//! before returning only safe resource metadata; it never searches connector
//! configuration, document content, credentials, or execution output.

use axum::extract::rejection::QueryRejection;
use axum::extract::{Query, State};
use mas_contracts::search::{SearchQuery, SearchResponseDto};
use serde::Deserialize;

use crate::handlers::Ctx;
use crate::response::{ok, ApiError, ApiResult};
use crate::state::AppState;

/// HTTP query shape for the palette.
///
/// `serde_urlencoded` does not consistently decode a scalar query parameter
/// into a `Vec<String>` across clients. The public surface therefore accepts
/// `kinds=agents,workflows` (or an empty `kinds=` for every safe kind), then
/// maps it explicitly onto the stable contracts DTO.
#[derive(Debug, Deserialize)]
pub struct SearchParams {
    q: String,
    #[serde(default)]
    kinds: String,
    limit: Option<u16>,
}

impl SearchParams {
    fn into_contract(self) -> SearchQuery {
        let kinds = if self.kinds.is_empty() {
            Vec::new()
        } else {
            self.kinds
                .split(',')
                .map(|kind| kind.trim().to_owned())
                .collect()
        };
        SearchQuery {
            q: self.q,
            kinds,
            limit: self.limit,
        }
    }
}

/// `GET /v1/search?q=&kinds=agents,workflows&limit=`.
pub async fn search(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    params: std::result::Result<Query<SearchParams>, QueryRejection>,
) -> ApiResult<SearchResponseDto> {
    // Connector read is the narrowest resource permission that covers every
    // resource returned by the palette. Requiring it prevents a service
    // identity that may read executions but not integration metadata from
    // learning connector names through this cross-resource endpoint.
    let service_ctx = crate::handlers::authorized_service_context(
        &ctx,
        mas_security::ResourceKind::Connector,
        mas_security::Action::Read,
        None,
    )
    .map_err(|error| ApiError::new(error, &ctx))?;
    let query = params
        .map_err(|_| {
            ApiError::new(
                mas_common::error::AppError::invalid_field(
                    "query",
                    "invalid",
                    "search query parameters are malformed",
                ),
                &ctx,
            )
        })?
        .0
        .into_contract();
    query
        .validate()
        .map_err(|error| ApiError::new(error, &ctx))?;
    state
        .search_service(&ctx)?
        .search(&service_ctx, &query)
        .await
        .map(|payload| ok(&ctx, payload))
        .map_err(|error| ApiError::new(error, &ctx))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn comma_separated_kinds_map_to_the_stable_contract_dto() {
        let query = SearchParams {
            q: "agent".to_owned(),
            kinds: "agents, workflows".to_owned(),
            limit: Some(5),
        }
        .into_contract();
        assert_eq!(query.kinds, vec!["agents", "workflows"]);
        query.validate().expect("mapped kinds are valid");
    }
}
