//! PURPOSE: The bundled OpenAPI document, served at `GET /v1/openapi.json`.
//!
//! `docs/openapi.yaml` is the human-edited source of truth;
//! `docs/openapi.json` is generated from it by
//! `rust-core/scripts/openapi_to_json.py` (see that script's header). The
//! `openapi_parity` integration test fails if the bundle, the YAML, and the
//! live router disagree — regenerate and re-commit after editing the YAML.

/// The OpenAPI document as JSON text, exactly as generated from the YAML.
pub const OPENAPI_JSON: &str = include_str!("../../../docs/openapi.json");
