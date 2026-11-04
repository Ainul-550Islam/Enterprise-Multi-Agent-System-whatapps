//! PURPOSE: OpenAPI <-> router parity.
//!
//! Parses `docs/openapi.yaml` and the real route tables in
//! `crates/api/src/router.rs`, `routes_governance.rs` and `routes_tooling.rs`;
//! fails if a router route is missing from the spec, or the spec lists a
//! route the router does not serve (method + path template). Runs in CI as
//! the `openapi-parity` job.
//!
//! It must NOT hand-maintain a second route list: router routes are extracted
//! from the route source files at test time so the spec stays tied to the
//! real route table. Internal `/v1/internal/*` routes are infra-only and
//! excluded from the public API surface.

use std::collections::BTreeSet;
use std::path::PathBuf;

/// One registered route: (method, path-template).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Route {
    method: String,
    path: String,
}

impl Route {}

/// Extract (method, path) route registrations from the axum router source.
///
/// axum registers routes as `.route("/v1/health/live", get(...))` or with
/// chained methods `.route("/v1/agents", post(...).get(...))`. We extract the
/// path literal as the first argument to `.route(` and each HTTP method
/// builder (`get`, `post`, `put`, `patch`, `delete`) chained on it. The
/// method window is taken after the path literal (routes may chain methods
/// across lines, so we scan a bounded window rather than splitting on `)`).
fn router_routes(source: &str) -> BTreeSet<Route> {
    let mut routes = BTreeSet::new();
    let mut remaining = source;
    while let Some(pos) = remaining.find(".route(") {
        let seg = &remaining[pos + ".route(".len()..];
        // Path literal: `"..."` immediately after optional whitespace.
        let path = seg
            .trim_start()
            .strip_prefix('"')
            .and_then(|rest| rest.split('"').next())
            .unwrap_or("")
            .to_string();
        if !path.starts_with("/v1/") {
            remaining = seg;
            continue;
        }
        // Scan after the path for chained method builders, bounded at the
        // next `.route(` so we don't bleed into the following registration.
        // axum may chain `.get().post().put()` across lines.
        let method_window = if let Some(next) = seg.find(".route(") {
            &seg[..next]
        } else {
            seg
        };
        for m in ["get", "post", "put", "patch", "delete"] {
            if method_window.contains(&format!("{m}(")) {
                routes.insert(Route {
                    method: m.to_string(),
                    path: path.clone(),
                });
            }
        }
        remaining = seg;
    }
    routes
}

/// Extract (method, path) pairs documented in the OpenAPI YAML.
/// Path keys are lines like `  /v1/...:`; method keys are indented
/// like `    get:` or `    post:` beneath the nearest path key.
fn spec_routes(yaml: &str) -> BTreeSet<Route> {
    let mut routes = BTreeSet::new();
    let mut current_path: Option<String> = None;
    for line in yaml.lines() {
        let trimmed = line.trim_start();
        if let Some(path) = trimmed.strip_prefix("/v1/") {
            if path.ends_with(':') {
                if let Some(stripped) = path.strip_suffix(':') {
                    current_path = Some(format!("/v1/{}", stripped));
                }
            }
            continue;
        }
        if let Some(method) = trimmed
            .strip_prefix("get:")
            .or_else(|| trimmed.strip_prefix("post:"))
            .or_else(|| trimmed.strip_prefix("put:"))
            .or_else(|| trimmed.strip_prefix("patch:"))
            .or_else(|| trimmed.strip_prefix("delete:"))
        {
            let _ = method;
            if let Some(path) = &current_path {
                let m = trimmed.split(':').next().unwrap_or("");
                routes.insert(Route {
                    method: m.to_string(),
                    path: path.clone(),
                });
            }
        }
    }
    routes
}

fn repo_root() -> PathBuf {
    // enterprise-mas (crate dir is enterprise-mas/rust-core/crates/api)
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .expect("crates/api -> rust-core workspace root")
        .to_path_buf()
}

fn rust_core() -> PathBuf {
    repo_root().join("rust-core")
}

#[test]
fn openapi_matches_router() {
    let rust = rust_core();
    // Router routes live in three source files; merge them all.
    let mut router = BTreeSet::new();
    for file in [
        "crates/api/src/router.rs",
        "crates/api/src/routes_governance.rs",
        "crates/api/src/routes_tooling.rs",
    ] {
        let source =
            std::fs::read_to_string(rust.join(file)).unwrap_or_else(|_| panic!("{file} readable"));
        router.extend(router_routes(&source));
    }
    // Drop internal infra routes (scheduler completion webhook, etc.) —
    // they are not part of the public API surface documented by OpenAPI.
    router.retain(|r| !r.path.starts_with("/v1/internal/"));

    let openapi = std::fs::read_to_string(rust.join("docs/openapi.yaml"))
        .expect("docs/openapi.yaml readable");
    let spec = spec_routes(&openapi);

    let spec_only: Vec<_> = spec.difference(&router).collect();
    let router_only: Vec<_> = router.difference(&spec).collect();

    let mut failures = Vec::new();
    for r in spec_only {
        failures.push(format!(
            "openapi documents route not served by router: {} {}",
            r.method, r.path
        ));
    }
    for r in router_only {
        failures.push(format!(
            "router serves route not documented in openapi: {} {}",
            r.method, r.path
        ));
    }

    if !failures.is_empty() {
        let mut msg = "openapi/router parity failed:\n".to_owned();
        for f in failures {
            msg.push_str(&format!("  - {f}\n"));
        }
        panic!("{msg}");
    }
}

/// Component parameter definitions are mapping keys: a duplicate is ambiguous
/// and many YAML parsers silently keep only the last value. This checks that
/// section independently of the route parity scanner above.
fn duplicate_parameter_names(yaml: &str) -> Vec<String> {
    let mut in_components = false;
    let mut in_parameters = false;
    let mut seen = BTreeSet::new();
    let mut duplicates = Vec::new();
    for line in yaml.lines() {
        if line == "components:" {
            in_components = true;
            continue;
        }
        if in_components && !line.starts_with(' ') && !line.trim().is_empty() {
            break;
        }
        if in_components && line == "  parameters:" {
            in_parameters = true;
            continue;
        }
        if in_parameters
            && line.starts_with("  ")
            && !line.starts_with("    ")
            && !line.trim().is_empty()
        {
            break;
        }
        if in_parameters && line.starts_with("    ") && !line.starts_with("     ") {
            if let Some(name) = line.trim().strip_suffix(':') {
                if !seen.insert(name.to_owned()) {
                    duplicates.push(name.to_owned());
                }
            }
        }
    }
    duplicates
}

#[test]
fn openapi_component_parameters_are_unique() {
    let openapi = std::fs::read_to_string(rust_core().join("docs/openapi.yaml"))
        .expect("docs/openapi.yaml readable");
    assert!(
        openapi.contains("components:\n"),
        "components section missing"
    );
    assert!(
        openapi.contains("  parameters:\n"),
        "parameters section missing"
    );
    assert!(
        duplicate_parameter_names(&openapi).is_empty(),
        "duplicate component parameters: {:?}",
        duplicate_parameter_names(&openapi)
    );
}

#[test]
fn duplicate_parameter_detector_finds_duplicates_only_in_parameter_section() {
    let yaml = "components:\n  parameters:\n    Provider:\n      name: provider\n    Provider:\n      name: provider\n  schemas:\n    Provider:\n      type: string\n";
    assert_eq!(duplicate_parameter_names(yaml), vec!["Provider"]);
}
