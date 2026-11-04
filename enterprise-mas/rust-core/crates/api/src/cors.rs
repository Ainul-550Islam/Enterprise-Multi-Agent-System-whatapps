//! Explicit browser cross-origin policy. CORS is not authentication or CSRF
//! protection; requests still require the normal authorization middleware.

use std::collections::BTreeSet;

use axum::http::{header, HeaderName, HeaderValue, Method};
use tower_http::cors::CorsLayer;
use url::{Host, Url};

const MAX_ORIGINS: usize = 32;

/// Static operator-owned origin allowlist. Empty disables cross-origin access.
#[derive(Debug, Clone, Default)]
pub struct CorsPolicy {
    origins: Vec<HeaderValue>,
}

impl CorsPolicy {
    /// Validates canonical exact web origins at startup. Plain HTTP is
    /// accepted only for loopback hosts in the explicitly enabled dev lane.
    /// Wildcards, opaque origins, credentials, paths, queries and fragments are
    /// never accepted. No request or tenant may expand this list.
    pub fn new(origins: &[String], allow_loopback_http: bool) -> Result<Self, String> {
        if origins.len() > MAX_ORIGINS {
            return Err("too many CORS origins".to_owned());
        }
        let mut checked = BTreeSet::new();
        for value in origins {
            if value.trim() != value
                || value.is_empty()
                || value.len() > 2048
                || value.contains('*')
            {
                return Err("invalid CORS origin".to_owned());
            }
            let url = Url::parse(value).map_err(|_| "invalid CORS origin".to_owned())?;
            let host = url
                .host()
                .ok_or_else(|| "CORS origin requires a host".to_owned())?;
            let loopback = match host {
                Host::Domain(name) => name.eq_ignore_ascii_case("localhost"),
                Host::Ipv4(ip) => ip.is_loopback(),
                Host::Ipv6(ip) => ip.is_loopback(),
            };
            if !(url.scheme() == "https"
                || (url.scheme() == "http" && allow_loopback_http && loopback))
                || !url.username().is_empty()
                || url.password().is_some()
                || url.path() != "/"
                || url.query().is_some()
                || url.fragment().is_some()
            {
                return Err(
                    "CORS origin must be an exact HTTPS origin without credentials or path"
                        .to_owned(),
                );
            }
            let origin = url.origin().ascii_serialization();
            if origin == "null" || (value != &origin && value != &format!("{origin}/")) {
                return Err("CORS origin must use canonical origin serialization".to_owned());
            }
            checked.insert(origin);
        }
        let origins = checked
            .into_iter()
            .map(|origin| {
                HeaderValue::from_str(&origin).map_err(|_| "invalid CORS origin header".to_owned())
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { origins })
    }

    /// Layer installed outside auth so browsers can preflight without a bearer
    /// token. It never enables cookies/credentials or wildcard origins.
    pub fn layer(&self) -> CorsLayer {
        CorsLayer::new()
            .allow_origin(self.origins.clone())
            .allow_methods([
                Method::GET,
                Method::HEAD,
                Method::POST,
                Method::PUT,
                Method::PATCH,
                Method::DELETE,
            ])
            .allow_headers([
                header::ACCEPT,
                header::AUTHORIZATION,
                header::CONTENT_TYPE,
                HeaderName::from_static("x-tenant-id"),
                HeaderName::from_static("x-organization-id"),
                HeaderName::from_static("x-project-id"),
                HeaderName::from_static("x-correlation-id"),
                HeaderName::from_static("idempotency-key"),
                HeaderName::from_static("last-event-id"),
            ])
            .expose_headers([
                HeaderName::from_static("x-correlation-id"),
                HeaderName::from_static("x-idempotent-replay"),
            ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{router::router, state::test_support::inmemory_state};
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    #[test]
    fn policy_rejects_unsafe_or_noncanonical_inputs() {
        for origin in [
            "*",
            "null",
            "https://*.example.com",
            "http://example.com",
            "https://user@example.com",
            "https://example.com/path",
            "https://example.com/a/..",
            "https://example.com?x=1",
            "https://example.com#x",
            "https://example.com:443",
            "HTTPS://EXAMPLE.COM",
            "https://example.com\n",
            "https://example.com,https://evil.test",
        ] {
            assert!(
                CorsPolicy::new(&[origin.to_owned()], false).is_err(),
                "accepted {origin:?}"
            );
        }
        assert!(CorsPolicy::new(&vec!["https://example.com".to_owned(); 33], false).is_err());
        assert!(CorsPolicy::new(&["http://localhost:3000".into()], false).is_err());
        assert!(CorsPolicy::new(
            &["http://localhost:3000".into(), "http://[::1]:3000".into()],
            true
        )
        .is_ok());
        assert!(CorsPolicy::new(&["http://localhost.evil.test".into()], true).is_err());
        let policy = CorsPolicy::new(
            &["https://example.com/".into(), "https://example.com".into()],
            false,
        )
        .unwrap();
        assert_eq!(
            policy.origins,
            vec![HeaderValue::from_static("https://example.com")]
        );
    }

    fn preflight(origin: &str, method: &str, headers: &str) -> Request<Body> {
        Request::builder()
            .method(Method::OPTIONS)
            .uri("/v1/projects")
            .header(header::ORIGIN, origin)
            .header(header::ACCESS_CONTROL_REQUEST_METHOD, method)
            .header(header::ACCESS_CONTROL_REQUEST_HEADERS, headers)
            .body(Body::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn router_preflights_without_auth_but_actual_requests_still_require_it() {
        let app = router(
            inmemory_state()
                .with_cors(CorsPolicy::new(&["https://ui.example.com".into()], false).unwrap()),
        );
        let response = app
            .clone()
            .oneshot(preflight(
                "https://ui.example.com",
                "POST",
                "authorization,content-type,x-project-id,idempotency-key",
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let h = response.headers();
        assert_eq!(
            h[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://ui.example.com"
        );
        assert!(h[header::ACCESS_CONTROL_ALLOW_METHODS]
            .to_str()
            .unwrap()
            .contains("POST"));
        for name in [
            "authorization",
            "content-type",
            "x-project-id",
            "idempotency-key",
        ] {
            assert!(h[header::ACCESS_CONTROL_ALLOW_HEADERS]
                .to_str()
                .unwrap()
                .split(',')
                .any(|x| x.trim().eq_ignore_ascii_case(name)));
        }
        assert!(!h.contains_key(header::ACCESS_CONTROL_ALLOW_CREDENTIALS));
        assert!(!h.contains_key("access-control-allow-private-network"));
        let vary = h
            .get_all(header::VARY)
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect::<Vec<_>>()
            .join(",")
            .to_ascii_lowercase();
        for field in [
            "origin",
            "access-control-request-method",
            "access-control-request-headers",
        ] {
            assert!(vary.contains(field), "missing {field}: {vary}");
        }
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/projects")
                    .header(header::ORIGIN, "https://ui.example.com")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://ui.example.com"
        );
        assert!(!response
            .headers()
            .contains_key(header::ACCESS_CONTROL_ALLOW_CREDENTIALS));
        let exposed = response.headers()[header::ACCESS_CONTROL_EXPOSE_HEADERS]
            .to_str()
            .unwrap();
        assert!(exposed.contains("x-idempotent-replay"));
    }

    #[tokio::test]
    async fn origins_methods_headers_and_default_fail_closed() {
        let app = router(
            inmemory_state()
                .with_cors(CorsPolicy::new(&["https://ui.example.com".into()], false).unwrap()),
        );
        for origin in [
            "https://evil.test",
            "https://ui.example.com.evil.test",
            "null",
            "http://ui.example.com",
        ] {
            let response = app
                .clone()
                .oneshot(preflight(origin, "POST", "authorization"))
                .await
                .unwrap();
            assert!(!response
                .headers()
                .contains_key(header::ACCESS_CONTROL_ALLOW_ORIGIN));
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri("/v1/health/live")
                        .header(header::ORIGIN, origin)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert!(!response
                .headers()
                .contains_key(header::ACCESS_CONTROL_ALLOW_ORIGIN));
        }
        let response = app
            .clone()
            .oneshot(preflight("https://ui.example.com", "TRACE", "x-evil"))
            .await
            .unwrap();
        assert!(!response.headers()[header::ACCESS_CONTROL_ALLOW_METHODS]
            .to_str()
            .unwrap()
            .contains("TRACE"));
        assert!(!response.headers()[header::ACCESS_CONTROL_ALLOW_HEADERS]
            .to_str()
            .unwrap()
            .contains("x-evil"));
        let response = router(inmemory_state())
            .oneshot(preflight("https://ui.example.com", "GET", "authorization"))
            .await
            .unwrap();
        assert!(!response
            .headers()
            .contains_key(header::ACCESS_CONTROL_ALLOW_ORIGIN));
    }
}
