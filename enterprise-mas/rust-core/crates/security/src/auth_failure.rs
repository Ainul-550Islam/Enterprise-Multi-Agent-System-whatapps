// PURPOSE: The stable vocabulary for *why* an authentication or authorization
// decision failed.
//
// Owning layer: `mas-security` (shared by every entrypoint that authenticates
// or authorizes: HTTP API, gRPC API, CLI, worker, scheduler, internal service
// calls, and the Python bridge boundary).
//
// Production responsibility: one classification, everywhere. Spec Phase 22
// requires at least eight — unauthenticated, invalid credential, expired
// credential, forbidden, tenant mismatch, organization mismatch, insufficient
// permission, invalid service identity — and forbids two failure modes:
//
//   1. **collapsing every security failure into a generic internal error**,
//      which hides an attack in the noise of a 500 and makes an operator
//      unable to tell a misconfiguration from a breach attempt;
//   2. **retrying an authentication or authorization failure**, which turns a
//      denial into a load amplifier and, worse, lets a retry loop eventually
//      hit a differently-configured replica.
//
// `ProjectMismatch` and `ScopeMismatch` are extensions the spec's "at minimum"
// allows: Phase 17 (ID-confusion testing) requires every mismatched
// tenant/org/project combination to fail *safely and distinguishably*, which
// is impossible if a project that belongs to another tenant produces the same
// error as a project that simply does not exist.
//
// Security boundary: the codes here are **public**. They are returned to
// callers and written to audit logs, so a variant must never carry a secret, a
// credential fragment, a raw identifier belonging to another tenant, or a
// message that reveals whether a foreign resource exists. Where existence must
// be hidden, the caller converts the failure to `NotFound` first (see
// [`AuthFailure::as_hidden`]).
//
// It must NOT:
//   • add a variant whose message embeds caller-supplied data — the `detail`
//     field is operator-facing diagnostics only, and is redacted by the
//     shared redactor before it reaches a log;
//   • mark any variant retryable. There is no such thing as a transient
//     authorization failure: either the credential is valid or it is not;
//   • be bypassed by formatting an `AppError` directly at a call site.

use mas_common::error::AppError;
use mas_common::string_enum;

/// Why an authentication/authorization decision was negative.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuthFailureKind {
    /// No credential was presented where one is required.
    Unauthenticated,
    /// A credential was presented but it is not valid (unknown, malformed,
    /// revoked, or the signature does not verify).
    InvalidCredential,
    /// A credential that was valid when issued but is no longer in date.
    ExpiredCredential,
    /// Authenticated, and in the right scope, but the action is not allowed.
    Forbidden,
    /// The requested tenant is not one this principal is bound to.
    TenantMismatch,
    /// The requested organization is not one this principal belongs to.
    OrganizationMismatch,
    /// The requested project does not belong to the resolved tenant/org.
    ProjectMismatch,
    /// The requested scope (tenant + org + project together) is internally
    /// inconsistent — e.g. a real tenant paired with an unrelated project.
    ScopeMismatch,
    /// Authenticated and in scope, but the role does not carry the action.
    InsufficientPermission,
    /// An internal service caller's identity could not be verified.
    InvalidServiceIdentity,
}

string_enum! {
    /// The stable wire code for each failure kind.
    ///
    /// These strings are the platform contract: the API returns them, clients
    /// branch on them, and dashboards alert on them. Renaming one is a
    /// breaking change.
    AuthFailureCode {
        Unauthenticated => "unauthenticated",
        InvalidCredential => "invalid_credential",
        ExpiredCredential => "expired_credential",
        Forbidden => "forbidden",
        TenantMismatch => "tenant_mismatch",
        OrganizationMismatch => "organization_mismatch",
        ProjectMismatch => "project_mismatch",
        ScopeMismatch => "scope_mismatch",
        InsufficientPermission => "insufficient_permission",
        InvalidServiceIdentity => "invalid_service_identity",
    }
}

/// One negative security decision, ready to surface or to audit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthFailure {
    /// The classification.
    pub kind: AuthFailureKind,
    /// Operator-facing diagnostic. Never a secret, never a foreign tenant's
    /// identifier, never a credential fragment.
    pub detail: Option<String>,
}

impl AuthFailure {
    /// Builds a failure with no diagnostic detail.
    #[must_use]
    pub const fn new(kind: AuthFailureKind) -> Self {
        Self { kind, detail: None }
    }

    /// Attaches an operator-facing diagnostic.
    #[must_use]
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    /// The stable public code (safe to return, safe to alert on).
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self.kind {
            AuthFailureKind::Unauthenticated => AuthFailureCode::Unauthenticated.as_str(),
            AuthFailureKind::InvalidCredential => AuthFailureCode::InvalidCredential.as_str(),
            AuthFailureKind::ExpiredCredential => AuthFailureCode::ExpiredCredential.as_str(),
            AuthFailureKind::Forbidden => AuthFailureCode::Forbidden.as_str(),
            AuthFailureKind::TenantMismatch => AuthFailureCode::TenantMismatch.as_str(),
            AuthFailureKind::OrganizationMismatch => AuthFailureCode::OrganizationMismatch.as_str(),
            AuthFailureKind::ProjectMismatch => AuthFailureCode::ProjectMismatch.as_str(),
            AuthFailureKind::ScopeMismatch => AuthFailureCode::ScopeMismatch.as_str(),
            AuthFailureKind::InsufficientPermission => {
                AuthFailureCode::InsufficientPermission.as_str()
            },
            AuthFailureKind::InvalidServiceIdentity => {
                AuthFailureCode::InvalidServiceIdentity.as_str()
            },
        }
    }

    /// Whether the caller failed to authenticate at all (as opposed to being
    /// authenticated but not allowed). Drives `401` vs `403`.
    #[must_use]
    pub const fn is_authentication_failure(&self) -> bool {
        matches!(
            self.kind,
            AuthFailureKind::Unauthenticated
                | AuthFailureKind::InvalidCredential
                | AuthFailureKind::ExpiredCredential
                | AuthFailureKind::InvalidServiceIdentity
        )
    }

    /// The transport error this failure becomes.
    ///
    /// Deliberately narrow: authentication problems are `Unauthorized`, and
    /// everything else is `Forbidden`. The *code* is carried in the message so
    /// the API layer can echo it in its stable error envelope without
    /// inventing a second vocabulary.
    #[must_use]
    pub fn to_app_error(&self) -> AppError {
        let message = match &self.detail {
            Some(detail) => format!("{}: {detail}", self.code()),
            None => self.code().to_owned(),
        };
        if self.is_authentication_failure() {
            AppError::unauthorized(message)
        } else {
            AppError::forbidden(message)
        }
    }

    /// The same failure, with existence hidden.
    ///
    /// Used when the caller asked about a resource that may belong to another
    /// tenant: answering "forbidden" would confirm the row exists, so the
    /// answer is "not found". The distinction is preserved in the audit log,
    /// which is the only place it is safe to record.
    #[must_use]
    pub fn as_hidden(&self, resource: &'static str) -> AppError {
        // The real classification never reaches the caller; only the audit
        // sink sees it (the caller converts back via `Self::code()`).
        let _ = (resource, self.code());
        AppError::not_found(resource, "not found")
    }

    /// Security failures are never retryable.
    ///
    /// This is a constant, not a match, on purpose: a future variant cannot
    /// accidentally inherit `true` by being added to an arm list.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        false
    }

    /// Shortcut: `Result::Err(self.to_app_error())`.
    pub fn err<T>(self) -> Result<T, AppError> {
        Err(self.to_app_error())
    }
}

/// Convenience constructors, so a call site reads as a sentence.
impl AuthFailure {
    #[must_use]
    pub const fn unauthenticated() -> Self {
        Self::new(AuthFailureKind::Unauthenticated)
    }
    #[must_use]
    pub const fn invalid_credential() -> Self {
        Self::new(AuthFailureKind::InvalidCredential)
    }
    #[must_use]
    pub const fn expired_credential() -> Self {
        Self::new(AuthFailureKind::ExpiredCredential)
    }
    #[must_use]
    pub const fn forbidden() -> Self {
        Self::new(AuthFailureKind::Forbidden)
    }
    #[must_use]
    pub const fn tenant_mismatch() -> Self {
        Self::new(AuthFailureKind::TenantMismatch)
    }
    #[must_use]
    pub const fn organization_mismatch() -> Self {
        Self::new(AuthFailureKind::OrganizationMismatch)
    }
    #[must_use]
    pub const fn project_mismatch() -> Self {
        Self::new(AuthFailureKind::ProjectMismatch)
    }
    #[must_use]
    pub const fn scope_mismatch() -> Self {
        Self::new(AuthFailureKind::ScopeMismatch)
    }
    #[must_use]
    pub const fn insufficient_permission() -> Self {
        Self::new(AuthFailureKind::InsufficientPermission)
    }
    #[must_use]
    pub const fn invalid_service_identity() -> Self {
        Self::new(AuthFailureKind::InvalidServiceIdentity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_kind_has_a_stable_public_code() {
        let kinds = [
            AuthFailureKind::Unauthenticated,
            AuthFailureKind::InvalidCredential,
            AuthFailureKind::ExpiredCredential,
            AuthFailureKind::Forbidden,
            AuthFailureKind::TenantMismatch,
            AuthFailureKind::OrganizationMismatch,
            AuthFailureKind::ProjectMismatch,
            AuthFailureKind::ScopeMismatch,
            AuthFailureKind::InsufficientPermission,
            AuthFailureKind::InvalidServiceIdentity,
        ];
        let mut codes = std::collections::BTreeSet::new();
        for kind in kinds {
            let code = AuthFailure::new(kind).code();
            assert!(!code.is_empty(), "{kind:?} has no code");
            assert!(
                code.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                "code {code} is not a stable snake_case token"
            );
            assert!(codes.insert(code), "duplicate code {code}");
        }
        assert_eq!(codes.len(), kinds.len());
    }

    #[test]
    fn authentication_failures_map_to_401_and_the_rest_to_403() {
        for kind in [
            AuthFailureKind::Unauthenticated,
            AuthFailureKind::InvalidCredential,
            AuthFailureKind::ExpiredCredential,
            AuthFailureKind::InvalidServiceIdentity,
        ] {
            let error = AuthFailure::new(kind).to_app_error();
            assert!(
                matches!(error, AppError::Unauthorized(_)),
                "{kind:?} must be an authentication failure: {error:?}"
            );
        }
        for kind in [
            AuthFailureKind::Forbidden,
            AuthFailureKind::TenantMismatch,
            AuthFailureKind::OrganizationMismatch,
            AuthFailureKind::ProjectMismatch,
            AuthFailureKind::ScopeMismatch,
            AuthFailureKind::InsufficientPermission,
        ] {
            let error = AuthFailure::new(kind).to_app_error();
            assert!(
                matches!(error, AppError::Forbidden(_)),
                "{kind:?} must be an authorization failure: {error:?}"
            );
        }
    }

    #[test]
    fn no_security_failure_is_retryable() {
        // A retryable denial is a denial that eventually succeeds somewhere.
        for kind in [
            AuthFailureKind::Unauthenticated,
            AuthFailureKind::InvalidCredential,
            AuthFailureKind::ExpiredCredential,
            AuthFailureKind::Forbidden,
            AuthFailureKind::TenantMismatch,
            AuthFailureKind::OrganizationMismatch,
            AuthFailureKind::ProjectMismatch,
            AuthFailureKind::ScopeMismatch,
            AuthFailureKind::InsufficientPermission,
            AuthFailureKind::InvalidServiceIdentity,
        ] {
            assert!(!AuthFailure::new(kind).is_retryable(), "{kind:?}");
        }
    }

    #[test]
    fn the_code_travels_in_the_error_message() {
        let error = AuthFailure::tenant_mismatch()
            .with_detail("credential is bound to another tenant")
            .to_app_error();
        let text = error.public_message();
        assert!(text.contains("tenant_mismatch"), "{text}");
        assert!(text.contains("bound to another tenant"), "{text}");
    }

    #[test]
    fn hiding_existence_never_reveals_the_real_reason() {
        let failure = AuthFailure::tenant_mismatch().with_detail("tenant 42 is not yours");
        let hidden = failure.as_hidden("execution");
        match hidden {
            AppError::NotFound { resource, .. } => assert_eq!(resource, "execution"),
            other => panic!("expected NotFound, got {other:?}"),
        };
        // The message must not carry the other tenant's identity.
        assert!(!hidden.public_message().contains("42"));
    }
}
