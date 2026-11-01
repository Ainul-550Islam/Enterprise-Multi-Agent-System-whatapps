//! The evaluation request: subject + action + resource + attributes + scope.
//!
//! This is the *only* input an evaluation may depend on (plus the active
//! policy set and the current timestamp). Attribute paths resolve against
//! it; see [`EvaluationRequest::lookup`] for the supported roots.

use mas_common::enums::Environment;
use mas_common::error::AppError;
use mas_common::ids::{OrganizationId, ProjectId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Who is asking.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvaluationSubject {
    /// Stable actor identifier (user, service account, or `"system"`-ish).
    pub actor: String,
    /// Roles the actor holds (RBAC facts; ABAC conditions may match them).
    #[serde(default)]
    pub roles: Vec<String>,
}

impl EvaluationSubject {
    pub fn new(actor: impl Into<String>) -> Result<Self> {
        let actor = actor.into();
        validation::validate_non_empty("subject.actor", &actor)?;
        Ok(Self {
            actor,
            roles: Vec::new(),
        })
    }

    #[must_use]
    pub fn with_roles<I, S>(mut self, roles: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.roles = roles.into_iter().map(Into::into).collect();
        self
    }
}

/// The scope the request executes under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvaluationContext {
    pub tenant_id: TenantId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub organization_id: Option<OrganizationId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<ProjectId>,
    pub environment: Environment,
}

impl EvaluationContext {
    pub fn new(tenant_id: TenantId, environment: Environment) -> Self {
        Self {
            tenant_id,
            organization_id: None,
            project_id: None,
            environment,
        }
    }

    #[must_use]
    pub const fn with_organization(mut self, organization_id: OrganizationId) -> Self {
        self.organization_id = Some(organization_id);
        self
    }

    #[must_use]
    pub const fn with_organization_opt(mut self, organization_id: Option<OrganizationId>) -> Self {
        self.organization_id = organization_id;
        self
    }

    #[must_use]
    pub const fn with_project(mut self, project_id: ProjectId) -> Self {
        self.project_id = Some(project_id);
        self
    }

    #[must_use]
    pub const fn with_project_opt(mut self, project_id: Option<ProjectId>) -> Self {
        self.project_id = project_id;
        self
    }
}

/// Canonical evaluation input.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvaluationRequest {
    pub subject: EvaluationSubject,
    /// Operation name, e.g. `task.submit`, `tool.invoke`, `execution.start`.
    pub action: String,
    /// Resource identifier, e.g. `tool:<uuid>`, `workflow:<uuid>`, `tenant`.
    pub resource: String,
    /// Request-scoped attributes (tool id, payload class, cost estimate…).
    #[serde(default)]
    pub attributes: Value,
    pub context: EvaluationContext,
    pub requested_at: Timestamp,
}

impl EvaluationRequest {
    pub fn new(
        subject: EvaluationSubject,
        action: impl Into<String>,
        resource: impl Into<String>,
        attributes: Value,
        context: EvaluationContext,
    ) -> Result<Self> {
        let action = action.into();
        let resource = resource.into();
        validation::validate_non_empty("action", &action)?;
        validation::validate_non_empty("resource", &resource)?;
        Ok(Self {
            subject,
            action,
            resource,
            attributes: if attributes.is_object() {
                attributes
            } else {
                Value::Object(Default::default())
            },
            context,
            requested_at: Timestamp::now(),
        })
    }

    #[must_use]
    pub const fn with_time(mut self, requested_at: Timestamp) -> Self {
        self.requested_at = requested_at;
        self
    }

    /// Resolves an attribute path. Supported roots:
    ///
    /// * `subject.actor`, `subject.roles` (array),  
    /// * `action`, `resource`,  
    /// * `tenant_id`, `organization_id`, `project_id`, `environment`,  
    /// * `requested_at` (RFC3339 string),  
    /// * `attributes.<dotted.path>` — walks objects; `"*"`-less, literal.
    ///
    /// Unknown or absent paths yield `None` (comparators then fail closed).
    #[must_use]
    pub fn lookup(&self, path: &str) -> Option<Value> {
        match path {
            "subject.actor" => Some(Value::from(self.subject.actor.clone())),
            "subject.roles" => Some(
                self.subject
                    .roles
                    .iter()
                    .cloned()
                    .map(Value::from)
                    .collect(),
            ),
            "action" => Some(Value::from(self.action.clone())),
            "resource" => Some(Value::from(self.resource.clone())),
            "tenant_id" => Some(Value::from(self.context.tenant_id.to_string())),
            "organization_id" => self
                .context
                .organization_id
                .map(|id| Value::from(id.to_string())),
            "project_id" => self
                .context
                .project_id
                .map(|id| Value::from(id.to_string())),
            "environment" => Some(Value::from(self.context.environment.to_string())),
            "requested_at" => Some(Value::from(self.requested_at.to_rfc3339_millis())),
            _ => path
                .strip_prefix("attributes.")
                .and_then(|inner| self.lookup_in(&self.attributes, inner)),
        }
    }

    fn lookup_in(&self, value: &Value, dotted: &str) -> Option<Value> {
        let mut cursor = Some(value);
        for segment in dotted.split('.') {
            cursor = cursor?.as_object()?.get(segment);
        }
        cursor.cloned()
    }

    /// Deterministic canonical form for cache keys / checksums.
    pub(crate) fn canonical(&self) -> Result<String> {
        serde_json::to_string(self)
            .map_err(|e| AppError::serialization(format!("request canonicalization: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> EvaluationRequest {
        let subject = EvaluationSubject::new("alice")
            .expect("subject")
            .with_roles(["editor"]);
        let context = EvaluationContext::new(TenantId::new(), Environment::Production);
        EvaluationRequest::new(
            subject,
            "tool.invoke",
            "tool:123",
            serde_json::json!({"tool": {"kind": "http", "nested": {"deep": 7}}}),
            context,
        )
        .expect("request")
    }

    #[test]
    fn lookup_resolves_every_root() {
        let request = request();
        assert_eq!(request.lookup("subject.actor"), Some(Value::from("alice")));
        assert_eq!(
            request.lookup("subject.roles"),
            Some(serde_json::json!(["editor"]))
        );
        assert_eq!(request.lookup("action"), Some(Value::from("tool.invoke")));
        assert_eq!(request.lookup("resource"), Some(Value::from("tool:123")));
        assert_eq!(
            request.lookup("attributes.tool.kind"),
            Some(Value::from("http"))
        );
        assert_eq!(
            request.lookup("attributes.tool.nested.deep"),
            Some(serde_json::json!(7))
        );
        assert_eq!(
            request.lookup("environment"),
            Some(Value::from("production"))
        );
        assert!(request.lookup("tenant_id").is_some());
        assert!(request.lookup("requested_at").is_some());
    }

    #[test]
    fn missing_paths_yield_none_and_non_objects_are_normalized() {
        let request = EvaluationRequest::new(
            EvaluationSubject::new("bob").expect("subject"),
            "task.submit",
            "task",
            Value::Null, // not an object → becomes {}
            EvaluationContext::new(TenantId::new(), Environment::Development),
        )
        .expect("request");
        assert_eq!(request.lookup("attributes.anything"), None);
        assert_eq!(request.lookup("subject.nonsense"), None);
        assert_eq!(request.lookup("organization_id"), None);
        assert_eq!(request.attributes, serde_json::json!({}));
    }

    #[test]
    fn validation_rejects_empty_action_or_resource() {
        let subject = EvaluationSubject::new("a").expect("subject");
        let context = EvaluationContext::new(TenantId::new(), Environment::Development);
        assert!(EvaluationRequest::new(subject.clone(), "", "r", Value::Null, context).is_err());
        assert!(EvaluationRequest::new(subject, "a", " ", Value::Null, context).is_err());
    }

    #[test]
    fn canonical_is_byte_stable() {
        let request = request();
        let a = request.canonical().expect("canon");
        let b = request.canonical().expect("canon");
        assert_eq!(a, b, "same request must canonicalize identically twice");
        assert!(a.contains("\"requested_at\""));
    }
}
