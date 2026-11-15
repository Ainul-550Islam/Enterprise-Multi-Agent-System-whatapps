/// WhatsApp template model, variable validation, category, and status sync.
///
/// WhatsApp requires that all template messages be pre-approved by Meta.
/// This crate defines the schema, validates variable constraints, and
/// provides status-tracking types. The actual sync with Meta (getting
/// template approval/status) is handled by the application layer via the
/// Graph API client.
use serde::Serialize;
use serde::Deserialize;

/// Category of a WhatsApp template as recognised by Meta.
///
/// Text-only templates have a lower approval barrier; media-inclusive
/// templates require more rigorous review.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TemplateCategory {
    /// Simple text template (often approved faster)
    Text,
    /// Template with media (image/video/document) — stricter review
    Media,
    /// Template with interactive components (button/list)
    Interactive,
    /// Template with authentication OTP code
    Authentication,
    /// Template with notification content
    Notification,
}

/// Status of a template in the Meta review pipeline.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TemplateStatus {
    /// Template submitted, under review
    Pending,
    /// Template approved by Meta — can be used
    Approved,
    /// Template rejected by Meta — cannot be used
    Rejected,
    /// Template archived (usually after long pending)
    Archived,
}

/// A template variable definition.
///
/// WhatsApp templates can have named variables with type constraints.
/// This model validates that the right number and types of variables
/// are provided at send time.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TemplateVariable {
    /// Variable name as referenced in the template string (e.g. {{1}})
    pub name: String,
    /// Variable type — determines what the sender may pass
    pub variable_type: TemplateVariableType,
    /// Whether the variable is required (always present) or optional
    pub required: bool,
}

/// Variable type as recognised by Meta's template system.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TemplateVariableType {
    /// Free-form text
    Text,
    /// Phone number in E.164 format
    PhoneNumber,
    /// Currency amount with optional code
    Currency,
    /// Section name from a list template
    Section,
}

/// A template model that carries category, language, and variable definitions.
///
/// This is the canonical representation used by the outbound builder
/// and by the notification adapter to validate send-requests.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TemplateModel {
    /// Human-readable template name (must match exactly what Meta approved)
    pub name: String,
    /// Language code (e.g. "en", "fr", "bn")
    pub language: String,
    /// Template category
    pub category: TemplateCategory,
    /// Template status (Pending/Approved/Rejected)
    pub status: TemplateStatus,
    /// Variable definitions for validation
    pub variables: Vec<TemplateVariable>,
}

impl TemplateModel {
    /// Validate that the given variable values match the template's
    /// variable definitions.
    ///
    /// Returns `Ok(())` if valid, or `Err` with a description of the
    /// mismatch.
    pub fn validate_variables(&self, values: &[(&str, &str)]) -> Result<(), String> {
        let expected_count = self.variables.len();
        let provided_count = values.len();

        if expected_count != provided_count {
            return Err(format!(
                "Template '{}': expected {} variable(s), got {}",
                self.name, expected_count, provided_count
            ));
        }

        for (i, (var, (val_key, val))) in self.variables.iter().zip(values.iter()).enumerate() {
            // Simple type check: if variable_type is Text, any string is fine.
            // In a full implementation we'd validate phone numbers, currencies, etc.
            if var.variable_type == TemplateVariableType::Text {
                // Text variables must be non-empty
                if val.is_empty() {
                    return Err(format!(
                        "Template '{}' variable '{}': must not be empty",
                        self.name, var.name
                    ));
                }
            }
            // Other types (PhoneNumber, Currency) would have stricter validation
            // that the application layer provides.
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_template() -> TemplateModel {
        TemplateModel {
            name: "hello_world".into(),
            language: "en".into(),
            category: TemplateCategory::Text,
            status: TemplateStatus::Approved,
            variables: vec![
                TemplateVariable {
                    name: "1".into(),
                    variable_type: TemplateVariableType::Text,
                    required: true,
                },
                TemplateVariable {
                    name: "2".into(),
                    variable_type: TemplateVariableType::Text,
                    required: true,
                },
            ],
        }
    }

    #[test]
    fn test_template_validation_ok() {
        let tm = make_template();
        let result = tm.validate_variables(&[("1", "Hello"), ("2", "World")]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_template_validation_wrong_count() {
        let tm = make_template();
        let result = tm.validate_variables(&[("1", "Only one")]);
        assert!(result.is_err());
        assert!(result.unwrap().contains("expected 2 variable(s)"));
    }

    #[test]
    fn test_template_validation_empty_text() {
        let tm = make_template();
        let result = tm.validate_variables(&[("1", ""), ("2", "World")]);
        assert!(result.is_err());
        assert!(result.unwrap().contains("must not be empty"));
    }
}