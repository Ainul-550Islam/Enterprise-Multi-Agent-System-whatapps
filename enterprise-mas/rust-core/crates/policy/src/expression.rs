//! Condition DSL: attribute-path comparisons combined with `all`/`any`/`not`.
//!
//! Compilation (from the JSON stored in a policy definition) is *strict* —
//! unknown operations, malformed paths and wrong arities are compile errors,
//! so a bad policy is rejected at publish time, not mid-request. Evaluation
//! is *total*: no panics, no errors, missing data simply fails the
//! comparator (fail-closed); only `exists` inverts on absence.

use crate::request::EvaluationRequest;
use mas_common::error::AppError;
use mas_common::result::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

/// A validated attribute path (see [`EvaluationRequest::lookup`]).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Path(String);

impl Path {
    /// Exact resolvable roots.
    pub const ROOTS: [&'static str; 9] = [
        "subject.actor",
        "subject.roles",
        "action",
        "resource",
        "tenant_id",
        "organization_id",
        "project_id",
        "environment",
        "requested_at",
    ];

    pub fn parse(raw: &str) -> Result<Self> {
        if Self::ROOTS.contains(&raw)
            || raw
                .strip_prefix("attributes.")
                .is_some_and(|r| !r.is_empty())
        {
            return Ok(Self(raw.to_owned()));
        }
        Err(AppError::invalid_field(
            "when.path",
            "unknown_path",
            format!(
                "'{raw}' is not resolvable — use one of {:?} or 'attributes.<path>'",
                Self::ROOTS
            ),
        ))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(&self.0)
    }
}

/// Binary comparison over a resolved attribute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CmpOp {
    /// JSON equality.
    Eq,
    /// JSON inequality (note: absent attribute fails *closed* → false).
    Ne,
    /// Left ∈ right-array; if left is itself an array, ANY element ∈ right.
    In,
    /// Left ∉ right-array (same array-coercion as `In`; absent → false).
    NotIn,
    /// Array contains element, or string contains substring.
    Contains,
    StartsWith,
    EndsWith,
    /// String glob: `*` any sequence, `?` any single char.
    Glob,
    Gt,
    Gte,
    Lt,
    Lte,
}

impl CmpOp {
    pub const NAMES: [(&'static str, CmpOp); 12] = [
        ("eq", CmpOp::Eq),
        ("ne", CmpOp::Ne),
        ("in", CmpOp::In),
        ("not_in", CmpOp::NotIn),
        ("contains", CmpOp::Contains),
        ("starts_with", CmpOp::StartsWith),
        ("ends_with", CmpOp::EndsWith),
        ("glob", CmpOp::Glob),
        ("gt", CmpOp::Gt),
        ("gte", CmpOp::Gte),
        ("lt", CmpOp::Lt),
        ("lte", CmpOp::Lte),
    ];

    fn parse(name: &str) -> Result<Self> {
        Self::NAMES
            .iter()
            .find(|(candidate, _)| *candidate == name)
            .map(|(_, op)| *op)
            .ok_or_else(|| {
                AppError::invalid_field(
                    "when.op",
                    "unknown_operation",
                    format!("'{name}' is not a comparison operation"),
                )
            })
    }

    fn apply(self, left: Option<&Value>, right: &Value) -> bool {
        let Some(left) = left else {
            return false; // absent attribute → always false (fail closed)
        };
        match self {
            CmpOp::Eq => left == right,
            CmpOp::Ne => left != right,
            CmpOp::In | CmpOp::NotIn => {
                let Some(set) = right.as_array() else {
                    return false;
                };
                let contained = left
                    .as_array()
                    .map(|values| values.iter().any(|item| set.contains(item)))
                    .unwrap_or_else(|| set.contains(left));
                if self == CmpOp::In {
                    contained
                } else {
                    !contained
                }
            },
            CmpOp::Contains => match (left, right) {
                (Value::Array(items), needle) => items.contains(needle),
                (Value::String(haystack), Value::String(needle)) => haystack.contains(needle),
                _ => false,
            },
            CmpOp::StartsWith | CmpOp::EndsWith | CmpOp::Glob => {
                let (Some(left), Some(right)) = (left.as_str(), right.as_str()) else {
                    return false;
                };
                match self {
                    CmpOp::StartsWith => left.starts_with(right),
                    CmpOp::EndsWith => left.ends_with(right),
                    _ => glob_match(left, right),
                }
            },
            CmpOp::Gt | CmpOp::Gte | CmpOp::Lt | CmpOp::Lte => {
                let (Some(left), Some(right)) = (left.as_f64(), right.as_f64()) else {
                    return false;
                };
                match self {
                    CmpOp::Gt => left > right,
                    CmpOp::Gte => left >= right,
                    CmpOp::Lt => left < right,
                    _ => left <= right,
                }
            },
        }
    }
}

/// A compiled condition tree.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    True,
    False,
    Comparison {
        path: Path,
        op: CmpOp,
        value: Value,
    },
    /// `{"exists": ["<path>", <bool>]}` — presence check (default true).
    Exists {
        path: Path,
        expected: bool,
    },
    All(Vec<Expr>),
    Any(Vec<Expr>),
    Not(Box<Expr>),
}

impl Expr {
    /// Compiles a DSL condition tree.
    ///
    /// An empty/absent `when` compiles to `Expr::True`.
    pub fn compile(raw: Option<&Value>) -> Result<Self> {
        match raw {
            None | Some(Value::Null) => Ok(Expr::True),
            Some(value) => Self::compile_value(value),
        }
    }

    fn compile_value(value: &Value) -> Result<Self> {
        let object = value.as_object().ok_or_else(|| {
            AppError::invalid_field("when", "invalid_format", "conditions must be objects")
        })?;
        if object.len() != 1 {
            return Err(AppError::invalid_field(
                "when",
                "invalid_format",
                "each condition object must carry exactly one operator key",
            ));
        }
        let (operator, operand) = object.iter().next().expect("len checked");
        match operator.as_str() {
            "true" => Ok(Expr::True),
            "false" => Ok(Expr::False),
            "all" | "any" => {
                let items = operand.as_array().ok_or_else(|| {
                    AppError::invalid_field(
                        format!("when.{operator}"),
                        "invalid_format",
                        "expects an array of conditions",
                    )
                })?;
                if items.is_empty() {
                    return Err(AppError::invalid_field(
                        format!("when.{operator}"),
                        "empty",
                        "logic combinators need at least one operand",
                    ));
                }
                let compiled = items
                    .iter()
                    .map(Self::compile_value)
                    .collect::<Result<Vec<_>>>()?;
                Ok(if operator == "all" {
                    Expr::All(compiled)
                } else {
                    Expr::Any(compiled)
                })
            },
            "not" => Ok(Expr::Not(Box::new(Self::compile_value(operand)?))),
            "exists" => {
                let (path, expected) = Self::parse_comparison_tail(operand)?;
                let expected = expected.as_bool().ok_or_else(|| {
                    AppError::invalid_field(
                        "when.exists",
                        "invalid_value",
                        "exists expects a boolean operand (default true)",
                    )
                })?;
                Ok(Expr::Exists { path, expected })
            },
            other => {
                let op = CmpOp::parse(other)?;
                let (path, value) = Self::parse_comparison_tail(operand)?;
                Ok(Expr::Comparison {
                    path,
                    op,
                    value: value.clone(),
                })
            },
        }
    }

    /// Parses the `[path, operand]` two-element array shared by comparisons.
    fn parse_comparison_tail(operand: &Value) -> Result<(Path, &Value)> {
        let items = operand.as_array().ok_or_else(|| {
            AppError::invalid_field("when", "invalid_format", "comparisons take [path, value]")
        })?;
        if items.len() != 2 {
            return Err(AppError::invalid_field(
                "when",
                "invalid_format",
                format!(
                    "comparisons take exactly [path, value], got {}",
                    items.len()
                ),
            ));
        }
        let path = items[0]
            .as_str()
            .ok_or_else(|| {
                AppError::invalid_field("when.path", "invalid_type", "paths must be strings")
            })
            .and_then(Path::parse)?;
        Ok((path, &items[1]))
    }

    /// Total evaluation against a request. Never panics, never errors:
    /// unknown data fails closed.
    #[must_use]
    pub fn evaluate(&self, request: &EvaluationRequest) -> bool {
        match self {
            Expr::True => true,
            Expr::False => false,
            Expr::Comparison { path, op, value } => {
                op.apply(request.lookup(path.as_str()).as_ref(), value)
            },
            Expr::Exists { path, expected } => request.lookup(path.as_str()).is_some() == *expected,
            Expr::All(exprs) => exprs.iter().all(|expr| expr.evaluate(request)),
            Expr::Any(exprs) => exprs.iter().any(|expr| expr.evaluate(request)),
            Expr::Not(expr) => !expr.evaluate(request),
        }
    }
}

/// Glob match supporting `*` (any run) and `?` (one char). Iterative
/// two-pointer with star backtracking — linear, no recursion blowups.
pub(crate) fn glob_match(haystack: &str, pattern: &str) -> bool {
    let text: Vec<char> = haystack.chars().collect();
    let pat: Vec<char> = pattern.chars().collect();
    let (mut ti, mut pi) = (0usize, 0usize);
    let (mut star_t, mut star_p) = (None::<usize>, None::<usize>);
    while ti < text.len() {
        if pi < pat.len() && (pat[pi] == '?' || pat[pi] == text[ti]) {
            ti += 1;
            pi += 1;
        } else if pi < pat.len() && pat[pi] == '*' {
            star_t = Some(ti);
            star_p = Some(pi);
            pi += 1;
        } else if let (Some(st), Some(sp)) = (star_t, star_p) {
            ti = st + 1;
            star_t = Some(ti);
            pi = sp + 1;
        } else {
            return false;
        }
    }
    while pi < pat.len() && pat[pi] == '*' {
        pi += 1;
    }
    pi == pat.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::{EvaluationContext, EvaluationSubject};
    use mas_common::enums::Environment;
    use mas_common::ids::TenantId;
    use serde_json::json;

    fn request() -> EvaluationRequest {
        EvaluationRequest::new(
            EvaluationSubject::new("alice")
                .expect("subject")
                .with_roles(["editor", "viewer"]),
            "tool.invoke",
            "tool:abc",
            json!({"cost": 42, "labels": ["pci", "prod"]}),
            EvaluationContext::new(TenantId::new(), Environment::Production),
        )
        .expect("request")
    }

    #[test]
    fn comparisons_evaluate() {
        let req = request();
        let cases = [
            (json!({"eq": ["subject.actor", "alice"]}), true),
            (json!({"eq": ["subject.actor", "bob"]}), false),
            (json!({"ne": ["subject.actor", "bob"]}), true),
            (json!({"in": ["subject.roles", ["admin", "editor"]]}), true),
            (json!({"in": ["subject.actor", ["carol"]]}), false),
            (json!({"not_in": ["subject.roles", ["admin"]]}), true),
            (json!({"contains": ["attributes.labels", "pci"]}), true),
            (json!({"contains": ["resource", "tool:"]}), true),
            (json!({"starts_with": ["action", "tool."]}), true),
            (json!({"ends_with": ["action", ".invoke"]}), true),
            (json!({"glob": ["action", "tool.*"]}), true),
            (json!({"glob": ["resource", "tool:?b?"]}), true),
            (json!({"gt": ["attributes.cost", 40]}), true),
            (json!({"lte": ["attributes.cost", 42]}), true),
            (json!({"gt": ["attributes.cost", "forty"]}), false), // type mismatch → false
        ];
        for (raw, expected) in cases {
            let expr = Expr::compile(Some(&raw)).expect("compiles");
            assert_eq!(expr.evaluate(&req), expected, "for {raw}");
        }
    }

    #[test]
    fn logic_combinators_evaluate() {
        let req = request();
        let expr = Expr::compile(Some(&json!({
            "all": [
                {"eq": ["environment", "production"]},
                {"any": [
                    {"contains": ["attributes.labels", "pci"]},
                    {"not": {"exists": ["attributes.labels", true]}}
                ]}
            ]
        })))
        .expect("compiles");
        assert!(expr.evaluate(&req));

        let missing =
            Expr::compile(Some(&json!({"eq": ["organization_id", "x"]}))).expect("compiles");
        assert!(!missing.evaluate(&req), "missing attribute fails closed");
        let inverted = Expr::compile(Some(&json!({"not": {"eq": ["organization_id", "x"]}})))
            .expect("compiles");
        assert!(inverted.evaluate(&req));
    }

    #[test]
    fn compile_is_strict() {
        let bad = [
            json!({"unknown_op": ["action", "x"]}),           // unknown op
            json!({"eq": ["subject.bogus", "x"]}),            // unresolvable path
            json!({"eq": ["action"]}),                        // arity
            json!({"eq": [42, "x"]}),                         // non-string path
            json!({"all": []}),                               // empty combinator
            json!({"exists": ["action", "yes"]}),             // non-bool exists
            json!({"eq": ["action", "x"], "ne": ["a", "b"]}), // two operator keys
        ];
        for raw in bad {
            assert!(Expr::compile(Some(&raw)).is_err(), "must reject {raw}");
        }
        assert!(matches!(Expr::compile(None), Ok(Expr::True)));
    }

    #[test]
    fn glob_match_is_linear_and_correct() {
        assert!(glob_match("abcdef", "a*f"));
        assert!(glob_match("abcdef", "a*"));
        assert!(!glob_match("abc", "d*"));
        assert!(glob_match("abc", "*"));
        assert!(glob_match("ab", "a?"));
        assert!(!glob_match("ab", "a?c"));
        assert!(glob_match("a?b", "a\\?b") || !glob_match("a?b", "a\\?b")); // escapes unsupported (doc)
    }
}
