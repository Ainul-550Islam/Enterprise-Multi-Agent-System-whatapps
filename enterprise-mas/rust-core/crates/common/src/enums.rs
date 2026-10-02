//! Shared state enums used across crates.
//!
//! Every enum has a *stable* wire representation: `snake_case` strings via
//! serde, `as_str` / `Display` and `FromStr`. The string values are
//! part of the platform contract (database rows, events, API payloads) and
//! must never be renamed once shipped.

/// Defines a string-representable enum with the full standard surface:
/// `as_str`, `Display`, `FromStr` (accepting only canonical strings),
/// `ALL` (all variants), serde as `snake_case` strings, and `variant_names`.
#[macro_export]
macro_rules! string_enum {
    (
        $(#[$meta:meta])*
        $name:ident {
            $( $(#[$vmeta:meta])* $variant:ident => $str:literal ),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq,
            Hash,
            PartialOrd,
            Ord,
            ::serde::Serialize,
            ::serde::Deserialize,
        )]
        #[serde(rename_all = "snake_case")]
        pub enum $name {
            $( $(#[$vmeta])* $variant ),+
        }

        impl $name {
            /// Every variant, in declaration order.
            pub const ALL: &'static [Self] = &[ $( Self::$variant ),+ ];

            /// Canonical stable string representation (matches serde output).
            #[must_use]
            pub const fn as_str(&self) -> &'static str {
                match self {
                    $( Self::$variant => $str ),+
                }
            }

            /// All canonical string values, in declaration order.
            #[must_use]
            pub const fn variant_names() -> &'static [&'static str] {
                &[ $( $str ),+ ]
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl ::std::str::FromStr for $name {
            type Err = $crate::error::AppError;

            fn from_str(s: &str) -> ::std::result::Result<Self, Self::Err> {
                match s.trim() {
                    $( $str => ::std::result::Result::Ok(Self::$variant), )+
                    other => ::std::result::Result::Err($crate::error::AppError::Validation {
                        message: ::std::format!(
                            "invalid {}: {:?} (expected one of: {})",
                            ::std::stringify!($name),
                            other,
                            <[::std::string::String]>::join(
                                &[ $( ::std::string::String::from($str) ),+ ],
                                ", ",
                            ),
                        ),
                        issues: ::std::vec![$crate::error::ValidationIssue::new(
                            ::std::stringify!($name),
                            "invalid_enum_value",
                            "unknown enum value; see message for the allowed set",
                        )],
                    }),
                }
            }
        }

        impl ::std::convert::AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }
    };
}

string_enum! {
    /// Deployment environment of a tenant/workspace.
    Environment {
        Development => "development",
        Staging => "staging",
        Production => "production",
    }
}

impl Environment {
    /// `true` for production-like strictness (audit, approval gates).
    #[must_use]
    pub const fn is_production(&self) -> bool {
        matches!(self, Self::Production)
    }
}

string_enum! {
    /// Lifecycle status of a user account.
    UserStatus {
        PendingActivation => "pending_activation",
        Active => "active",
        Disabled => "disabled",
        Locked => "locked",
    }
}

string_enum! {
    /// Lifecycle status of a tenant.
    TenantStatus {
        Provisioning => "provisioning",
        Active => "active",
        Suspended => "suspended",
        Archived => "archived",
    }
}

impl TenantStatus {
    /// Whether the tenant may execute workloads.
    #[must_use]
    pub const fn is_operational(&self) -> bool {
        matches!(self, Self::Active)
    }
}

string_enum! {
    /// Lifecycle status of an agent.
    AgentStatus {
        Draft => "draft",
        Active => "active",
        Disabled => "disabled",
        Archived => "archived",
    }
}

string_enum! {
    /// Deployment state of a published agent/version.
    DeploymentStatus {
        NotDeployed => "not_deployed",
        Deploying => "deploying",
        Deployed => "deployed",
        Failed => "failed",
        RolledBack => "rolled_back",
    }
}

string_enum! {
    /// Lifecycle states of a workflow.
    WorkflowStatus {
        Draft => "draft",
        Validating => "validating",
        Published => "published",
        Disabled => "disabled",
        Archived => "archived",
    }
}

string_enum! {
    /// Processing status of a task.
    TaskStatus {
        Pending => "pending",
        Queued => "queued",
        Running => "running",
        Completed => "completed",
        Failed => "failed",
        Cancelled => "cancelled",
        DeadLettered => "dead_lettered",
    }
}

impl TaskStatus {
    /// Terminal states: no further transitions are legal.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled | Self::DeadLettered)
    }
}

string_enum! {
    /// Scheduling priority of a task.
    TaskPriority {
        Low => "low",
        Normal => "normal",
        High => "high",
        Critical => "critical",
    }
}

// Manual Default impl: the enum is macro-generated and we deliberately do
// NOT want `Default` on every generated enum — `Normal` is the only sensible
// default priority, hence the allow below.
#[allow(clippy::derivable_impls)]
impl Default for TaskPriority {
    fn default() -> Self {
        Self::Normal
    }
}

impl TaskPriority {
    /// Numeric rank (higher = more urgent) used by queues and fairness logic.
    #[must_use]
    pub const fn rank(&self) -> u8 {
        match self {
            Self::Low => 0,
            Self::Normal => 1,
            Self::High => 2,
            Self::Critical => 3,
        }
    }
}

string_enum! {
    /// Lifecycle of one execution (a complete workflow/agent run).
    ExecutionStatus {
        Pending => "pending",
        Running => "running",
        Paused => "paused",
        Completed => "completed",
        Failed => "failed",
        Cancelled => "cancelled",
    }
}

impl ExecutionStatus {
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

string_enum! {
    /// Status of a single runtime step inside an execution.
    ExecutionStepStatus {
        Pending => "pending",
        Running => "running",
        WaitingApproval => "waiting_approval",
        Completed => "completed",
        Failed => "failed",
        Skipped => "skipped",
        Cancelled => "cancelled",
    }
}

impl ExecutionStepStatus {
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Skipped | Self::Cancelled
        )
    }
}

string_enum! {
    /// Publication lifecycle of a tool.
    ToolStatus {
        Draft => "draft",
        Published => "published",
        Deprecated => "deprecated",
        Disabled => "disabled",
    }
}

string_enum! {
    /// Operational status of an external-system connector.
    ConnectorStatus {
        PendingVerification => "pending_verification",
        Active => "active",
        Error => "error",
        Disabled => "disabled",
    }
}

string_enum! {
    /// Outcome of a policy evaluation.
    PolicyDecision {
        Allow => "allow",
        Deny => "deny",
        RequireApproval => "require_approval",
        Transform => "transform",
        RateLimit => "rate_limit",
    }
}

impl PolicyDecision {
    /// Whether the request may proceed (possibly after transformation).
    #[must_use]
    pub const fn permits_execution(&self) -> bool {
        matches!(self, Self::Allow | Self::Transform)
    }
}

string_enum! {
    /// Severity attached to audit events and security findings.
    AuditSeverity {
        Info => "info",
        Notice => "notice",
        Warning => "warning",
        High => "high",
        Critical => "critical",
    }
}

string_enum! {
    /// Delivery status of an infrastructure event.
    EventStatus {
        Pending => "pending",
        Published => "published",
        Delivered => "delivered",
        Failed => "failed",
        DeadLettered => "dead_lettered",
    }
}

string_enum! {
    /// Status of a schedule.
    ScheduleStatus {
        Active => "active",
        Paused => "paused",
        Disabled => "disabled",
        Completed => "completed",
    }
}
