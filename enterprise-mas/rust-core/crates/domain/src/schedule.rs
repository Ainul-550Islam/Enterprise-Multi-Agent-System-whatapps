//! Schedule aggregate: cron / interval / one-time triggers that materialize
//! tasks. The scheduling loop lives in the scheduling crate; this aggregate
//! owns recurrence rules and next-run computation.

use chrono::Utc;
use mas_common::enums::ScheduleStatus;
use mas_common::error::AppError;
use mas_common::ids::{ProjectId, ScheduleId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::time::Duration;

/// Recurrence rule of a schedule.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScheduleKind {
    /// Six/seven-field cron expression (with seconds) or classic 5-field
    /// cron (interpreted as `<sec=0> min hour dom mon dow`). Evaluated in UTC.
    Cron {
        /// Cron expression (validated in `Schedule::new`).
        expression: String,
        /// IANA timezone name; only `UTC` is honored today (kept for
        /// forward compatibility and validated syntactically).
        #[serde(default = "default_timezone")]
        /// IANA timezone; currently `UTC` honored.
        timezone: String,
    },
    /// Fixed interval between runs.
    Interval {
        /// Seconds between firings.
        every_seconds: u64,
    },
    /// Single fire-and-complete run.
    OneTime {
        /// Fire-at timestamp.
        run_at: Timestamp,
    },
}

fn default_timezone() -> String {
    "UTC".to_owned()
}

fn parse_cron(expression: &str) -> Result<cron::Schedule> {
    let trimmed = expression.trim();
    let field_count = trimmed.split_whitespace().count();
    // cron crate expects seconds first; adapt classic 5-field expressions.
    let normalized = if field_count == 5 {
        format!("0 {trimmed}")
    } else {
        trimmed.to_owned()
    };
    cron::Schedule::from_str(&normalized).map_err(|err| {
        AppError::invalid_field(
            "expression",
            "invalid_cron",
            format!("invalid cron expression ({field_count} fields): {err}"),
        )
    })
}

/// The schedule aggregate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schedule {
    pub id: ScheduleId,
    pub tenant_id: TenantId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<ProjectId>,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub kind: ScheduleKind,
    /// Template payload used to create the task on each fire
    /// (`operation`, `input`, `priority`, target refs…).
    pub task_template: serde_json::Value,
    pub status: ScheduleStatus,
    /// When disabled and re-enabled, whether to fire missed runs once.
    pub catch_up: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_at: Option<Timestamp>,
    /// Cached next fire time (recomputed on every lifecycle change).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_run_at: Option<Timestamp>,
    /// Do not create more runs after this time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_at: Option<Timestamp>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl Schedule {
    pub fn new(
        tenant_id: TenantId,
        project_id: Option<ProjectId>,
        name: impl Into<String>,
        kind: ScheduleKind,
        task_template: serde_json::Value,
    ) -> Result<Self> {
        let name = name.into();
        validation::validate_resource_name("name", &name)?;
        if !task_template.is_object() {
            return Err(AppError::invalid_field(
                "task_template",
                "invalid_format",
                "task template must be a JSON object",
            ));
        }
        Self::validate_recurrence(&kind)?;
        let now = Timestamp::now();
        let mut schedule = Self {
            id: ScheduleId::new(),
            tenant_id,
            project_id,
            name,
            description: None,
            kind,
            task_template,
            status: ScheduleStatus::Disabled,
            catch_up: false,
            last_run_at: None,
            next_run_at: None,
            end_at: None,
            created_at: now,
            updated_at: now,
        };
        schedule.recompute_next_run(&now)?;
        Ok(schedule)
    }

    /// Validates a recurrence rule without constructing a schedule.
    pub fn validate_recurrence(kind: &ScheduleKind) -> Result<()> {
        match kind {
            ScheduleKind::Cron {
                expression,
                timezone,
            } => {
                parse_cron(expression)?;
                if !matches!(timezone.as_str(), "UTC" | "Etc/UTC") {
                    return Err(AppError::invalid_field(
                        "timezone",
                        "unsupported_timezone",
                        "only UTC schedules are supported at this time",
                    ));
                }
                Ok(())
            },
            ScheduleKind::Interval { every_seconds } => {
                if *every_seconds < 10 || *every_seconds > 31_536_000 {
                    return Err(AppError::invalid_field(
                        "every_seconds",
                        "out_of_range",
                        "intervals must be 10 seconds ..= 1 year",
                    ));
                }
                Ok(())
            },
            ScheduleKind::OneTime { run_at } => {
                if !run_at.is_future() {
                    return Err(AppError::invalid_field(
                        "run_at",
                        "out_of_range",
                        "one-time schedules must be in the future",
                    ));
                }
                Ok(())
            },
        }
    }

    /// Computes the next fire time strictly after `after`, honoring `end_at`.
    /// `None` ⇒ no further runs.
    pub fn next_run_at_after(&self, after: &Timestamp) -> Option<Timestamp> {
        let candidate = match &self.kind {
            ScheduleKind::Cron { expression, .. } => {
                let parsed = parse_cron(expression).ok()?;
                let after_dt: chrono::DateTime<Utc> = (*after).into_datetime();
                parsed.after(&after_dt).next().map(Timestamp::from_datetime)
            },
            ScheduleKind::Interval { every_seconds } => {
                after.checked_add(Duration::from_secs(*every_seconds))
            },
            ScheduleKind::OneTime { run_at } => {
                if run_at.is_after(after) {
                    Some(*run_at)
                } else {
                    None
                }
            },
        }?;
        match self.end_at {
            Some(end) if candidate.is_after(&end) => None,
            _ => Some(candidate),
        }
    }

    /// Disabled → Active; recomputes the next run.
    pub fn enable(&mut self) -> Result<()> {
        match self.status {
            ScheduleStatus::Disabled | ScheduleStatus::Paused => {
                self.status = ScheduleStatus::Active;
                self.recompute_next_run(&Timestamp::now())?;
                self.touch();
                Ok(())
            },
            ScheduleStatus::Active => Ok(()),
            ScheduleStatus::Completed => Err(AppError::conflict(
                "completed schedules cannot be re-enabled",
            )),
        }
    }

    /// Active → Paused (definition retained).
    pub fn pause(&mut self) -> Result<()> {
        match self.status {
            ScheduleStatus::Active => {
                self.status = ScheduleStatus::Paused;
                self.next_run_at = None;
                self.touch();
                Ok(())
            },
            other => Err(AppError::conflict(format!(
                "schedule in status '{other}' cannot be paused"
            ))),
        }
    }

    /// Any non-completed → Disabled.
    pub fn disable(&mut self) -> Result<()> {
        match self.status {
            ScheduleStatus::Completed => {
                Err(AppError::conflict("completed schedules are terminal"))
            },
            ScheduleStatus::Disabled => Ok(()),
            _ => {
                self.status = ScheduleStatus::Disabled;
                self.next_run_at = None;
                self.touch();
                Ok(())
            },
        }
    }

    /// Records that a fire occurred at `fired_at` and computes the next run.
    /// One-time schedules complete on first fire.
    pub fn mark_fired(&mut self, fired_at: Timestamp) -> Result<()> {
        if self.status != ScheduleStatus::Active {
            return Err(AppError::conflict(format!(
                "schedule {} in status '{}' cannot fire",
                self.id, self.status
            )));
        }
        self.last_run_at = Some(fired_at);
        if matches!(self.kind, ScheduleKind::OneTime { .. }) {
            self.status = ScheduleStatus::Completed;
            self.next_run_at = None;
        } else {
            self.recompute_next_run(&fired_at)?;
        }
        self.touch();
        Ok(())
    }

    /// Whether a run is due at `now`.
    #[must_use]
    pub fn is_due(&self, now: &Timestamp) -> bool {
        self.status == ScheduleStatus::Active
            && self.next_run_at.is_some_and(|next| !next.is_after(now))
    }

    fn recompute_next_run(&mut self, now: &Timestamp) -> Result<()> {
        self.next_run_at = if self.status == ScheduleStatus::Active || self.next_run_at.is_some() {
            self.next_run_at_after(now)
        } else {
            None // constructed disabled; enable() will compute
        };
        Ok(())
    }

    fn touch(&mut self) {
        self.updated_at = Timestamp::now();
    }
}
