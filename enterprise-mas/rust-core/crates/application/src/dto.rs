//! Internal mapping from domain aggregates to the versioned `contracts`
//! wire shapes. API processes serialize ONLY these DTOs; aggregates never
//! cross the process boundary verbatim (private invariants stay private).

use mas_common::enums::{AgentStatus, ExecutionStatus};
#[cfg(test)]
use mas_common::ids::{AgentId, WorkflowId};
use mas_common::ids::{AgentVersionId, ExecutionId};
use mas_contracts::agent::{AgentResponse, AgentVersionDto};
use mas_contracts::execution::{ExecutionResponse, ExecutionStepDto, ExecutionUsageDto};
use mas_contracts::workflow::{
    WorkflowEdgeDto, WorkflowGraphDto, WorkflowNodeDto, WorkflowVersionDto,
};
use mas_domain::{Agent, AgentVersion, Execution, ExecutionStep, WorkflowVersion};

/// Domain `Agent` → wire `AgentResponse`.
#[must_use]
pub fn agent_response(agent: &Agent) -> AgentResponse {
    AgentResponse {
        id: agent.id,
        project_id: agent.project_id,
        tenant_id: agent.tenant_id,
        name: agent.name.clone(),
        slug: agent.slug.as_str().to_owned(),
        kind: agent.kind.as_str().to_owned(),
        status: agent_status_label(&agent.status),
        current_version: agent.current_version,
        active_version_id: active_version_hint(agent),
        tags: capability_tags(agent),
        created_at: agent.created_at,
        updated_at: agent.updated_at,
    }
}

/// Domain immutable agent version → versioned wire contract.
#[must_use]
pub fn agent_version_response(version: &AgentVersion) -> AgentVersionDto {
    AgentVersionDto {
        id: version.id(),
        agent_id: version.agent_id(),
        version_number: version.version_number(),
        configuration_checksum: version.configuration_checksum().to_owned(),
        configuration_snapshot: version.configuration_snapshot().clone(),
        published_by: version.published_by(),
        published_at: version.published_at(),
        deployment_status: version.deployment_status(),
        rollback_of: version.rollback_of(),
    }
}

/// Domain workflow snapshot → immutable version-history contract.
#[must_use]
pub fn workflow_version_response(version: &WorkflowVersion) -> WorkflowVersionDto {
    let graph = version.graph();
    WorkflowVersionDto {
        id: version.id(),
        workflow_id: version.workflow_id(),
        tenant_id: version.tenant_id(),
        version_number: version.version_number(),
        checksum: version.checksum().to_owned(),
        graph: WorkflowGraphDto {
            nodes: graph
                .nodes
                .iter()
                .map(|node| WorkflowNodeDto {
                    node_key: node.node_key.clone(),
                    node_type: node.node_type.to_string(),
                    name: node.name.clone(),
                    config: node.config.clone(),
                    timeout_ms: node
                        .config
                        .get("timeout_ms")
                        .and_then(serde_json::Value::as_u64),
                    max_retries: node
                        .config
                        .get("max_retries")
                        .and_then(serde_json::Value::as_u64)
                        .and_then(|value| u32::try_from(value).ok()),
                })
                .collect(),
            edges: graph
                .edges
                .iter()
                .map(|edge| WorkflowEdgeDto {
                    from: edge.from.clone(),
                    to: edge.to.clone(),
                    condition: edge.condition.clone(),
                    priority: edge.priority,
                })
                .collect(),
        },
        topo_order: version.topo_order().to_vec(),
        created_by: version.created_by(),
        created_at: version.created_at(),
        rollback_of: version.rollback_of(),
    }
}

fn agent_status_label(status: &AgentStatus) -> String {
    status.as_str().to_owned()
}

/// Agents don't expose a mutable "active version" pointer; the published
/// checkpoint is the authoritative hint and the worker resolves it.
fn active_version_hint(_agent: &Agent) -> Option<AgentVersionId> {
    None
}

/// Capabilities surface as tags on the wire: `type` or `type:target`.
fn capability_tags(agent: &Agent) -> Vec<String> {
    agent
        .capabilities
        .iter()
        .map(|capability| match &capability.target {
            Some(target) => format!("{}:{target}", capability.capability_type),
            None => capability.capability_type.to_string(),
        })
        .collect()
}

/// Domain `Execution` → wire `ExecutionResponse`.
#[must_use]
pub fn execution_response(execution: &Execution) -> ExecutionResponse {
    let usage = &execution.resource_usage;
    ExecutionResponse {
        id: execution.id,
        status: execution_status_copy(&execution.status),
        tenant_id: execution.tenant_id,
        workflow_id: execution.workflow_id,
        agent_id: execution.agent_id,
        parent_execution_id: execution.parent_execution_id,
        root_execution_id: execution.root_execution_id,
        correlation_id: execution.correlation_id.clone(),
        cancellation_requested: execution.cancellation_requested(),
        usage: ExecutionUsageDto {
            steps_executed: usage.steps_executed,
            tool_calls: usage.tool_calls,
            tokens_input: usage.tokens_input,
            tokens_output: usage.tokens_output,
            active_time_ms: usage.active_time_ms,
        },
        output: execution.output.clone(),
        failure: execution.failure.clone(),
        created_at: execution.created_at,
        started_at: execution.started_at,
        finished_at: execution.finished_at,
    }
}

/// Domain step → safe timeline DTO. Payload references stay inside the
/// application/runtime boundary; only classified error information and
/// measured timing are projected.
#[must_use]
pub fn execution_step_response(step: &ExecutionStep) -> ExecutionStepDto {
    ExecutionStepDto {
        id: step.id,
        execution_id: step.execution_id,
        kind: step.step_type.clone(),
        sequence: step.sequence,
        node_key: step.node_key.clone(),
        attempt: step.attempt,
        status: step.status,
        tool_id: step.tool_id,
        agent_id: step.agent_id,
        // ExecutionStep currently has no per-step token counters. Keep them
        // absent rather than attributing execution-wide usage to one step.
        tokens_input: None,
        tokens_output: None,
        latency_ms: step
            .duration()
            .and_then(|duration| u64::try_from(duration.as_millis()).ok()),
        error_code: step.error.as_ref().map(|error| error.code.clone()),
        error_message: step.error.as_ref().map(|error| error.message.clone()),
        started_at: step.started_at,
        finished_at: step.finished_at,
        created_at: step.created_at,
    }
}

const fn execution_status_copy(status: &ExecutionStatus) -> ExecutionStatus {
    *status
}

/// Convenience for header/audit references.
#[must_use]
pub fn execution_root_hint(execution: &Execution) -> (ExecutionId, ExecutionId) {
    (execution.id, execution.root_execution_id)
}

// ------------------------------------------------------------------------
// Tests
// ------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::enums::ExecutionStatus;
    use mas_common::ids::{OrganizationId, ProjectId, TenantId};
    use mas_domain::value_objects::Slug;
    use mas_domain::AgentKind;

    #[test]
    fn agent_maps_to_wire_shape() {
        let agent = Agent::create(
            TenantId::new(),
            OrganizationId::new(),
            ProjectId::new(),
            "Sales Copilot",
            Slug::new("sales-copilot").expect("slug"),
            AgentKind::Standard,
        )
        .expect("agent");
        let response = agent_response(&agent);
        assert_eq!(response.name, "Sales Copilot");
        assert_eq!(response.slug, "sales-copilot");
        assert_eq!(response.status, "draft");
        assert!(response.current_version.is_none());
        assert!(response.active_version_id.is_none());
        assert!(response.tags.is_empty());
    }

    #[test]
    fn execution_step_projection_omits_payload_references_and_unknown_usage() {
        let execution_id = mas_common::ids::ExecutionId::new();
        let mut step = ExecutionStep::new(execution_id, "tool.invoke", 0, None)
            .with_input_ref("object://private/input");
        step.output_ref = Some("object://private/output".to_owned());
        let response = execution_step_response(&step);
        let json = serde_json::to_value(response).expect("safe DTO");
        assert_eq!(json["kind"], "tool.invoke");
        assert!(json.get("input_ref").is_none());
        assert!(json.get("output_ref").is_none());
        assert!(json.get("tokens_input").is_none());
        assert!(json.get("tokens_output").is_none());
        assert!(json.get("latency_ms").is_none());
    }

    #[test]
    fn execution_maps_usage_and_flags() {
        let execution = Execution::new(
            TenantId::new(),
            OrganizationId::new(),
            ProjectId::new(),
            None,
            Some(AgentId::new()),
            serde_json::json!({"ticket": "T-1"}),
            "corr-map-1",
            "svc-test",
        )
        .expect("execution");
        assert_eq!(execution.status, ExecutionStatus::Pending);
        let response = execution_response(&execution);
        assert_eq!(response.correlation_id, "corr-map-1");
        assert!(!response.cancellation_requested);
        assert_eq!(response.usage.steps_executed, 0);
        assert!(response.output.is_none());
        let (id, root) = execution_root_hint(&execution);
        let _workflow_hint: Option<WorkflowId> = response.workflow_id;
        assert_eq!(id, root, "root is its own root");
    }
}
