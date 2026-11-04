//! Tests for the closed Rust-owned runtime snapshot contract.

use mas_common::ids::{OrganizationId, ProjectId, TenantId, UserId};
use mas_domain::{Agent, AgentKind, AgentVersion, Slug};
use mas_execution::agent_bridge::AgentRuntimeSnapshot;
use serde_json::{json, Value};

fn agent() -> Agent {
    let mut agent = Agent::create(
        TenantId::new(),
        OrganizationId::new(),
        ProjectId::new(),
        "Snapshot Agent",
        Slug::new("snapshot-agent").expect("slug"),
        AgentKind::Standard,
    )
    .expect("agent");
    agent.enable().expect("enable");
    agent.record_published_version(1).expect("publish");
    agent
}

fn version(agent: &Agent, configuration: Value) -> AgentVersion {
    AgentVersion::publish(agent.id, 1, "a".repeat(64), configuration, UserId::new())
        .expect("published version")
}

#[test]
fn materializes_the_exact_published_configuration_and_scope() {
    let agent = agent();
    let version = version(
        &agent,
        json!({"model_hint":"provider/model", "system_prompt":"private test prompt"}),
    );
    let snapshot = AgentRuntimeSnapshot::from_domain(&agent, &version).expect("snapshot");
    assert_eq!(snapshot.agent_id, agent.id);
    assert_eq!(snapshot.tenant_id, agent.tenant_id);
    assert_eq!(snapshot.organization_id, agent.organization_id);
    assert_eq!(snapshot.project_id, agent.project_id);
    assert_eq!(snapshot.agent_version_id, version.id());
    assert_eq!(snapshot.configuration.system_prompt, "private test prompt");
    assert_eq!(
        snapshot.configuration.model_hint.as_deref(),
        Some("provider/model")
    );
    assert_eq!(snapshot.origin, "user");
    assert!(!snapshot.read_only);
    let encoded = serde_json::to_value(&snapshot).expect("serialize");
    assert_eq!(encoded["configuration"]["limits"]["max_steps"], 25);
    assert!(!format!("{snapshot:?}").contains("private test prompt"));
}

#[test]
fn rejects_other_agent_versions_and_disabled_agents() {
    let mut first = agent();
    let second = agent();
    let foreign = version(&second, json!({}));
    assert!(AgentRuntimeSnapshot::from_domain(&first, &foreign).is_err());
    let own = version(&first, json!({}));
    first.disable().expect("disable");
    assert!(AgentRuntimeSnapshot::from_domain(&first, &own).is_err());
}

#[test]
fn rejects_unknown_fields_secrets_and_out_of_bounds_limits() {
    let agent = agent();
    for configuration in [
        json!({"api_key":"should-not-cross-boundary"}),
        json!({"limits":{"max_steps":1001}}),
        json!({"limits":{"max_tool_calls":501}}),
        json!({"allowed_tools":["duplicate","duplicate"]}),
    ] {
        let version = version(&agent, configuration);
        assert!(AgentRuntimeSnapshot::from_domain(&agent, &version).is_err());
    }
}
