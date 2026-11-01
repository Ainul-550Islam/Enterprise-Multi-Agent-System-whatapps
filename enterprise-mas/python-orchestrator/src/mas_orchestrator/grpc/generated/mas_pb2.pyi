from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TransitionAction(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TRANSITION_ACTION_UNSPECIFIED: _ClassVar[TransitionAction]
    TRANSITION_ACTION_START: _ClassVar[TransitionAction]
    TRANSITION_ACTION_PAUSE: _ClassVar[TransitionAction]
    TRANSITION_ACTION_RESUME: _ClassVar[TransitionAction]
    TRANSITION_ACTION_CANCEL: _ClassVar[TransitionAction]

class ScheduleStateAction(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SCHEDULE_STATE_ACTION_UNSPECIFIED: _ClassVar[ScheduleStateAction]
    SCHEDULE_STATE_ACTION_PAUSE: _ClassVar[ScheduleStateAction]
    SCHEDULE_STATE_ACTION_RESUME: _ClassVar[ScheduleStateAction]
    SCHEDULE_STATE_ACTION_DISABLE: _ClassVar[ScheduleStateAction]
TRANSITION_ACTION_UNSPECIFIED: TransitionAction
TRANSITION_ACTION_START: TransitionAction
TRANSITION_ACTION_PAUSE: TransitionAction
TRANSITION_ACTION_RESUME: TransitionAction
TRANSITION_ACTION_CANCEL: TransitionAction
SCHEDULE_STATE_ACTION_UNSPECIFIED: ScheduleStateAction
SCHEDULE_STATE_ACTION_PAUSE: ScheduleStateAction
SCHEDULE_STATE_ACTION_RESUME: ScheduleStateAction
SCHEDULE_STATE_ACTION_DISABLE: ScheduleStateAction

class Empty(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ExecutionReply(_message.Message):
    __slots__ = ("execution_id", "status", "tenant_id", "workflow_id", "agent_id", "root_execution_id", "correlation_id", "cancellation_requested", "replayed", "failure", "output_json")
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    AGENT_ID_FIELD_NUMBER: _ClassVar[int]
    ROOT_EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    CANCELLATION_REQUESTED_FIELD_NUMBER: _ClassVar[int]
    REPLAYED_FIELD_NUMBER: _ClassVar[int]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_JSON_FIELD_NUMBER: _ClassVar[int]
    execution_id: str
    status: str
    tenant_id: str
    workflow_id: str
    agent_id: str
    root_execution_id: str
    correlation_id: str
    cancellation_requested: bool
    replayed: bool
    failure: str
    output_json: str
    def __init__(self, execution_id: _Optional[str] = ..., status: _Optional[str] = ..., tenant_id: _Optional[str] = ..., workflow_id: _Optional[str] = ..., agent_id: _Optional[str] = ..., root_execution_id: _Optional[str] = ..., correlation_id: _Optional[str] = ..., cancellation_requested: _Optional[bool] = ..., replayed: _Optional[bool] = ..., failure: _Optional[str] = ..., output_json: _Optional[str] = ...) -> None: ...

class RegisterOrganizationRequest(_message.Message):
    __slots__ = ("legal_name", "display_name", "slug")
    LEGAL_NAME_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_NAME_FIELD_NUMBER: _ClassVar[int]
    SLUG_FIELD_NUMBER: _ClassVar[int]
    legal_name: str
    display_name: str
    slug: str
    def __init__(self, legal_name: _Optional[str] = ..., display_name: _Optional[str] = ..., slug: _Optional[str] = ...) -> None: ...

class OrganizationReply(_message.Message):
    __slots__ = ("id", "legal_name", "display_name", "slug")
    ID_FIELD_NUMBER: _ClassVar[int]
    LEGAL_NAME_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_NAME_FIELD_NUMBER: _ClassVar[int]
    SLUG_FIELD_NUMBER: _ClassVar[int]
    id: str
    legal_name: str
    display_name: str
    slug: str
    def __init__(self, id: _Optional[str] = ..., legal_name: _Optional[str] = ..., display_name: _Optional[str] = ..., slug: _Optional[str] = ...) -> None: ...

class ListOrganizationsReply(_message.Message):
    __slots__ = ("organizations",)
    ORGANIZATIONS_FIELD_NUMBER: _ClassVar[int]
    organizations: _containers.RepeatedCompositeFieldContainer[OrganizationReply]
    def __init__(self, organizations: _Optional[_Iterable[_Union[OrganizationReply, _Mapping]]] = ...) -> None: ...

class RegisterTenantRequest(_message.Message):
    __slots__ = ("name", "slug", "environment", "isolation")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SLUG_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    ISOLATION_FIELD_NUMBER: _ClassVar[int]
    name: str
    slug: str
    environment: str
    isolation: str
    def __init__(self, name: _Optional[str] = ..., slug: _Optional[str] = ..., environment: _Optional[str] = ..., isolation: _Optional[str] = ...) -> None: ...

class TenantReply(_message.Message):
    __slots__ = ("id", "name", "slug", "environment", "isolation")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SLUG_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    ISOLATION_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    slug: str
    environment: str
    isolation: str
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., slug: _Optional[str] = ..., environment: _Optional[str] = ..., isolation: _Optional[str] = ...) -> None: ...

class ListTenantsReply(_message.Message):
    __slots__ = ("tenants",)
    TENANTS_FIELD_NUMBER: _ClassVar[int]
    tenants: _containers.RepeatedCompositeFieldContainer[TenantReply]
    def __init__(self, tenants: _Optional[_Iterable[_Union[TenantReply, _Mapping]]] = ...) -> None: ...

class RegisterProjectRequest(_message.Message):
    __slots__ = ("name", "slug")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SLUG_FIELD_NUMBER: _ClassVar[int]
    name: str
    slug: str
    def __init__(self, name: _Optional[str] = ..., slug: _Optional[str] = ...) -> None: ...

class ProjectReply(_message.Message):
    __slots__ = ("id", "name", "slug")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SLUG_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    slug: str
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., slug: _Optional[str] = ...) -> None: ...

class InviteMembershipRequest(_message.Message):
    __slots__ = ("user_id", "role")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    role: str
    def __init__(self, user_id: _Optional[str] = ..., role: _Optional[str] = ...) -> None: ...

class MembershipReply(_message.Message):
    __slots__ = ("id", "user_id", "role", "status")
    ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    id: str
    user_id: str
    role: str
    status: str
    def __init__(self, id: _Optional[str] = ..., user_id: _Optional[str] = ..., role: _Optional[str] = ..., status: _Optional[str] = ...) -> None: ...

class RegisterAgentRequest(_message.Message):
    __slots__ = ("project_id", "name", "slug", "kind")
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SLUG_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    project_id: str
    name: str
    slug: str
    kind: str
    def __init__(self, project_id: _Optional[str] = ..., name: _Optional[str] = ..., slug: _Optional[str] = ..., kind: _Optional[str] = ...) -> None: ...

class AgentReply(_message.Message):
    __slots__ = ("id", "project_id", "tenant_id", "name", "slug", "kind", "status", "current_version")
    ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SLUG_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CURRENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    id: str
    project_id: str
    tenant_id: str
    name: str
    slug: str
    kind: str
    status: str
    current_version: int
    def __init__(self, id: _Optional[str] = ..., project_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., name: _Optional[str] = ..., slug: _Optional[str] = ..., kind: _Optional[str] = ..., status: _Optional[str] = ..., current_version: _Optional[int] = ...) -> None: ...

class GetAgentRequest(_message.Message):
    __slots__ = ("agent_id",)
    AGENT_ID_FIELD_NUMBER: _ClassVar[int]
    agent_id: str
    def __init__(self, agent_id: _Optional[str] = ...) -> None: ...

class ListAgentsRequest(_message.Message):
    __slots__ = ("project_id",)
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    project_id: str
    def __init__(self, project_id: _Optional[str] = ...) -> None: ...

class ListAgentsReply(_message.Message):
    __slots__ = ("agents",)
    AGENTS_FIELD_NUMBER: _ClassVar[int]
    agents: _containers.RepeatedCompositeFieldContainer[AgentReply]
    def __init__(self, agents: _Optional[_Iterable[_Union[AgentReply, _Mapping]]] = ...) -> None: ...

class PublishAgentVersionRequest(_message.Message):
    __slots__ = ("agent_id", "configuration_checksum", "configuration_snapshot_json", "published_by")
    AGENT_ID_FIELD_NUMBER: _ClassVar[int]
    CONFIGURATION_CHECKSUM_FIELD_NUMBER: _ClassVar[int]
    CONFIGURATION_SNAPSHOT_JSON_FIELD_NUMBER: _ClassVar[int]
    PUBLISHED_BY_FIELD_NUMBER: _ClassVar[int]
    agent_id: str
    configuration_checksum: str
    configuration_snapshot_json: str
    published_by: str
    def __init__(self, agent_id: _Optional[str] = ..., configuration_checksum: _Optional[str] = ..., configuration_snapshot_json: _Optional[str] = ..., published_by: _Optional[str] = ...) -> None: ...

class AgentVersionReply(_message.Message):
    __slots__ = ("id", "version_number", "checksum")
    ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_NUMBER_FIELD_NUMBER: _ClassVar[int]
    CHECKSUM_FIELD_NUMBER: _ClassVar[int]
    id: str
    version_number: int
    checksum: str
    def __init__(self, id: _Optional[str] = ..., version_number: _Optional[int] = ..., checksum: _Optional[str] = ...) -> None: ...

class CreateWorkflowRequest(_message.Message):
    __slots__ = ("project_id", "name")
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    project_id: str
    name: str
    def __init__(self, project_id: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...

class WorkflowReply(_message.Message):
    __slots__ = ("id", "project_id", "name", "node_count", "graph_version")
    ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    NODE_COUNT_FIELD_NUMBER: _ClassVar[int]
    GRAPH_VERSION_FIELD_NUMBER: _ClassVar[int]
    id: str
    project_id: str
    name: str
    node_count: int
    graph_version: int
    def __init__(self, id: _Optional[str] = ..., project_id: _Optional[str] = ..., name: _Optional[str] = ..., node_count: _Optional[int] = ..., graph_version: _Optional[int] = ...) -> None: ...

class WorkflowNodeMsg(_message.Message):
    __slots__ = ("node_key", "node_type", "name", "config_json")
    NODE_KEY_FIELD_NUMBER: _ClassVar[int]
    NODE_TYPE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    CONFIG_JSON_FIELD_NUMBER: _ClassVar[int]
    node_key: str
    node_type: str
    name: str
    config_json: str
    def __init__(self, node_key: _Optional[str] = ..., node_type: _Optional[str] = ..., name: _Optional[str] = ..., config_json: _Optional[str] = ...) -> None: ...

class WorkflowEdgeMsg(_message.Message):
    __slots__ = ("from_key", "to_key")
    FROM_KEY_FIELD_NUMBER: _ClassVar[int]
    TO_KEY_FIELD_NUMBER: _ClassVar[int]
    from_key: str
    to_key: str
    def __init__(self, from_key: _Optional[str] = ..., to_key: _Optional[str] = ...) -> None: ...

class UpdateWorkflowGraphRequest(_message.Message):
    __slots__ = ("workflow_id", "nodes", "edges")
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    NODES_FIELD_NUMBER: _ClassVar[int]
    EDGES_FIELD_NUMBER: _ClassVar[int]
    workflow_id: str
    nodes: _containers.RepeatedCompositeFieldContainer[WorkflowNodeMsg]
    edges: _containers.RepeatedCompositeFieldContainer[WorkflowEdgeMsg]
    def __init__(self, workflow_id: _Optional[str] = ..., nodes: _Optional[_Iterable[_Union[WorkflowNodeMsg, _Mapping]]] = ..., edges: _Optional[_Iterable[_Union[WorkflowEdgeMsg, _Mapping]]] = ...) -> None: ...

class UpdateWorkflowGraphReply(_message.Message):
    __slots__ = ("workflow_id", "topological_order")
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    TOPOLOGICAL_ORDER_FIELD_NUMBER: _ClassVar[int]
    workflow_id: str
    topological_order: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, workflow_id: _Optional[str] = ..., topological_order: _Optional[_Iterable[str]] = ...) -> None: ...

class GetWorkflowRequest(_message.Message):
    __slots__ = ("workflow_id",)
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    workflow_id: str
    def __init__(self, workflow_id: _Optional[str] = ...) -> None: ...

class ListWorkflowsRequest(_message.Message):
    __slots__ = ("project_id",)
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    project_id: str
    def __init__(self, project_id: _Optional[str] = ...) -> None: ...

class ListWorkflowsReply(_message.Message):
    __slots__ = ("workflows",)
    WORKFLOWS_FIELD_NUMBER: _ClassVar[int]
    workflows: _containers.RepeatedCompositeFieldContainer[WorkflowReply]
    def __init__(self, workflows: _Optional[_Iterable[_Union[WorkflowReply, _Mapping]]] = ...) -> None: ...

class SubmitExecutionRequest(_message.Message):
    __slots__ = ("project_id", "workflow_id", "agent_id", "input_json")
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    AGENT_ID_FIELD_NUMBER: _ClassVar[int]
    INPUT_JSON_FIELD_NUMBER: _ClassVar[int]
    project_id: str
    workflow_id: str
    agent_id: str
    input_json: str
    def __init__(self, project_id: _Optional[str] = ..., workflow_id: _Optional[str] = ..., agent_id: _Optional[str] = ..., input_json: _Optional[str] = ...) -> None: ...

class TransitionExecutionRequest(_message.Message):
    __slots__ = ("execution_id", "action")
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    execution_id: str
    action: TransitionAction
    def __init__(self, execution_id: _Optional[str] = ..., action: _Optional[_Union[TransitionAction, str]] = ...) -> None: ...

class GetExecutionRequest(_message.Message):
    __slots__ = ("execution_id",)
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    execution_id: str
    def __init__(self, execution_id: _Optional[str] = ...) -> None: ...

class ListExecutionsReply(_message.Message):
    __slots__ = ("executions",)
    EXECUTIONS_FIELD_NUMBER: _ClassVar[int]
    executions: _containers.RepeatedCompositeFieldContainer[ExecutionReply]
    def __init__(self, executions: _Optional[_Iterable[_Union[ExecutionReply, _Mapping]]] = ...) -> None: ...

class RegisterScheduleRequest(_message.Message):
    __slots__ = ("project_id", "name", "kind", "every_seconds", "task_template_json")
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    EVERY_SECONDS_FIELD_NUMBER: _ClassVar[int]
    TASK_TEMPLATE_JSON_FIELD_NUMBER: _ClassVar[int]
    project_id: str
    name: str
    kind: str
    every_seconds: int
    task_template_json: str
    def __init__(self, project_id: _Optional[str] = ..., name: _Optional[str] = ..., kind: _Optional[str] = ..., every_seconds: _Optional[int] = ..., task_template_json: _Optional[str] = ...) -> None: ...

class SetScheduleStateRequest(_message.Message):
    __slots__ = ("schedule_id", "action")
    SCHEDULE_ID_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    schedule_id: str
    action: ScheduleStateAction
    def __init__(self, schedule_id: _Optional[str] = ..., action: _Optional[_Union[ScheduleStateAction, str]] = ...) -> None: ...

class ScheduleReply(_message.Message):
    __slots__ = ("id", "name", "status")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    status: str
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., status: _Optional[str] = ...) -> None: ...

class ListSchedulesReply(_message.Message):
    __slots__ = ("schedules",)
    SCHEDULES_FIELD_NUMBER: _ClassVar[int]
    schedules: _containers.RepeatedCompositeFieldContainer[ScheduleReply]
    def __init__(self, schedules: _Optional[_Iterable[_Union[ScheduleReply, _Mapping]]] = ...) -> None: ...

class ResourceLimitsMsg(_message.Message):
    __slots__ = ("timeout_ms", "max_steps", "max_parallel_tasks", "max_tool_calls", "max_payload_bytes", "max_retries")
    TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
    MAX_STEPS_FIELD_NUMBER: _ClassVar[int]
    MAX_PARALLEL_TASKS_FIELD_NUMBER: _ClassVar[int]
    MAX_TOOL_CALLS_FIELD_NUMBER: _ClassVar[int]
    MAX_PAYLOAD_BYTES_FIELD_NUMBER: _ClassVar[int]
    MAX_RETRIES_FIELD_NUMBER: _ClassVar[int]
    timeout_ms: int
    max_steps: int
    max_parallel_tasks: int
    max_tool_calls: int
    max_payload_bytes: int
    max_retries: int
    def __init__(self, timeout_ms: _Optional[int] = ..., max_steps: _Optional[int] = ..., max_parallel_tasks: _Optional[int] = ..., max_tool_calls: _Optional[int] = ..., max_payload_bytes: _Optional[int] = ..., max_retries: _Optional[int] = ...) -> None: ...

class AgentRunRequest(_message.Message):
    __slots__ = ("agent_id", "agent_version", "input_json", "correlation_id", "request_id", "execution_id", "task_id", "tenant_id", "organization_id", "project_id", "protocol_version", "deadline_rfc3339", "limits", "token_budget_max", "idempotency_key", "identity", "task_attempt_id", "agent_snapshot_json")
    AGENT_ID_FIELD_NUMBER: _ClassVar[int]
    AGENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    INPUT_JSON_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ORGANIZATION_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    DEADLINE_RFC3339_FIELD_NUMBER: _ClassVar[int]
    LIMITS_FIELD_NUMBER: _ClassVar[int]
    TOKEN_BUDGET_MAX_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    IDENTITY_FIELD_NUMBER: _ClassVar[int]
    TASK_ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    AGENT_SNAPSHOT_JSON_FIELD_NUMBER: _ClassVar[int]
    agent_id: str
    agent_version: str
    input_json: str
    correlation_id: str
    request_id: str
    execution_id: str
    task_id: str
    tenant_id: str
    organization_id: str
    project_id: str
    protocol_version: str
    deadline_rfc3339: str
    limits: ResourceLimitsMsg
    token_budget_max: int
    idempotency_key: str
    identity: ServiceIdentityMsg
    task_attempt_id: str
    agent_snapshot_json: str
    def __init__(self, agent_id: _Optional[str] = ..., agent_version: _Optional[str] = ..., input_json: _Optional[str] = ..., correlation_id: _Optional[str] = ..., request_id: _Optional[str] = ..., execution_id: _Optional[str] = ..., task_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., organization_id: _Optional[str] = ..., project_id: _Optional[str] = ..., protocol_version: _Optional[str] = ..., deadline_rfc3339: _Optional[str] = ..., limits: _Optional[_Union[ResourceLimitsMsg, _Mapping]] = ..., token_budget_max: _Optional[int] = ..., idempotency_key: _Optional[str] = ..., identity: _Optional[_Union[ServiceIdentityMsg, _Mapping]] = ..., task_attempt_id: _Optional[str] = ..., agent_snapshot_json: _Optional[str] = ...) -> None: ...

class ServiceIdentityMsg(_message.Message):
    __slots__ = ("subject", "issuer", "scopes")
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    ISSUER_FIELD_NUMBER: _ClassVar[int]
    SCOPES_FIELD_NUMBER: _ClassVar[int]
    subject: str
    issuer: str
    scopes: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, subject: _Optional[str] = ..., issuer: _Optional[str] = ..., scopes: _Optional[_Iterable[str]] = ...) -> None: ...

class AgentUsageMsg(_message.Message):
    __slots__ = ("tokens_input", "tokens_output", "tool_calls", "steps", "active_time_ms")
    TOKENS_INPUT_FIELD_NUMBER: _ClassVar[int]
    TOKENS_OUTPUT_FIELD_NUMBER: _ClassVar[int]
    TOOL_CALLS_FIELD_NUMBER: _ClassVar[int]
    STEPS_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_TIME_MS_FIELD_NUMBER: _ClassVar[int]
    tokens_input: int
    tokens_output: int
    tool_calls: int
    steps: int
    active_time_ms: int
    def __init__(self, tokens_input: _Optional[int] = ..., tokens_output: _Optional[int] = ..., tool_calls: _Optional[int] = ..., steps: _Optional[int] = ..., active_time_ms: _Optional[int] = ...) -> None: ...

class AgentSucceeded(_message.Message):
    __slots__ = ("output_json", "usage", "model_key", "finish_reason")
    OUTPUT_JSON_FIELD_NUMBER: _ClassVar[int]
    USAGE_FIELD_NUMBER: _ClassVar[int]
    MODEL_KEY_FIELD_NUMBER: _ClassVar[int]
    FINISH_REASON_FIELD_NUMBER: _ClassVar[int]
    output_json: str
    usage: AgentUsageMsg
    model_key: str
    finish_reason: str
    def __init__(self, output_json: _Optional[str] = ..., usage: _Optional[_Union[AgentUsageMsg, _Mapping]] = ..., model_key: _Optional[str] = ..., finish_reason: _Optional[str] = ...) -> None: ...

class AgentFailed(_message.Message):
    __slots__ = ("code", "message", "retryable", "usage", "correlation_id", "request_id")
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    RETRYABLE_FIELD_NUMBER: _ClassVar[int]
    USAGE_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    code: str
    message: str
    retryable: bool
    usage: AgentUsageMsg
    correlation_id: str
    request_id: str
    def __init__(self, code: _Optional[str] = ..., message: _Optional[str] = ..., retryable: _Optional[bool] = ..., usage: _Optional[_Union[AgentUsageMsg, _Mapping]] = ..., correlation_id: _Optional[str] = ..., request_id: _Optional[str] = ...) -> None: ...

class AgentRunReply(_message.Message):
    __slots__ = ("succeeded", "failed")
    SUCCEEDED_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    succeeded: AgentSucceeded
    failed: AgentFailed
    def __init__(self, succeeded: _Optional[_Union[AgentSucceeded, _Mapping]] = ..., failed: _Optional[_Union[AgentFailed, _Mapping]] = ...) -> None: ...

class BridgeHealthReply(_message.Message):
    __slots__ = ("healthy", "detail")
    HEALTHY_FIELD_NUMBER: _ClassVar[int]
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    healthy: bool
    detail: str
    def __init__(self, healthy: _Optional[bool] = ..., detail: _Optional[str] = ...) -> None: ...

class SyncSourceRequest(_message.Message):
    __slots__ = ("tenant_id", "organization_id", "project_id", "correlation_id", "request_id", "protocol_version", "deadline_rfc3339", "identity", "source_id", "source_name", "source_kind", "configuration_json", "credential_secret_ref", "cursor", "required_roles", "denied_principals")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ORGANIZATION_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    DEADLINE_RFC3339_FIELD_NUMBER: _ClassVar[int]
    IDENTITY_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    SOURCE_KIND_FIELD_NUMBER: _ClassVar[int]
    CONFIGURATION_JSON_FIELD_NUMBER: _ClassVar[int]
    CREDENTIAL_SECRET_REF_FIELD_NUMBER: _ClassVar[int]
    CURSOR_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_ROLES_FIELD_NUMBER: _ClassVar[int]
    DENIED_PRINCIPALS_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    organization_id: str
    project_id: str
    correlation_id: str
    request_id: str
    protocol_version: str
    deadline_rfc3339: str
    identity: ServiceIdentityMsg
    source_id: str
    source_name: str
    source_kind: str
    configuration_json: str
    credential_secret_ref: str
    cursor: str
    required_roles: _containers.RepeatedScalarFieldContainer[str]
    denied_principals: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, tenant_id: _Optional[str] = ..., organization_id: _Optional[str] = ..., project_id: _Optional[str] = ..., correlation_id: _Optional[str] = ..., request_id: _Optional[str] = ..., protocol_version: _Optional[str] = ..., deadline_rfc3339: _Optional[str] = ..., identity: _Optional[_Union[ServiceIdentityMsg, _Mapping]] = ..., source_id: _Optional[str] = ..., source_name: _Optional[str] = ..., source_kind: _Optional[str] = ..., configuration_json: _Optional[str] = ..., credential_secret_ref: _Optional[str] = ..., cursor: _Optional[str] = ..., required_roles: _Optional[_Iterable[str]] = ..., denied_principals: _Optional[_Iterable[str]] = ...) -> None: ...

class SyncSourceReply(_message.Message):
    __slots__ = ("source_id", "discovered_count", "changed_count", "indexed_count", "failed_count", "next_cursor", "errors")
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    DISCOVERED_COUNT_FIELD_NUMBER: _ClassVar[int]
    CHANGED_COUNT_FIELD_NUMBER: _ClassVar[int]
    INDEXED_COUNT_FIELD_NUMBER: _ClassVar[int]
    FAILED_COUNT_FIELD_NUMBER: _ClassVar[int]
    NEXT_CURSOR_FIELD_NUMBER: _ClassVar[int]
    ERRORS_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    discovered_count: int
    changed_count: int
    indexed_count: int
    failed_count: int
    next_cursor: str
    errors: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, source_id: _Optional[str] = ..., discovered_count: _Optional[int] = ..., changed_count: _Optional[int] = ..., indexed_count: _Optional[int] = ..., failed_count: _Optional[int] = ..., next_cursor: _Optional[str] = ..., errors: _Optional[_Iterable[str]] = ...) -> None: ...

class IngestDocumentRequest(_message.Message):
    __slots__ = ("tenant_id", "organization_id", "project_id", "correlation_id", "request_id", "protocol_version", "deadline_rfc3339", "identity", "source_id", "document_id", "source_kind", "title", "mime_type", "content", "source_uri", "content_sha256", "required_roles", "denied_principals", "metadata_json")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ORGANIZATION_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    DEADLINE_RFC3339_FIELD_NUMBER: _ClassVar[int]
    IDENTITY_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    DOCUMENT_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_KIND_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    MIME_TYPE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    SOURCE_URI_FIELD_NUMBER: _ClassVar[int]
    CONTENT_SHA256_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_ROLES_FIELD_NUMBER: _ClassVar[int]
    DENIED_PRINCIPALS_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    organization_id: str
    project_id: str
    correlation_id: str
    request_id: str
    protocol_version: str
    deadline_rfc3339: str
    identity: ServiceIdentityMsg
    source_id: str
    document_id: str
    source_kind: str
    title: str
    mime_type: str
    content: bytes
    source_uri: str
    content_sha256: str
    required_roles: _containers.RepeatedScalarFieldContainer[str]
    denied_principals: _containers.RepeatedScalarFieldContainer[str]
    metadata_json: str
    def __init__(self, tenant_id: _Optional[str] = ..., organization_id: _Optional[str] = ..., project_id: _Optional[str] = ..., correlation_id: _Optional[str] = ..., request_id: _Optional[str] = ..., protocol_version: _Optional[str] = ..., deadline_rfc3339: _Optional[str] = ..., identity: _Optional[_Union[ServiceIdentityMsg, _Mapping]] = ..., source_id: _Optional[str] = ..., document_id: _Optional[str] = ..., source_kind: _Optional[str] = ..., title: _Optional[str] = ..., mime_type: _Optional[str] = ..., content: _Optional[bytes] = ..., source_uri: _Optional[str] = ..., content_sha256: _Optional[str] = ..., required_roles: _Optional[_Iterable[str]] = ..., denied_principals: _Optional[_Iterable[str]] = ..., metadata_json: _Optional[str] = ...) -> None: ...

class DocumentIngestionReply(_message.Message):
    __slots__ = ("document_id", "sha256", "byte_count", "chunk_count", "embedded_count", "skipped_reason")
    DOCUMENT_ID_FIELD_NUMBER: _ClassVar[int]
    SHA256_FIELD_NUMBER: _ClassVar[int]
    BYTE_COUNT_FIELD_NUMBER: _ClassVar[int]
    CHUNK_COUNT_FIELD_NUMBER: _ClassVar[int]
    EMBEDDED_COUNT_FIELD_NUMBER: _ClassVar[int]
    SKIPPED_REASON_FIELD_NUMBER: _ClassVar[int]
    document_id: str
    sha256: str
    byte_count: int
    chunk_count: int
    embedded_count: int
    skipped_reason: str
    def __init__(self, document_id: _Optional[str] = ..., sha256: _Optional[str] = ..., byte_count: _Optional[int] = ..., chunk_count: _Optional[int] = ..., embedded_count: _Optional[int] = ..., skipped_reason: _Optional[str] = ...) -> None: ...

class PurgeDocumentRequest(_message.Message):
    __slots__ = ("tenant_id", "organization_id", "project_id", "correlation_id", "request_id", "protocol_version", "deadline_rfc3339", "identity", "source_id", "document_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ORGANIZATION_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    DEADLINE_RFC3339_FIELD_NUMBER: _ClassVar[int]
    IDENTITY_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    DOCUMENT_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    organization_id: str
    project_id: str
    correlation_id: str
    request_id: str
    protocol_version: str
    deadline_rfc3339: str
    identity: ServiceIdentityMsg
    source_id: str
    document_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., organization_id: _Optional[str] = ..., project_id: _Optional[str] = ..., correlation_id: _Optional[str] = ..., request_id: _Optional[str] = ..., protocol_version: _Optional[str] = ..., deadline_rfc3339: _Optional[str] = ..., identity: _Optional[_Union[ServiceIdentityMsg, _Mapping]] = ..., source_id: _Optional[str] = ..., document_id: _Optional[str] = ...) -> None: ...

class PurgeDocumentReply(_message.Message):
    __slots__ = ("document_id", "removed_chunks")
    DOCUMENT_ID_FIELD_NUMBER: _ClassVar[int]
    REMOVED_CHUNKS_FIELD_NUMBER: _ClassVar[int]
    document_id: str
    removed_chunks: int
    def __init__(self, document_id: _Optional[str] = ..., removed_chunks: _Optional[int] = ...) -> None: ...

class DataHubHealthReply(_message.Message):
    __slots__ = ("healthy", "detail")
    HEALTHY_FIELD_NUMBER: _ClassVar[int]
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    healthy: bool
    detail: str
    def __init__(self, healthy: _Optional[bool] = ..., detail: _Optional[str] = ...) -> None: ...
