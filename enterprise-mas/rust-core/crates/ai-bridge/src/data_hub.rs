//! PURPOSE: Production gRPC adapter for the Rust-owned Data Hub bridge port.
//!
//! Shares the AgentBridge channel, service identity, bearer credential,
//! body-bound internal signature, metadata conventions, deadline bounds and
//! canonical gRPC error mapper. No second authentication scheme is introduced.

use std::time::Duration;

use mas_common::error::AppError;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_execution::data_hub_bridge::{
    DataHubBridgePort, DataHubCallContext, DataHubDocumentIngestionResult,
    DataHubIngestDocumentRequest, DataHubPurgeDocumentRequest, DataHubPurgeDocumentResult,
    DataHubSyncSourceRequest, DataHubSyncSourceResult,
};
use prost::Message;
use tonic::transport::Channel;
use uuid::Uuid;

use crate::gen::data_hub_bridge_api_client::DataHubBridgeApiClient;
use crate::gen::{
    DataHubHealthReply, DocumentIngestionReply, IngestDocumentRequest, PurgeDocumentReply,
    PurgeDocumentRequest, ServiceIdentityMsg, SyncSourceReply, SyncSourceRequest,
};
use crate::{
    insert_metadata, map_status, GrpcAgentBridge, GrpcAgentBridgeConfig, AI_PROTOCOL_VERSION,
    AUTHORIZATION_HEADER, CORRELATION_HEADER, MINIMUM_RPC_TIMEOUT, ORGANIZATION_HEADER,
    PROTOCOL_VERSION_HEADER, REQUEST_ID_HEADER, TENANT_HEADER,
};

/// Maximum supported request body size for a Data Hub RPC.
pub const DEFAULT_MAX_REQUEST_BYTES: usize = 16 * 1024 * 1024;
/// Maximum accepted request size; prevents an accidental unbounded bridge payload.
pub const MAX_REQUEST_BYTES: usize = 64 * 1024 * 1024;
/// Maximum RPC timeout, matching the AgentBridge upper bound.
const MAXIMUM_RPC_TIMEOUT: Duration = Duration::from_secs(900);

/// Configuration for [`GrpcDataHubBridge`].
#[derive(Debug, Clone)]
pub struct GrpcDataHubBridgeConfig {
    /// Shared gRPC endpoint, TLS, service identity and authentication settings.
    pub agent_bridge: GrpcAgentBridgeConfig,
    /// Maximum protobuf-encoded request size, including message overhead.
    pub max_request_bytes: usize,
}

impl Default for GrpcDataHubBridgeConfig {
    fn default() -> Self {
        Self {
            agent_bridge: GrpcAgentBridgeConfig::default(),
            max_request_bytes: DEFAULT_MAX_REQUEST_BYTES,
        }
    }
}

/// Production Data Hub adapter over the Rust-owned `DataHubBridgeApi` proto.
#[derive(Debug, Clone)]
pub struct GrpcDataHubBridge {
    client: DataHubBridgeApiClient<Channel>,
    agent_bridge: GrpcAgentBridge,
    max_request_bytes: usize,
}

impl GrpcDataHubBridge {
    /// Connects to the same Python orchestrator endpoint and trust settings as
    /// the AgentBridge adapter.
    pub async fn connect(config: GrpcDataHubBridgeConfig) -> Result<Self> {
        if config.max_request_bytes == 0 || config.max_request_bytes > MAX_REQUEST_BYTES {
            return Err(AppError::validation(format!(
                "Data Hub max_request_bytes must be between 1 and {MAX_REQUEST_BYTES}"
            )));
        }
        let agent_bridge = GrpcAgentBridge::connect(config.agent_bridge).await?;
        Self::from_agent_bridge(agent_bridge, config.max_request_bytes)
    }

    /// Composes the Data Hub client from an already-connected AgentBridge.
    ///
    /// Both clients use that exact channel and trust configuration, so the
    /// Data Hub path cannot drift into a second endpoint, credential or
    /// service identity. A single worker can construct AgentBridge once and
    /// share the same client boundary with Data Hub calls.
    pub fn from_agent_bridge(
        agent_bridge: GrpcAgentBridge,
        max_request_bytes: usize,
    ) -> Result<Self> {
        if max_request_bytes == 0 || max_request_bytes > MAX_REQUEST_BYTES {
            return Err(AppError::validation(format!(
                "Data Hub max_request_bytes must be between 1 and {MAX_REQUEST_BYTES}"
            )));
        }
        let client = DataHubBridgeApiClient::new(agent_bridge.channel.clone())
            .max_decoding_message_size(agent_bridge.config.max_response_bytes)
            .max_encoding_message_size(max_request_bytes);
        Ok(Self {
            client,
            agent_bridge,
            max_request_bytes,
        })
    }

    /// Configured service identity shared with AgentBridge.
    fn identity(&self) -> ServiceIdentityMsg {
        self.agent_bridge.identity()
    }

    /// Computes the RPC timeout from the request's absolute deadline.
    fn rpc_timeout(&self, deadline: &Timestamp) -> Duration {
        let remaining = deadline
            .as_datetime()
            .signed_duration_since(chrono::Utc::now())
            .num_milliseconds()
            .max(0) as u64;
        let bounded =
            Duration::from_millis(remaining).clamp(MINIMUM_RPC_TIMEOUT, MAXIMUM_RPC_TIMEOUT);
        bounded.min(
            self.agent_bridge
                .config
                .default_timeout
                .max(MINIMUM_RPC_TIMEOUT),
        )
    }

    /// Applies exactly the authentication and identity metadata used by
    /// AgentBridge. The body is serialized before this call so the signature
    /// covers the exact protobuf bytes sent on the wire.
    fn authenticate<T: Message>(
        &self,
        request: &mut tonic::Request<T>,
        context: &DataHubCallContext,
        body: &[u8],
    ) -> Result<()> {
        let metadata = request.metadata_mut();
        insert_metadata(metadata, TENANT_HEADER, &context.tenant_id.to_string())?;
        insert_metadata(
            metadata,
            ORGANIZATION_HEADER,
            &context.organization_id.to_string(),
        )?;
        if let Some(token) = self.agent_bridge.config.bearer_token.as_ref() {
            insert_metadata(
                metadata,
                AUTHORIZATION_HEADER,
                &format!("Bearer {}", token.expose()),
            )?;
        }
        if let Some(secret) = self.agent_bridge.config.internal_signing_secret.as_ref() {
            let signature = mas_security::internal_service::sign(
                secret.expose().as_bytes(),
                &self.agent_bridge.config.internal_service_id,
                &Timestamp::now(),
                body,
            );
            insert_metadata(
                metadata,
                mas_security::internal_service::SIGNATURE_HEADER,
                &signature,
            )?;
            insert_metadata(
                metadata,
                mas_security::internal_service::SERVICE_HEADER,
                &self.agent_bridge.config.internal_service_id,
            )?;
        }
        insert_metadata(metadata, CORRELATION_HEADER, &context.correlation_id)?;
        insert_metadata(metadata, PROTOCOL_VERSION_HEADER, AI_PROTOCOL_VERSION)?;
        if let Some(request_id) = context.request_id.as_deref() {
            insert_metadata(metadata, REQUEST_ID_HEADER, request_id)?;
        }
        Ok(())
    }

    /// Encodes an individual request and enforces the configured body limit
    /// before signing or transmitting it.
    fn encode_bounded<T: Message>(&self, request: &T) -> Result<Vec<u8>> {
        let body = request.encode_to_vec();
        if body.len() > self.max_request_bytes {
            return Err(AppError::validation(format!(
                "Data Hub request is {} bytes; configured limit is {} bytes",
                body.len(),
                self.max_request_bytes
            )));
        }
        Ok(body)
    }

    fn encode_sync_request(&self, request: &DataHubSyncSourceRequest) -> Result<SyncSourceRequest> {
        request.context.require_authoritative_scope()?;
        ensure_not_nil(request.source_id, "source_id")?;
        if request.source_name.trim().is_empty() {
            return Err(AppError::validation(
                "Data Hub source name must not be empty",
            ));
        }
        validate_non_secret_config(&request.configuration)?;
        let configuration_json = serde_json::to_string(&request.configuration)
            .map_err(|error| AppError::serialization(format!("Data Hub source config: {error}")))?;
        Ok(SyncSourceRequest {
            tenant_id: request.context.tenant_id.to_string(),
            organization_id: request.context.organization_id.to_string(),
            project_id: request.context.project_id.to_string(),
            correlation_id: request.context.correlation_id.clone(),
            request_id: request.context.request_id.clone().unwrap_or_default(),
            protocol_version: AI_PROTOCOL_VERSION.to_owned(),
            deadline_rfc3339: request.context.deadline.to_rfc3339_millis(),
            identity: Some(self.identity()),
            source_id: request.source_id.to_string(),
            source_name: request.source_name.clone(),
            source_kind: request.source_kind.as_str().to_owned(),
            configuration_json,
            credential_secret_ref: request.credential_secret_ref.clone(),
            cursor: request.cursor.clone(),
            required_roles: request.required_roles.clone(),
            denied_principals: request.denied_principals.clone(),
        })
    }

    fn encode_ingest_request(
        &self,
        request: &DataHubIngestDocumentRequest,
    ) -> Result<IngestDocumentRequest> {
        request.context.require_authoritative_scope()?;
        ensure_not_nil(request.source_id, "source_id")?;
        ensure_not_nil(request.document_id, "document_id")?;
        if request.title.trim().is_empty() || request.mime_type.trim().is_empty() {
            return Err(AppError::validation(
                "Data Hub document title and MIME type must not be empty",
            ));
        }
        if let Some(expected) = request.content_sha256.as_deref() {
            validate_sha256(expected, "expected content SHA-256")?;
        }
        validate_non_secret_config(&request.metadata)?;
        let metadata_json = serde_json::to_string(&request.metadata).map_err(|error| {
            AppError::serialization(format!("Data Hub document metadata: {error}"))
        })?;
        Ok(IngestDocumentRequest {
            tenant_id: request.context.tenant_id.to_string(),
            organization_id: request.context.organization_id.to_string(),
            project_id: request.context.project_id.to_string(),
            correlation_id: request.context.correlation_id.clone(),
            request_id: request.context.request_id.clone().unwrap_or_default(),
            protocol_version: AI_PROTOCOL_VERSION.to_owned(),
            deadline_rfc3339: request.context.deadline.to_rfc3339_millis(),
            identity: Some(self.identity()),
            source_id: request.source_id.to_string(),
            document_id: request.document_id.to_string(),
            source_kind: request.source_kind.as_str().to_owned(),
            title: request.title.clone(),
            mime_type: request.mime_type.clone(),
            content: request.content.clone(),
            source_uri: request.source_uri.clone(),
            content_sha256: request.content_sha256.clone(),
            required_roles: request.required_roles.clone(),
            denied_principals: request.denied_principals.clone(),
            metadata_json,
        })
    }

    fn encode_purge_request(
        &self,
        request: &DataHubPurgeDocumentRequest,
    ) -> Result<PurgeDocumentRequest> {
        request.context.require_authoritative_scope()?;
        ensure_not_nil(request.source_id, "source_id")?;
        ensure_not_nil(request.document_id, "document_id")?;
        Ok(PurgeDocumentRequest {
            tenant_id: request.context.tenant_id.to_string(),
            organization_id: request.context.organization_id.to_string(),
            project_id: request.context.project_id.to_string(),
            correlation_id: request.context.correlation_id.clone(),
            request_id: request.context.request_id.clone().unwrap_or_default(),
            protocol_version: AI_PROTOCOL_VERSION.to_owned(),
            deadline_rfc3339: request.context.deadline.to_rfc3339_millis(),
            identity: Some(self.identity()),
            source_id: request.source_id.to_string(),
            document_id: request.document_id.to_string(),
        })
    }

    async fn race_cancellation<T, F>(context: &DataHubCallContext, call: F) -> Result<T>
    where
        F: std::future::Future<Output = std::result::Result<tonic::Response<T>, tonic::Status>>,
    {
        tokio::select! {
            biased;
            () = context.cancellation.cancelled() => {
                Err(AppError::cancelled("Data Hub request cancelled"))
            },
            outcome = call => outcome.map(tonic::Response::into_inner).map_err(map_status),
        }
    }
}

#[async_trait::async_trait]
impl DataHubBridgePort for GrpcDataHubBridge {
    fn name(&self) -> &'static str {
        "grpc-data-hub-bridge"
    }

    async fn sync_source(
        &self,
        request: DataHubSyncSourceRequest,
    ) -> Result<DataHubSyncSourceResult> {
        let wire = self.encode_sync_request(&request)?;
        let body = self.encode_bounded(&wire)?;
        let mut call = tonic::Request::new(wire);
        call.set_timeout(self.rpc_timeout(&request.context.deadline));
        self.authenticate(&mut call, &request.context, &body)?;
        let mut client = self.client.clone();
        let reply: SyncSourceReply =
            Self::race_cancellation(&request.context, client.sync_source(call)).await?;
        let source_id = parse_uuid(&reply.source_id, "source_id")?;
        if source_id != request.source_id {
            return Err(protocol_error(
                "SyncSource reply source id did not match the request",
            ));
        }
        Ok(DataHubSyncSourceResult {
            source_id,
            discovered_count: reply.discovered_count,
            changed_count: reply.changed_count,
            indexed_count: reply.indexed_count,
            failed_count: reply.failed_count,
            next_cursor: nonempty(reply.next_cursor),
            errors: reply.errors,
        })
    }

    async fn ingest_document(
        &self,
        request: DataHubIngestDocumentRequest,
    ) -> Result<DataHubDocumentIngestionResult> {
        let wire = self.encode_ingest_request(&request)?;
        let body = self.encode_bounded(&wire)?;
        let mut call = tonic::Request::new(wire);
        call.set_timeout(self.rpc_timeout(&request.context.deadline));
        self.authenticate(&mut call, &request.context, &body)?;
        let mut client = self.client.clone();
        let reply: DocumentIngestionReply =
            Self::race_cancellation(&request.context, client.ingest_document(call)).await?;
        let document_id = parse_uuid(&reply.document_id, "document_id")?;
        if document_id != request.document_id {
            return Err(protocol_error(
                "IngestDocument reply document id did not match the request",
            ));
        }
        validate_sha256(&reply.sha256, "document SHA-256")?;
        if let Some(expected) = request.content_sha256.as_deref() {
            if !reply.sha256.eq_ignore_ascii_case(expected) {
                return Err(protocol_error(
                    "IngestDocument reply digest did not match the expected digest",
                ));
            }
        }
        if reply.byte_count != request.content.len() as u64 {
            return Err(protocol_error(
                "IngestDocument reply byte count did not match the request",
            ));
        }
        if reply.embedded_count > reply.chunk_count {
            return Err(protocol_error(
                "IngestDocument reply embedded count exceeded its chunk count",
            ));
        }
        Ok(DataHubDocumentIngestionResult {
            document_id,
            sha256: reply.sha256.to_ascii_lowercase(),
            byte_count: reply.byte_count,
            chunk_count: reply.chunk_count,
            embedded_count: reply.embedded_count,
            skipped_reason: nonempty(reply.skipped_reason.unwrap_or_default()),
        })
    }

    async fn purge_document(
        &self,
        request: DataHubPurgeDocumentRequest,
    ) -> Result<DataHubPurgeDocumentResult> {
        let wire = self.encode_purge_request(&request)?;
        let body = self.encode_bounded(&wire)?;
        let mut call = tonic::Request::new(wire);
        call.set_timeout(self.rpc_timeout(&request.context.deadline));
        self.authenticate(&mut call, &request.context, &body)?;
        let mut client = self.client.clone();
        let reply: PurgeDocumentReply =
            Self::race_cancellation(&request.context, client.purge_document(call)).await?;
        let document_id = parse_uuid(&reply.document_id, "document_id")?;
        if document_id != request.document_id {
            return Err(protocol_error(
                "PurgeDocument reply document id did not match the request",
            ));
        }
        Ok(DataHubPurgeDocumentResult {
            document_id,
            removed_chunks: reply.removed_chunks,
        })
    }

    async fn health(&self) -> Result<()> {
        // The health RPC mirrors AgentBridge health: it reports liveness and
        // does not carry tenant content or perform an authenticated mutation.
        let mut client = self.client.clone();
        let mut call = tonic::Request::new(crate::gen::Empty {});
        call.set_timeout(Duration::from_secs(5));
        let reply: DataHubHealthReply = client.health(call).await.map_err(map_status)?.into_inner();
        if reply.healthy {
            Ok(())
        } else {
            Err(AppError::external_service(
                "data-hub",
                format!("Python Data Hub reports unhealthy: {}", reply.detail),
            ))
        }
    }
}

fn ensure_not_nil(id: Uuid, field: &'static str) -> Result<()> {
    if id.is_nil() {
        return Err(AppError::invalid_field(
            field,
            "required",
            "must be concrete",
        ));
    }
    Ok(())
}

fn parse_uuid(raw: &str, field: &'static str) -> Result<Uuid> {
    Uuid::parse_str(raw).map_err(|_| protocol_error(&format!("Data Hub reply had invalid {field}")))
}

fn validate_sha256(raw: &str, field: &'static str) -> Result<()> {
    if raw.len() != 64 || !raw.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(AppError::invalid_field(
            field,
            "invalid_digest",
            "must be a 64-character SHA-256 hex digest",
        ));
    }
    Ok(())
}

fn nonempty(raw: String) -> Option<String> {
    (!raw.is_empty()).then_some(raw)
}

fn protocol_error(message: &str) -> AppError {
    AppError::external_service("data-hub", message)
}

fn validate_non_secret_config(value: &serde_json::Value) -> Result<()> {
    match value {
        serde_json::Value::Object(object) => {
            for (key, value) in object {
                let normalized = key.to_ascii_lowercase().replace('-', "_");
                let is_reference = normalized.ends_with("_secret_ref")
                    || normalized.ends_with("_credential_ref")
                    || normalized == "secret_reference";
                if !is_reference
                    && [
                        "password",
                        "secret",
                        "token",
                        "api_key",
                        "apikey",
                        "access_key",
                        "client_key",
                        "private_key",
                        "authorization",
                        "credential",
                    ]
                    .iter()
                    .any(|fragment| normalized.contains(fragment))
                {
                    return Err(AppError::invalid_field(
                        "configuration",
                        "secret_material_forbidden",
                        "secret material must be provided through a secret reference",
                    ));
                }
                validate_non_secret_config(value)?;
            }
            Ok(())
        },
        serde_json::Value::Array(values) => {
            for value in values {
                validate_non_secret_config(value)?;
            }
            Ok(())
        },
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gen::agent_bridge_api_client::AgentBridgeApiClient;
    use mas_common::ids::{OrganizationId, ProjectId, TenantId};
    use mas_common::timestamps::Timestamp;
    use mas_execution::data_hub_bridge::DataHubSourceKind;
    use mas_orchestration::cancellation::CancellationToken;

    fn context() -> DataHubCallContext {
        DataHubCallContext {
            tenant_id: TenantId::new(),
            organization_id: OrganizationId::new(),
            project_id: ProjectId::new(),
            correlation_id: "corr-data-hub-1".to_owned(),
            request_id: Some("request-data-hub-1".to_owned()),
            deadline: Timestamp::now(),
            cancellation: CancellationToken::new(),
        }
    }

    fn adapter() -> GrpcDataHubBridge {
        let channel = Channel::from_static("http://127.0.0.1:1").connect_lazy();
        let agent_bridge = GrpcAgentBridge {
            client: AgentBridgeApiClient::new(channel.clone()),
            channel: channel.clone(),
            config: GrpcAgentBridgeConfig::default(),
        };
        GrpcDataHubBridge::from_agent_bridge(agent_bridge, DEFAULT_MAX_REQUEST_BYTES)
            .expect("shared AgentBridge composition")
    }

    #[tokio::test]
    async fn source_request_carries_authoritative_identity_and_secret_reference_only() {
        let adapter = adapter();
        let request = DataHubSyncSourceRequest {
            context: context(),
            source_id: Uuid::new_v4(),
            source_name: "Knowledge Base".to_owned(),
            source_kind: DataHubSourceKind::Web,
            configuration: serde_json::json!({"start_url": "https://docs.example.test"}),
            credential_secret_ref: Some("vault://tenant/source".to_owned()),
            cursor: None,
            required_roles: vec!["reader".to_owned()],
            denied_principals: vec![],
        };
        let wire = adapter.encode_sync_request(&request).expect("wire request");
        assert_eq!(wire.tenant_id, request.context.tenant_id.to_string());
        assert_eq!(
            wire.organization_id,
            request.context.organization_id.to_string()
        );
        assert_eq!(wire.project_id, request.context.project_id.to_string());
        assert_eq!(wire.source_kind, "web");
        assert_eq!(
            wire.credential_secret_ref.as_deref(),
            Some("vault://tenant/source")
        );
        assert_eq!(wire.identity.expect("identity").subject, "rust-core");
    }

    #[test]
    fn source_configuration_rejects_secret_material_but_allows_references() {
        assert!(validate_non_secret_config(&serde_json::json!({
            "credential_secret_ref": "vault://tenant/credential"
        }))
        .is_ok());
        let error = validate_non_secret_config(&serde_json::json!({"api_key": "do-not-send"}))
            .expect_err("raw credential must be rejected");
        assert!(matches!(error, AppError::Validation { .. }));
    }

    #[test]
    fn data_hub_reply_digest_must_be_a_sha256_hex_value() {
        assert!(validate_sha256(&"a".repeat(64), "digest").is_ok());
        assert!(validate_sha256("invalid", "digest").is_err());
    }
}
