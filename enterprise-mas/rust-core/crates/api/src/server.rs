//! Process bootstrap: bind HTTP + gRPC listeners and run them with
//! graceful shutdown (SIGINT / SIGTERM drain in-flight requests, the
//! accept loop stops immediately).

use mas_common::result::Result;
use std::net::SocketAddr;
use tokio::net::TcpListener;

use crate::grpc::GrpcServices;
use crate::state::AppState;

/// Bind configuration for one process instance.
#[derive(Debug, Clone, Copy)]
pub struct ServerConfig {
    /// HTTP listener address.
    pub http_addr: SocketAddr,
    /// gRPC listener address.
    pub grpc_addr: SocketAddr,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            http_addr: SocketAddr::from(([0, 0, 0, 0], 8080)),
            grpc_addr: SocketAddr::from(([0, 0, 0, 0], 50051)),
        }
    }
}

/// Runs HTTP + gRPC concurrently until shutdown. Returns when both servers
/// drained.
pub async fn serve(state: AppState, config: ServerConfig) -> Result<()> {
    let http_listener = TcpListener::bind(config.http_addr).await.map_err(|err| {
        mas_common::error::AppError::internal(format!("bind http {}: {err}", config.http_addr))
    })?;
    let http_local = http_listener.local_addr().map_err(|err| {
        mas_common::error::AppError::internal(format!("resolve http address: {err}"))
    })?;

    let grpc_services = GrpcServices::new(state.clone());
    let grpc_server = tonic::transport::Server::builder()
        .add_service(grpc_services.tenancy_server())
        .add_service(grpc_services.agent_server())
        .add_service(grpc_services.workflow_server())
        .add_service(grpc_services.execution_server())
        .add_service(grpc_services.schedule_server())
        .serve_with_shutdown(config.grpc_addr, shutdown_signal());

    tracing::info!(%http_local, grpc = %config.grpc_addr, "api process listening");
    let http_router = crate::router::router(state);
    let http_server = axum::serve(http_listener, http_router.into_make_service())
        .with_graceful_shutdown(shutdown_signal());

    let (http_result, grpc_result) = tokio::join!(http_server, grpc_server);
    if let Err(err) = http_result {
        return Err(mas_common::error::AppError::internal(format!(
            "http serve: {err}"
        )));
    }
    if let Err(err) = grpc_result {
        return Err(mas_common::error::AppError::internal(format!(
            "grpc serve: {err}"
        )));
    }
    tracing::info!("api process shutdown complete");
    Ok(())
}

/// Ctrl-C / SIGTERM combination future.
pub async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            tracing::error!(%err, "ctrl-c listener failed");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            },
            Err(err) => tracing::error!(%err, "sigterm listener failed"),
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => tracing::info!("shutdown requested (SIGINT)"),
        () = terminate => tracing::info!("shutdown requested (SIGTERM)"),
    }
}
