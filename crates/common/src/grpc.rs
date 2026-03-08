use anyhow::Result;
use std::net::SocketAddr;
use tonic::transport::Server;
use tonic_health::server::health_reporter;
use tonic_reflection::server::Builder as ReflectionBuilder;
use tracing::info;

/// Start a gRPC server with health checks, reflection, and graceful shutdown.
///
/// `service` must implement `NamedService` (e.g., the output of `XxxServer::new()`).
/// The server runs until SIGINT/SIGTERM.
pub async fn serve_grpc<S>(service: S, addr: SocketAddr, service_label: &str) -> Result<()>
where
    S: tonic::server::NamedService
        + tower::Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
            Error = std::convert::Infallible,
        > + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
{
    let (health_reporter, health_service) = health_reporter();
    health_reporter
        .set_serving::<S>()
        .await;

    info!(%addr, "Starting {service_label} gRPC server");

    let reflection_service = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(moria_proto::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .build_v1()?;

    Server::builder()
        .add_service(health_service)
        .add_service(reflection_service)
        .add_service(service)
        .serve_with_shutdown(addr, crate::signal::shutdown_signal(service_label))
        .await?;

    crate::telemetry::shutdown_tracing();
    info!("{service_label} service shut down gracefully");
    Ok(())
}
