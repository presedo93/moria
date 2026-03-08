use tracing::info;

/// Waits for SIGINT (Ctrl-C) or SIGTERM, then returns.
/// Use with `tokio::select!` or `serve_with_shutdown` for graceful shutdown.
pub async fn shutdown_signal(service: &str) {
    let ctrl_c = tokio::signal::ctrl_c();
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to register SIGTERM handler");

    tokio::select! {
        _ = ctrl_c => info!("{service}: received SIGINT, shutting down"),
        _ = sigterm.recv() => info!("{service}: received SIGTERM, shutting down"),
    }
}
