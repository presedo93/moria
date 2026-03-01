use anyhow::Result;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::runtime::Tokio;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Initialize tracing with console output and optional OTel export.
///
/// If `OTEL_EXPORTER_OTLP_ENDPOINT` is set, spans are exported via OTLP/gRPC.
/// Otherwise, only console logging is active.
pub fn init_tracing(service_name: &str) -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("info,{service_name}=debug")));

    let otel_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok();

    if let Some(endpoint) = otel_endpoint {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(&endpoint)
            .build()?;

        let tracer_provider = opentelemetry_sdk::trace::TracerProvider::builder()
            .with_batch_exporter(exporter, Tokio)
            .with_resource(opentelemetry_sdk::Resource::new(vec![
                KeyValue::new("service.name", service_name.to_string()),
            ]))
            .build();

        opentelemetry::global::set_tracer_provider(tracer_provider.clone());

        let otel_layer = tracing_opentelemetry::layer()
            .with_tracer(tracer_provider.tracer(service_name.to_string()));

        tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer())
            .with(otel_layer)
            .init();

        tracing::info!(%endpoint, "OpenTelemetry tracing initialized");
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer())
            .init();
    }

    Ok(())
}

/// Gracefully shut down the OTel tracer provider, flushing pending spans.
pub fn shutdown_tracing() {
    opentelemetry::global::shutdown_tracer_provider();
}
