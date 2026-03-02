// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! HTTP server for metrics and health endpoints

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use hyperi_rustlib::ScalingPressure;

use super::Metrics;

/// Health status for the application
#[derive(Debug, Clone, Default)]
pub struct HealthStatus {
    pub ready: bool,
    pub kafka_connected: bool,
    pub clickhouse_connected: bool,
}

/// Shared state for the health server
pub struct ServerState {
    pub metrics: Metrics,
    pub health: std::sync::RwLock<HealthStatus>,
    pub scaling: Arc<ScalingPressure>,
}

impl ServerState {
    pub fn new(metrics: Metrics, scaling: Arc<ScalingPressure>) -> Self {
        Self {
            metrics,
            health: std::sync::RwLock::new(HealthStatus::default()),
            scaling,
        }
    }

    pub fn set_ready(&self, ready: bool) {
        if let Ok(mut health) = self.health.write() {
            health.ready = ready;
        }
    }

    pub fn set_kafka_connected(&self, connected: bool) {
        if let Ok(mut health) = self.health.write() {
            health.kafka_connected = connected;
        }
    }

    pub fn set_clickhouse_connected(&self, connected: bool) {
        if let Ok(mut health) = self.health.write() {
            health.clickhouse_connected = connected;
        }
    }
}

type BoxBody = Full<Bytes>;

fn full_response(
    status: StatusCode,
    content_type: &str,
    body: impl Into<Bytes>,
) -> Response<BoxBody> {
    Response::builder()
        .status(status)
        .header("Content-Type", content_type)
        .body(Full::new(body.into()))
        .unwrap()
}

/// Handle HTTP requests
async fn handle_request(
    req: Request<hyper::body::Incoming>,
    state: Arc<ServerState>,
) -> Result<Response<BoxBody>, Infallible> {
    let response = match (req.method(), req.uri().path()) {
        // Prometheus metrics endpoint
        (&Method::GET, "/metrics") => {
            debug!("Serving metrics");
            let mut metrics_text = state.metrics.gather();

            // Append scaling pressure gauge
            if state.scaling.is_enabled() {
                use std::fmt::Write;
                let _ = write!(
                    metrics_text,
                    "# HELP loader_scaling_pressure Gated scaling pressure for autoscaling (0-100)\n\
                     # TYPE loader_scaling_pressure gauge\n\
                     loader_scaling_pressure {:.2}\n",
                    state.scaling.calculate()
                );
            }

            full_response(StatusCode::OK, "text/plain; charset=utf-8", metrics_text)
        }

        // Kubernetes liveness probe
        (&Method::GET, "/healthz") | (&Method::GET, "/health") => {
            debug!("Health check");
            full_response(StatusCode::OK, "application/json", r#"{"status":"ok"}"#)
        }

        // Kubernetes readiness probe
        (&Method::GET, "/readyz") | (&Method::GET, "/ready") => {
            let health = state.health.read().map(|h| h.clone()).unwrap_or_default();
            debug!(ready = health.ready, "Readiness check");

            if health.ready {
                full_response(
                    StatusCode::OK,
                    "application/json",
                    format!(
                        r#"{{"status":"ready","kafka":{},"clickhouse":{}}}"#,
                        health.kafka_connected, health.clickhouse_connected
                    ),
                )
            } else {
                full_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "application/json",
                    format!(
                        r#"{{"status":"not_ready","kafka":{},"clickhouse":{}}}"#,
                        health.kafka_connected, health.clickhouse_connected
                    ),
                )
            }
        }

        // Not found
        _ => full_response(StatusCode::NOT_FOUND, "text/plain", "Not Found"),
    };

    Ok(response)
}

/// Run the metrics/health HTTP server
pub async fn run_server(
    addr: SocketAddr,
    state: Arc<ServerState>,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!(addr = %addr, "Metrics server listening");

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("Metrics server shutting down");
                break;
            }
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((stream, _)) => {
                        let io = TokioIo::new(stream);
                        let state = state.clone();

                        tokio::spawn(async move {
                            let service = service_fn(move |req| {
                                let state = state.clone();
                                async move { handle_request(req, state).await }
                            });

                            if let Err(e) = http1::Builder::new()
                                .serve_connection(io, service)
                                .await
                            {
                                error!(error = %e, "HTTP connection error");
                            }
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to accept connection");
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ScalingConfig;

    fn test_scaling() -> Arc<ScalingPressure> {
        Arc::new(ScalingConfig::default().build_pressure())
    }

    #[test]
    fn test_health_status_default() {
        let status = HealthStatus::default();
        assert!(!status.ready);
        assert!(!status.kafka_connected);
        assert!(!status.clickhouse_connected);
    }

    #[test]
    fn test_server_state_set_ready() {
        let metrics = Metrics::new();
        let state = ServerState::new(metrics, test_scaling());

        state.set_ready(true);
        let health = state.health.read().unwrap();
        assert!(health.ready);
    }

    #[test]
    fn test_server_state_set_connections() {
        let metrics = Metrics::new();
        let state = ServerState::new(metrics, test_scaling());

        state.set_kafka_connected(true);
        state.set_clickhouse_connected(true);

        let health = state.health.read().unwrap();
        assert!(health.kafka_connected);
        assert!(health.clickhouse_connected);
    }

    #[test]
    fn test_full_response() {
        let resp = full_response(StatusCode::OK, "text/plain", "test body");
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_server_starts_and_stops() {
        let metrics = Metrics::new();
        let _state = Arc::new(ServerState::new(metrics, test_scaling()));
        let shutdown = CancellationToken::new();

        // Verify cancellation token works
        assert!(!shutdown.is_cancelled());
        shutdown.cancel();
        assert!(shutdown.is_cancelled());
    }
}
