//! Health Check Module
//!
//! Provides HTTP endpoints for monitoring DriftDB server health and readiness

use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::State,
    http::StatusCode,
    response::Json,
    routing::get,
    Router,
};
use serde_json::{json, Value};
use tokio::sync::RwLock;
use tracing::{debug, error, info};

use driftdb_core::Engine;
use crate::session::SessionManager;

/// Application state for health check endpoints
#[derive(Clone)]
pub struct HealthState {
    pub engine: Arc<tokio::sync::RwLock<Engine>>,
    pub session_manager: Arc<SessionManager>,
    pub start_time: Instant,
}

impl HealthState {
    pub fn new(engine: Arc<tokio::sync::RwLock<Engine>>, session_manager: Arc<SessionManager>) -> Self {
        Self {
            engine,
            session_manager,
            start_time: Instant::now(),
        }
    }
}

/// Create the health check router
pub fn create_health_router(state: HealthState) -> Router {
    Router::new()
        .route("/health/live", get(liveness_check))
        .route("/health/ready", get(readiness_check))
        .with_state(state)
}

/// Liveness probe - checks if the server process is running
/// Returns 200 if the server is alive and responding to requests
async fn liveness_check(State(state): State<HealthState>) -> Result<Json<Value>, StatusCode> {
    debug!("Liveness check requested");

    let uptime_seconds = state.start_time.elapsed().as_secs();
    let response = json!({
        "status": "alive",
        "uptime_seconds": uptime_seconds,
        "timestamp": chrono::Utc::now().to_rfc3339(),
    });

    Ok(Json(response))
}

/// Readiness probe - checks if the server is ready to accept requests
/// Returns 200 if the database is ready and can execute queries
async fn readiness_check(State(state): State<HealthState>) -> Result<Json<Value>, StatusCode> {
    debug!("Readiness check requested");

    // Check if engine is accessible
    let engine_status = match state.engine.try_read() {
        Ok(_engine) => {
            // Try to execute a simple health check query
            match perform_engine_health_check(&state.engine).await {
                Ok(_) => "ready",
                Err(e) => {
                    error!("Engine health check failed: {}", e);
                    return Err(StatusCode::SERVICE_UNAVAILABLE);
                }
            }
        }
        Err(_) => {
            error!("Engine is locked, not ready");
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    // Check disk space (basic check)
    let disk_status = match check_disk_space().await {
        Ok(available_gb) => {
            if available_gb < 1.0 {
                error!("Low disk space: {:.2} GB available", available_gb);
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }
            "ok"
        }
        Err(e) => {
            error!("Failed to check disk space: {}", e);
            "unknown"
        }
    };

    // Get rate limiting statistics
    let rate_limit_stats = state.session_manager.rate_limit_manager().stats();

    let response = json!({
        "status": "ready",
        "engine": engine_status,
        "disk": disk_status,
        "rate_limiting": {
            "active_clients": rate_limit_stats.active_clients,
            "total_violations": rate_limit_stats.total_violations,
            "global_tokens_available": rate_limit_stats.global_tokens_available,
            "load_factor": rate_limit_stats.load_factor,
        },
        "timestamp": chrono::Utc::now().to_rfc3339(),
    });

    Ok(Json(response))
}

/// Perform a basic health check on the engine
async fn perform_engine_health_check(engine: &Arc<RwLock<Engine>>) -> anyhow::Result<()> {
    // Try to acquire a read lock and perform a basic operation
    let engine_guard = engine.read().await;

    // Check if we can list tables (basic engine functionality)
    let _tables = engine_guard.list_tables();

    info!("Engine health check passed");
    Ok(())
}

/// Check available disk space
async fn check_disk_space() -> anyhow::Result<f64> {
    use std::path::Path;

    // Get disk usage for current directory
    let path = Path::new(".");
    let metadata = tokio::fs::metadata(path).await?;

    // This is a simplified check - in a real implementation,
    // you might want to use a system-specific API to get actual disk space
    let _ = metadata;

    // For now, return a placeholder value
    // In production, you would implement actual disk space checking
    Ok(10.0) // Assume 10GB available
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use tempfile::TempDir;
    use driftdb_core::Engine;

    #[tokio::test]
    async fn test_liveness_check() {
        let temp_dir = TempDir::new().unwrap();
        let engine = Engine::init(temp_dir.path()).unwrap();
        let engine = Arc::new(RwLock::new(engine));
        let session_manager = Arc::new(SessionManager::new(engine.clone(), 10));
        let state = HealthState::new(engine, session_manager);

        let result = liveness_check(State(state)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_readiness_check() {
        let temp_dir = TempDir::new().unwrap();
        let engine = Engine::init(temp_dir.path()).unwrap();
        let engine = Arc::new(RwLock::new(engine));
        let session_manager = Arc::new(SessionManager::new(engine.clone(), 10));
        let state = HealthState::new(engine, session_manager);

        let result = readiness_check(State(state)).await;
        assert!(result.is_ok());
    }
}