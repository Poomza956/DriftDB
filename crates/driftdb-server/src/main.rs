//! DriftDB Server with PostgreSQL Wire Protocol
//!
//! This server allows DriftDB to be accessed using any PostgreSQL client,
//! including psql, pgAdmin, DBeaver, and all PostgreSQL drivers.

mod protocol;
mod session;
mod executor;

use std::net::SocketAddr;
use std::sync::Arc;
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use tokio::net::TcpListener;
use tracing::{error, info};

use driftdb_core::Engine;
use session::SessionManager;

#[derive(Parser, Debug)]
#[command(name = "driftdb-server")]
#[command(about = "DriftDB Server with PostgreSQL wire protocol")]
struct Args {
    /// Database directory
    #[arg(short, long, env = "DRIFTDB_DATA_PATH", default_value = "./data")]
    data_path: PathBuf,

    /// Listen address
    #[arg(short, long, env = "DRIFTDB_LISTEN", default_value = "127.0.0.1:5433")]
    listen: SocketAddr,

    /// Maximum connections
    #[arg(short = 'c', long, env = "DRIFTDB_MAX_CONNECTIONS", default_value = "100")]
    max_connections: usize,

    /// Enable SQL:2011 temporal extensions
    #[arg(long, env = "DRIFTDB_TEMPORAL", default_value = "true")]
    enable_temporal: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("driftdb_server=info".parse()?)
        )
        .init();

    let args = Args::parse();

    info!(
        "Starting DriftDB Server v{} on {}",
        env!("CARGO_PKG_VERSION"),
        args.listen
    );

    // Initialize or open the database
    let engine = if args.data_path.exists() {
        info!("Opening existing database at {:?}", args.data_path);
        Engine::open(&args.data_path)?
    } else {
        info!("Initializing new database at {:?}", args.data_path);
        Engine::init(&args.data_path)?
    };

    let engine = Arc::new(tokio::sync::RwLock::new(engine));

    // Create session manager
    let session_manager = Arc::new(SessionManager::new(
        engine.clone(),
        args.max_connections,
    ));

    // Bind to address
    let listener = TcpListener::bind(args.listen).await?;
    info!("DriftDB Server listening on {}", args.listen);
    info!("Connect with: psql -h {} -p {} -d driftdb",
          args.listen.ip(),
          args.listen.port());

    // Accept connections
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                info!("New connection from {}", addr);

                let session_mgr = session_manager.clone();
                tokio::spawn(async move {
                    if let Err(e) = session_mgr.handle_connection(stream, addr).await {
                        error!("Connection error from {}: {}", addr, e);
                    }
                });
            }
            Err(e) => {
                error!("Failed to accept connection: {}", e);
            }
        }
    }
}