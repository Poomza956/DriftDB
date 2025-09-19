//! PostgreSQL Session Management

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::Result;
use bytes::BytesMut;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use driftdb_core::Engine;
use crate::protocol::{self, Message, TransactionStatus};
use crate::executor::QueryExecutor;

pub struct SessionManager {
    engine: Arc<RwLock<Engine>>,
    max_connections: usize,
    current_connections: AtomicU32,
    next_process_id: AtomicU32,
    auth_db: protocol::auth::UserDb,
}

impl SessionManager {
    pub fn new(engine: Arc<RwLock<Engine>>, max_connections: usize) -> Self {
        Self {
            engine,
            max_connections,
            current_connections: AtomicU32::new(0),
            next_process_id: AtomicU32::new(1000),
            auth_db: protocol::auth::UserDb::new(),
        }
    }

    pub async fn handle_connection(
        self: Arc<Self>,
        mut stream: TcpStream,
        addr: SocketAddr,
    ) -> Result<()> {
        // Check connection limit
        let conn_count = self.current_connections.fetch_add(1, Ordering::SeqCst);
        if conn_count >= self.max_connections as u32 {
            self.current_connections.fetch_sub(1, Ordering::SeqCst);
            warn!("Connection limit reached, rejecting {}", addr);
            return Ok(());
        }

        // Create session
        let process_id = self.next_process_id.fetch_add(1, Ordering::SeqCst) as i32;
        let secret_key = rand::random::<i32>();

        let session = Session {
            process_id,
            secret_key,
            addr,
            username: None,
            database: "driftdb".to_string(),
            transaction_status: TransactionStatus::Idle,
            engine: self.engine.clone(),
            auth_db: &self.auth_db,
        };

        // Handle session
        let result = session.run(&mut stream).await;

        // Clean up
        self.current_connections.fetch_sub(1, Ordering::SeqCst);

        if let Err(e) = result {
            error!("Session error from {}: {}", addr, e);
        } else {
            info!("Session closed for {}", addr);
        }

        Ok(())
    }
}

struct Session<'a> {
    process_id: i32,
    secret_key: i32,
    addr: SocketAddr,
    username: Option<String>,
    database: String,
    transaction_status: TransactionStatus,
    engine: Arc<RwLock<Engine>>,
    auth_db: &'a protocol::auth::UserDb,
}

impl<'a> Session<'a> {
    async fn run(mut self, stream: &mut TcpStream) -> Result<()> {
        let mut buffer = BytesMut::with_capacity(8192);
        let mut startup_done = false;

        loop {
            // Read from stream
            let n = stream.read_buf(&mut buffer).await?;
            if n == 0 {
                debug!("Connection closed by client {}", self.addr);
                break;
            }

            // Decode messages
            while let Some(msg) = protocol::codec::decode_message(&mut buffer, startup_done)? {
                debug!("Received message: {:?}", msg);

                match msg {
                    Message::SSLRequest => {
                        // We don't support SSL yet
                        stream.write_all(b"N").await?;
                    }

                    Message::StartupMessage { parameters, .. } => {
                        self.handle_startup(stream, parameters).await?;
                        startup_done = true;
                    }

                    Message::PasswordMessage { password } => {
                        if self.handle_password(stream, password).await? {
                            self.send_ready_for_query(stream).await?;
                        } else {
                            break;
                        }
                    }

                    Message::Query { sql } => {
                        self.handle_query(stream, &sql).await?;
                        self.send_ready_for_query(stream).await?;
                    }

                    Message::Terminate => {
                        debug!("Client requested termination");
                        break;
                    }

                    _ => {
                        warn!("Unhandled message type: {:?}", msg);
                        let error = Message::error(
                            protocol::error_codes::FEATURE_NOT_SUPPORTED,
                            "Message type not supported",
                        );
                        self.send_message(stream, &error).await?;
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle_startup(
        &mut self,
        stream: &mut TcpStream,
        parameters: std::collections::HashMap<String, String>,
    ) -> Result<()> {
        // Extract connection parameters
        self.username = parameters.get("user").cloned();
        if let Some(db) = parameters.get("database") {
            self.database = db.clone();
        }

        info!("Startup: user={:?}, database={}", self.username, self.database);

        // Send authentication request
        let auth_msg = Message::AuthenticationCleartextPassword;
        self.send_message(stream, &auth_msg).await?;

        Ok(())
    }

    async fn handle_password(
        &mut self,
        stream: &mut TcpStream,
        password: String,
    ) -> Result<bool> {
        let username = self.username.as_ref()
            .ok_or_else(|| anyhow::anyhow!("No username provided"))?;

        if self.auth_db.authenticate(username, &password) {
            info!("Authentication successful for {}", username);

            // Send authentication OK
            self.send_message(stream, &Message::AuthenticationOk).await?;

            // Send backend key data
            let key_data = Message::BackendKeyData {
                process_id: self.process_id,
                secret_key: self.secret_key,
            };
            self.send_message(stream, &key_data).await?;

            // Send parameter status messages
            self.send_parameter_status(stream, "server_version", "14.0 (DriftDB 0.2.0)").await?;
            self.send_parameter_status(stream, "server_encoding", "UTF8").await?;
            self.send_parameter_status(stream, "client_encoding", "UTF8").await?;
            self.send_parameter_status(stream, "DateStyle", "ISO, MDY").await?;

            Ok(true)
        } else {
            warn!("Authentication failed for {}", username);
            let error = Message::error(
                protocol::error_codes::INVALID_AUTHORIZATION,
                "Authentication failed",
            );
            self.send_message(stream, &error).await?;
            Ok(false)
        }
    }

    async fn handle_query(&mut self, stream: &mut TcpStream, sql: &str) -> Result<()> {
        info!("Query from {}: {}", self.addr, sql);

        // Execute query using our SQL executor
        let executor = QueryExecutor::new(self.engine.clone());
        match executor.execute(sql).await {
            Ok(result) => {
                self.send_query_result(stream, result).await?;
            }
            Err(e) => {
                error!("Query error: {}", e);
                let error = Message::error(
                    protocol::error_codes::SYNTAX_ERROR,
                    &format!("Query error: {}", e),
                );
                self.send_message(stream, &error).await?;
            }
        }

        Ok(())
    }

    async fn send_query_result(
        &mut self,
        stream: &mut TcpStream,
        result: crate::executor::QueryResult,
    ) -> Result<()> {
        use crate::executor::QueryResult;

        match result {
            QueryResult::Select { columns, rows } => {
                // Send row description
                let fields = columns.iter()
                    .map(|col| protocol::FieldDescription::new(
                        col.clone(),
                        protocol::DataType::Text,
                    ))
                    .collect();

                let row_desc = Message::RowDescription { fields };
                self.send_message(stream, &row_desc).await?;

                // Store row count before iteration
                let row_count = rows.len();

                // Send data rows
                for row in rows {
                    let values = row.iter()
                        .map(|val| {
                            protocol::value_to_postgres_text(val)
                                .map(|s| s.into_bytes())
                        })
                        .collect();

                    let data_row = Message::DataRow { values };
                    self.send_message(stream, &data_row).await?;
                }

                // Send command complete
                let complete = Message::CommandComplete {
                    tag: format!("SELECT {}", row_count),
                };
                self.send_message(stream, &complete).await?;
            }

            QueryResult::Insert { count } => {
                let complete = Message::CommandComplete {
                    tag: format!("INSERT 0 {}", count),
                };
                self.send_message(stream, &complete).await?;
            }

            QueryResult::Update { count } => {
                let complete = Message::CommandComplete {
                    tag: format!("UPDATE {}", count),
                };
                self.send_message(stream, &complete).await?;
            }

            QueryResult::Delete { count } => {
                let complete = Message::CommandComplete {
                    tag: format!("DELETE {}", count),
                };
                self.send_message(stream, &complete).await?;
            }

            QueryResult::CreateTable => {
                let complete = Message::CommandComplete {
                    tag: "CREATE TABLE".to_string(),
                };
                self.send_message(stream, &complete).await?;
            }

            QueryResult::Empty => {
                self.send_message(stream, &Message::EmptyQueryResponse).await?;
            }
        }

        Ok(())
    }

    async fn send_ready_for_query(&self, stream: &mut TcpStream) -> Result<()> {
        let msg = Message::ReadyForQuery {
            status: self.transaction_status.to_byte(),
        };
        self.send_message(stream, &msg).await
    }

    async fn send_parameter_status(
        &self,
        stream: &mut TcpStream,
        name: &str,
        value: &str,
    ) -> Result<()> {
        let msg = Message::ParameterStatus {
            name: name.to_string(),
            value: value.to_string(),
        };
        self.send_message(stream, &msg).await
    }

    async fn send_message(&self, stream: &mut TcpStream, msg: &Message) -> Result<()> {
        let bytes = protocol::codec::encode_message(msg);
        stream.write_all(&bytes).await?;
        stream.flush().await?;
        Ok(())
    }
}