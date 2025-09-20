//! PostgreSQL Session Management

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::{Result, anyhow};
use bytes::BytesMut;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use serde_json::Value;

use driftdb_core::{Engine, EnginePool, EngineGuard, RateLimitManager};
use crate::protocol::{self, Message, TransactionStatus};
use crate::executor::QueryExecutor;

pub struct SessionManager {
    engine_pool: EnginePool,
    next_process_id: AtomicU32,
    auth_db: Arc<protocol::auth::UserDb>,
    rate_limit_manager: Arc<RateLimitManager>,
}

impl SessionManager {
    pub fn new(engine_pool: EnginePool, auth_config: protocol::auth::AuthConfig, rate_limit_manager: Arc<RateLimitManager>) -> Self {
        Self {
            engine_pool,
            next_process_id: AtomicU32::new(1000),
            auth_db: Arc::new(protocol::auth::UserDb::new(auth_config)),
            rate_limit_manager,
        }
    }

    pub fn auth_db(&self) -> &Arc<protocol::auth::UserDb> {
        &self.auth_db
    }

    pub fn rate_limit_manager(&self) -> &Arc<RateLimitManager> {
        &self.rate_limit_manager
    }

    pub async fn handle_connection(
        self: Arc<Self>,
        mut stream: TcpStream,
        addr: SocketAddr,
    ) -> Result<()> {
        // Check rate limiting first
        if !self.rate_limit_manager.allow_connection(addr) {
            warn!("Connection rate limit exceeded for {}, dropping connection", addr);
            // Don't send any response - just drop the connection
            return Ok(());
        }

        // Try to acquire a connection from the pool
        let engine_guard = match self.engine_pool.acquire(addr).await {
            Ok(guard) => guard,
            Err(e) => {
                warn!("Connection limit reached or pool error, rejecting {}: {}", addr, e);
                // Release the rate limit connection since we're not using it
                self.rate_limit_manager.release_connection(addr);
                // Send an error response to the client before closing
                let _ = stream.write_all(b"N").await; // SSL not supported response
                return Ok(());
            }
        };

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
            engine_guard,
            auth_db: self.auth_db.clone(),
            rate_limit_manager: self.rate_limit_manager.clone(),
            authenticated: false,
            auth_challenge: None,
        };

        // Handle session
        let result = session.run(&mut stream).await;

        // Clean up rate limiting state
        self.rate_limit_manager.release_connection(addr);

        if let Err(e) = result {
            error!("Session error from {}: {}", addr, e);
        } else {
            info!("Session closed for {}", addr);
        }

        Ok(())
    }
}

struct Session {
    process_id: i32,
    secret_key: i32,
    addr: SocketAddr,
    username: Option<String>,
    database: String,
    transaction_status: TransactionStatus,
    engine_guard: EngineGuard,
    auth_db: Arc<protocol::auth::UserDb>,
    rate_limit_manager: Arc<RateLimitManager>,
    authenticated: bool,
    auth_challenge: Option<Vec<u8>>,
}

impl Session {
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
                        if !self.authenticated && self.auth_db.config().require_auth {
                            let error = Message::error(
                                protocol::error_codes::INVALID_AUTHORIZATION,
                                "Authentication required",
                            );
                            self.send_message(stream, &error).await?;
                        } else {
                            self.handle_query(stream, &sql).await?;
                            self.send_ready_for_query(stream).await?;
                        }
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

        let username = self.username.as_deref().unwrap_or("anonymous");
        info!("Startup: user={}, database={}", username, self.database);

        // Check authentication requirements
        let auth_config = self.auth_db.config();

        if !auth_config.require_auth || auth_config.method == protocol::auth::AuthMethod::Trust {
            // Trust authentication or auth disabled
            self.authenticated = true;
            let is_superuser = self.username.as_ref()
                .map(|u| self.auth_db.is_superuser(u))
                .unwrap_or(false);
            self.rate_limit_manager.set_client_auth(self.addr, true, is_superuser);
            self.send_message(stream, &Message::AuthenticationOk).await?;
            self.send_startup_complete(stream).await?;
        } else {
            // Require authentication
            match auth_config.method {
                protocol::auth::AuthMethod::MD5 => {
                    // Generate MD5 challenge
                    let salt = protocol::auth::generate_md5_challenge();
                    self.auth_challenge = Some(salt.to_vec());

                    let auth_msg = Message::AuthenticationMD5Password { salt };
                    self.send_message(stream, &auth_msg).await?;
                    // Return here to wait for password message
                    return Ok(());
                }
                protocol::auth::AuthMethod::ScramSha256 => {
                    // Generate SCRAM-SHA-256 challenge
                    if let Some(challenge) = protocol::auth::generate_auth_challenge(&auth_config.method) {
                        self.auth_challenge = Some(challenge.clone());

                        let auth_msg = Message::AuthenticationSASL {
                            mechanisms: vec!["SCRAM-SHA-256".to_string()],
                        };
                        self.send_message(stream, &auth_msg).await?;
                        // Return here to wait for SASL response
                        return Ok(());
                    }
                }
                protocol::auth::AuthMethod::Trust => {
                    // Already handled above
                    unreachable!()
                }
            }
        }

        Ok(())
    }

    async fn send_startup_complete(&self, stream: &mut TcpStream) -> Result<()> {
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
        self.send_parameter_status(stream, "application_name", "").await?;

        // Send ready for query
        self.send_ready_for_query(stream).await?;

        Ok(())
    }

    async fn handle_password(
        &mut self,
        stream: &mut TcpStream,
        password: String,
    ) -> Result<bool> {
        let username = self.username.as_ref()
            .ok_or_else(|| anyhow::anyhow!("No username provided"))?;

        let client_addr = self.addr.to_string();

        // Prepare challenge salt for MD5
        let challenge_salt = if self.auth_db.config().method == protocol::auth::AuthMethod::MD5 {
            self.auth_challenge.as_ref().and_then(|challenge| {
                if challenge.len() >= 4 {
                    let mut salt = [0u8; 4];
                    salt.copy_from_slice(&challenge[0..4]);
                    Some(salt)
                } else {
                    None
                }
            })
        } else {
            None
        };

        match self.auth_db.authenticate(username, &password, &client_addr, challenge_salt.as_ref()) {
            Ok(true) => {
                self.authenticated = true;
                let is_superuser = self.auth_db.is_superuser(username);
                self.rate_limit_manager.set_client_auth(self.addr, true, is_superuser);
                info!("Authentication successful for {} from {}", username, client_addr);

                // Send authentication OK
                self.send_message(stream, &Message::AuthenticationOk).await?;

                // Send startup completion
                self.send_startup_complete(stream).await?;

                Ok(true)
            }
            Ok(false) | Err(_) => {
                warn!("Authentication failed for {} from {}", username, client_addr);

                let error_msg = if self.auth_db.get_user_info(username).map_or(false, |user| user.is_locked()) {
                    "User account is temporarily locked due to failed login attempts"
                } else {
                    "Authentication failed"
                };

                let error = Message::error(
                    protocol::error_codes::INVALID_AUTHORIZATION,
                    error_msg,
                );
                self.send_message(stream, &error).await?;
                Ok(false)
            }
        }
    }

    async fn handle_query(&mut self, stream: &mut TcpStream, sql: &str) -> Result<()> {
        info!("Query from {}: {}", self.addr, sql);

        // Check rate limiting for this query
        if !self.rate_limit_manager.allow_query(self.addr, sql) {
            warn!("Query rate limit exceeded for {}: {}", self.addr, sql);
            let error = Message::error(
                protocol::error_codes::TOO_MANY_CONNECTIONS,
                "Rate limit exceeded. Please slow down your requests.",
            );
            self.send_message(stream, &error).await?;
            return Ok(());
        }

        let start_time = std::time::Instant::now();

        // Determine query type for metrics
        let query_type = determine_query_type(sql);

        // Check for user management commands first
        if let Some(result) = self.handle_user_management_query(sql).await? {
            self.send_query_result(stream, result).await?;
            return Ok(());
        }

        // Execute query using our SQL executor
        let executor = QueryExecutor::new_with_guard(&self.engine_guard);
        match executor.execute(sql).await {
            Ok(result) => {
                let duration = start_time.elapsed().as_secs_f64();

                // Record successful query metrics if registry is available
                if crate::metrics::REGISTRY.gather().len() > 0 {
                    crate::metrics::record_query(&query_type, "success", duration);
                }

                self.send_query_result(stream, result).await?;
            }
            Err(e) => {
                let duration = start_time.elapsed().as_secs_f64();

                error!("Query error: {}", e);

                // Record failed query metrics if registry is available
                if crate::metrics::REGISTRY.gather().len() > 0 {
                    crate::metrics::record_query(&query_type, "error", duration);
                    crate::metrics::record_error("query", &query_type);
                }

                let error = Message::error(
                    protocol::error_codes::SYNTAX_ERROR,
                    &format!("Query error: {}", e),
                );
                self.send_message(stream, &error).await?;
            }
        }

        Ok(())
    }

    async fn handle_user_management_query(&self, sql: &str) -> Result<Option<crate::executor::QueryResult>> {
        let sql_upper = sql.trim().to_uppercase();

        // CREATE USER command
        if sql_upper.starts_with("CREATE USER") {
            return self.handle_create_user(sql).await;
        }

        // DROP USER command
        if sql_upper.starts_with("DROP USER") {
            return self.handle_drop_user(sql).await;
        }

        // ALTER USER command (password change)
        if sql_upper.starts_with("ALTER USER") && sql_upper.contains("PASSWORD") {
            return self.handle_alter_user_password(sql).await;
        }

        // SHOW USERS command
        if sql_upper == "SHOW USERS" || sql_upper == "SELECT * FROM PG_USER" {
            return self.handle_show_users().await;
        }

        // SHOW AUTH_ATTEMPTS command
        if sql_upper == "SHOW AUTH_ATTEMPTS" {
            return self.handle_show_auth_attempts().await;
        }

        Ok(None)
    }

    async fn handle_create_user(&self, sql: &str) -> Result<Option<crate::executor::QueryResult>> {
        // Check if current user is superuser
        if let Some(username) = &self.username {
            if !self.auth_db.is_superuser(username) {
                return Err(anyhow!("Permission denied: only superusers can create users"));
            }
        } else {
            return Err(anyhow!("Permission denied: authentication required"));
        }

        // Parse CREATE USER statement (simplified)
        // Example: CREATE USER 'testuser' WITH PASSWORD 'testpass' SUPERUSER;
        let parts: Vec<&str> = sql.split_whitespace().collect();
        if parts.len() < 6 {
            return Err(anyhow!("Invalid CREATE USER syntax"));
        }

        let new_username = parts[2].trim_matches('\'').trim_matches('"');

        // Find password
        let password_pos = parts.iter().position(|&x| x.to_uppercase() == "PASSWORD");
        if password_pos.is_none() || password_pos.unwrap() + 1 >= parts.len() {
            return Err(anyhow!("Password required for CREATE USER"));
        }

        let password = parts[password_pos.unwrap() + 1].trim_matches('\'').trim_matches('"');

        // Check for SUPERUSER flag
        let is_superuser = sql.to_uppercase().contains("SUPERUSER");

        // Validate username and password
        protocol::auth::validate_username(new_username)?;
        protocol::auth::validate_password(password)?;

        // Create user
        self.auth_db.create_user(new_username.to_string(), password, is_superuser)?;

        Ok(Some(crate::executor::QueryResult::CreateTable))
    }

    async fn handle_drop_user(&self, sql: &str) -> Result<Option<crate::executor::QueryResult>> {
        // Check if current user is superuser
        if let Some(username) = &self.username {
            if !self.auth_db.is_superuser(username) {
                return Err(anyhow!("Permission denied: only superusers can drop users"));
            }
        } else {
            return Err(anyhow!("Permission denied: authentication required"));
        }

        // Parse DROP USER statement
        let parts: Vec<&str> = sql.split_whitespace().collect();
        if parts.len() < 3 {
            return Err(anyhow!("Invalid DROP USER syntax"));
        }

        let target_username = parts[2].trim_matches('\'').trim_matches('"');
        self.auth_db.drop_user(target_username)?;

        Ok(Some(crate::executor::QueryResult::Delete { count: 1 }))
    }

    async fn handle_alter_user_password(&self, sql: &str) -> Result<Option<crate::executor::QueryResult>> {
        // Users can change their own password, or superusers can change any password
        let current_user = self.username.as_ref()
            .ok_or_else(|| anyhow!("Authentication required"))?;

        // Parse ALTER USER statement
        // Example: ALTER USER 'username' PASSWORD 'newpassword';
        let parts: Vec<&str> = sql.split_whitespace().collect();
        if parts.len() < 5 {
            return Err(anyhow!("Invalid ALTER USER syntax"));
        }

        let target_username = parts[2].trim_matches('\'').trim_matches('"');

        // Check permissions
        if target_username != current_user && !self.auth_db.is_superuser(current_user) {
            return Err(anyhow!("Permission denied: can only change own password"));
        }

        let password_pos = parts.iter().position(|&x| x.to_uppercase() == "PASSWORD");
        if password_pos.is_none() || password_pos.unwrap() + 1 >= parts.len() {
            return Err(anyhow!("Password required"));
        }

        let new_password = parts[password_pos.unwrap() + 1].trim_matches('\'').trim_matches('"');
        protocol::auth::validate_password(new_password)?;

        self.auth_db.change_password(target_username, new_password)?;

        Ok(Some(crate::executor::QueryResult::Update { count: 1 }))
    }

    async fn handle_show_users(&self) -> Result<Option<crate::executor::QueryResult>> {
        // Check if current user is superuser or authenticated
        if let Some(username) = &self.username {
            if !self.auth_db.is_superuser(username) {
                return Err(anyhow!("Permission denied: only superusers can view user list"));
            }
        } else {
            return Err(anyhow!("Permission denied: authentication required"));
        }

        let usernames = self.auth_db.list_users();
        let mut rows = Vec::new();

        for username in usernames {
            if let Some(user) = self.auth_db.get_user_info(&username) {
                let row = vec![
                    Value::String(user.username),
                    Value::Bool(user.is_superuser),
                    Value::String(user.auth_method.to_string()),
                    Value::Number(serde_json::Number::from(user.created_at)),
                    match user.last_login {
                        Some(ts) => Value::Number(serde_json::Number::from(ts)),
                        None => Value::Null,
                    },
                    Value::Number(serde_json::Number::from(user.failed_attempts)),
                    match user.locked_until {
                        Some(ts) => Value::Number(serde_json::Number::from(ts)),
                        None => Value::Null,
                    },
                ];
                rows.push(row);
            }
        }

        Ok(Some(crate::executor::QueryResult::Select {
            columns: vec![
                "username".to_string(),
                "is_superuser".to_string(),
                "auth_method".to_string(),
                "created_at".to_string(),
                "last_login".to_string(),
                "failed_attempts".to_string(),
                "locked_until".to_string(),
            ],
            rows,
        }))
    }

    async fn handle_show_auth_attempts(&self) -> Result<Option<crate::executor::QueryResult>> {
        // Check if current user is superuser
        if let Some(username) = &self.username {
            if !self.auth_db.is_superuser(username) {
                return Err(anyhow!("Permission denied: only superusers can view auth attempts"));
            }
        } else {
            return Err(anyhow!("Permission denied: authentication required"));
        }

        let attempts = self.auth_db.get_recent_auth_attempts(100);
        let mut rows = Vec::new();

        for attempt in attempts {
            let row = vec![
                Value::String(attempt.username),
                Value::Number(serde_json::Number::from(attempt.timestamp)),
                Value::Bool(attempt.success),
                Value::String(attempt.client_addr),
            ];
            rows.push(row);
        }

        Ok(Some(crate::executor::QueryResult::Select {
            columns: vec![
                "username".to_string(),
                "timestamp".to_string(),
                "success".to_string(),
                "client_addr".to_string(),
            ],
            rows,
        }))
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

/// Determine the query type from SQL for metrics classification
fn determine_query_type(sql: &str) -> String {
    let sql_upper = sql.trim().to_uppercase();

    if sql_upper.starts_with("SELECT") {
        "SELECT"
    } else if sql_upper.starts_with("INSERT") {
        "INSERT"
    } else if sql_upper.starts_with("UPDATE") {
        "UPDATE"
    } else if sql_upper.starts_with("DELETE") {
        "DELETE"
    } else if sql_upper.starts_with("CREATE") {
        "CREATE"
    } else if sql_upper.starts_with("DROP") {
        "DROP"
    } else if sql_upper.starts_with("ALTER") {
        "ALTER"
    } else if sql_upper.starts_with("PATCH") {
        "PATCH"
    } else if sql_upper.starts_with("SOFT DELETE") {
        "SOFT_DELETE"
    } else {
        "OTHER"
    }.to_string()
}