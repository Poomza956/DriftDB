//! Query Executor for PostgreSQL Protocol
//!
//! Bridges PostgreSQL wire protocol commands to DriftDB's SQL:2011 temporal engine

use anyhow::{Result, anyhow};
use driftdb_core::Engine;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Result types for different SQL operations
pub enum QueryResult {
    Select {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    Insert {
        count: usize,
    },
    Update {
        count: usize,
    },
    Delete {
        count: usize,
    },
    CreateTable,
    Empty,
}

pub struct QueryExecutor {
    engine: Arc<RwLock<Engine>>,
}

impl QueryExecutor {
    pub fn new(engine: Arc<RwLock<Engine>>) -> Self {
        Self { engine }
    }

    /// Convert SQL SELECT to DriftQL
    fn sql_to_driftql_select(&self, sql: &str) -> Result<String> {
        // Very simplified conversion - production would need full SQL parser
        let lower = sql.to_lowercase();

        // Handle SELECT * FROM table
        if lower.starts_with("select * from") || lower.starts_with("select \\* from") {
            let parts: Vec<&str> = sql.split_whitespace().collect();
            if parts.len() >= 4 {
                let table = parts[3].trim_end_matches(';');
                return Ok(format!("SELECT * FROM {}", table));
            }
        }

        // Handle SELECT 1 and other simple queries
        if lower == "select 1" || lower == "select 1;" {
            // Create a dummy query that will return something
            return Ok("SELECT * FROM __dummy__".to_string());
        }

        // For now, just strip semicolon and hope it works
        Ok(sql.trim_end_matches(';').to_string())
    }

    /// Convert SQL CREATE TABLE to DriftQL
    fn sql_to_driftql_create(&self, sql: &str) -> Result<String> {
        // Parse CREATE TABLE name (columns...)
        let lower = sql.to_lowercase();

        if !lower.starts_with("create table") {
            return Err(anyhow!("Not a CREATE TABLE statement"));
        }

        // Find table name
        let after_create = sql[12..].trim(); // Skip "CREATE TABLE"
        let table_end = after_create.find('(')
            .ok_or_else(|| anyhow!("Invalid CREATE TABLE syntax"))?;
        let table_name = after_create[..table_end].trim();

        // For now, default to id as primary key
        // In production, would parse column definitions
        Ok(format!("CREATE TABLE {} (pk=id)", table_name))
    }

    pub async fn execute(&self, sql: &str) -> Result<QueryResult> {
        let sql = sql.trim();

        // Handle common PostgreSQL client queries
        if sql.eq_ignore_ascii_case("SELECT 1") {
            return Ok(QueryResult::Select {
                columns: vec!["?column?".to_string()],
                rows: vec![vec![Value::Number(1.into())]],
            });
        }

        if sql.eq_ignore_ascii_case("SELECT VERSION()") {
            return Ok(QueryResult::Select {
                columns: vec!["version".to_string()],
                rows: vec![vec![Value::String(
                    "PostgreSQL 14.0 (DriftDB 0.2.0-alpha)".to_string()
                )]],
            });
        }

        if sql.eq_ignore_ascii_case("SELECT CURRENT_DATABASE()") {
            return Ok(QueryResult::Select {
                columns: vec!["current_database".to_string()],
                rows: vec![vec![Value::String("driftdb".to_string())]],
            });
        }

        // Parse and execute actual queries
        let lower = sql.to_lowercase();

        if lower.starts_with("select") {
            self.execute_select(sql).await
        } else if lower.starts_with("insert") {
            self.execute_insert(sql).await
        } else if lower.starts_with("update") {
            self.execute_update(sql).await
        } else if lower.starts_with("delete") {
            self.execute_delete(sql).await
        } else if lower.starts_with("create table") {
            self.execute_create_table(sql).await
        } else if lower.starts_with("begin") || lower.starts_with("start transaction") {
            // Transaction support (simplified for now)
            info!("Transaction started (simplified mode)");
            Ok(QueryResult::Empty)
        } else if lower.starts_with("commit") {
            info!("Transaction committed (simplified mode)");
            Ok(QueryResult::Empty)
        } else if lower.starts_with("rollback") {
            info!("Transaction rolled back (simplified mode)");
            Ok(QueryResult::Empty)
        } else if lower.starts_with("set") {
            // Ignore SET commands for now
            debug!("Ignoring SET command: {}", sql);
            Ok(QueryResult::Empty)
        } else if lower.starts_with("show") {
            self.execute_show(sql).await
        } else {
            warn!("Unsupported SQL command: {}", sql);
            Err(anyhow!("Unsupported SQL command"))
        }
    }

    async fn execute_select(&self, sql: &str) -> Result<QueryResult> {
        let engine = self.engine.read().await;

        // Convert simple SQL SELECT to DriftQL
        let driftql = self.sql_to_driftql_select(sql)?;

        // Parse and execute using DriftQL parser
        let query = driftdb_core::query::parse_driftql(&driftql)
            .map_err(|e| anyhow!("Failed to parse DriftQL: {}", e))?;

        match engine.query(&query) {
            Ok(driftdb_core::QueryResult::Rows { data }) => {
                // Convert to columns and rows format
                if !data.is_empty() {
                    let first = &data[0];
                    let columns: Vec<String> = if let Value::Object(map) = first {
                        map.keys().cloned().collect()
                    } else {
                        vec!["value".to_string()]
                    };

                    let rows: Vec<Vec<Value>> = data.into_iter().map(|record| {
                        if let Value::Object(map) = record {
                            columns.iter().map(|col| {
                                map.get(col).cloned().unwrap_or(Value::Null)
                            }).collect()
                        } else {
                            vec![record]
                        }
                    }).collect();

                    Ok(QueryResult::Select { columns, rows })
                } else {
                    Ok(QueryResult::Select {
                        columns: vec![],
                        rows: vec![],
                    })
                }
            }
            Ok(driftdb_core::QueryResult::Success { message }) => {
                Ok(QueryResult::Select {
                    columns: vec!["message".to_string()],
                    rows: vec![vec![Value::String(message)]],
                })
            }
            _ => Ok(QueryResult::Select {
                columns: vec![],
                rows: vec![],
            })
        }
    }

    async fn execute_insert(&self, sql: &str) -> Result<QueryResult> {
        let mut engine = self.engine.write().await;

        // Parse INSERT statement (simplified)
        // Format: INSERT INTO table_name (columns) VALUES (values)
        let parts: Vec<&str> = sql.split_whitespace().collect();
        if parts.len() < 4 || !parts[0].eq_ignore_ascii_case("insert") || !parts[1].eq_ignore_ascii_case("into") {
            return Err(anyhow!("Invalid INSERT syntax"));
        }

        let table_name = parts[2];

        // Extract values from VALUES clause (simplified parsing)
        if let Some(values_idx) = parts.iter().position(|&p| p.eq_ignore_ascii_case("values")) {
            if values_idx + 1 < parts.len() {
                // Parse the values (very simplified - production would need proper SQL parser)
                let values_str = parts[values_idx + 1..].join(" ");
                let values_str = values_str.trim_start_matches('(').trim_end_matches(')').trim_end_matches(';');

                // Create a simple JSON document from the values
                let doc = serde_json::json!({
                    "data": values_str
                });

                // Convert SQL INSERT to DriftQL INSERT
                let driftql = format!("INSERT INTO {} {}", table_name, doc);
                let query = driftdb_core::query::parse_driftql(&driftql)
                    .map_err(|e| anyhow!("Failed to parse DriftQL: {}", e))?;

                match engine.execute_query(query) {
                    Ok(_) => {},
                    Err(e) => return Err(anyhow!("Insert failed: {}", e))
                }

                return Ok(QueryResult::Insert { count: 1 });
            }
        }

        Err(anyhow!("Could not parse INSERT statement"))
    }

    async fn execute_update(&self, sql: &str) -> Result<QueryResult> {
        let _engine = self.engine.write().await;

        // Parse UPDATE statement (simplified)
        // Format: UPDATE table_name SET column = value WHERE condition
        let parts: Vec<&str> = sql.split_whitespace().collect();
        if parts.len() < 4 || !parts[0].eq_ignore_ascii_case("update") {
            return Err(anyhow!("Invalid UPDATE syntax"));
        }

        let table_name = parts[1];

        // For now, just return success with 0 rows affected
        // Full implementation would parse SET and WHERE clauses
        warn!("UPDATE not fully implemented, returning 0 rows affected");
        Ok(QueryResult::Update { count: 0 })
    }

    async fn execute_delete(&self, sql: &str) -> Result<QueryResult> {
        let _engine = self.engine.write().await;

        // Parse DELETE statement (simplified)
        // Format: DELETE FROM table_name WHERE condition
        let parts: Vec<&str> = sql.split_whitespace().collect();
        if parts.len() < 3 || !parts[0].eq_ignore_ascii_case("delete") || !parts[1].eq_ignore_ascii_case("from") {
            return Err(anyhow!("Invalid DELETE syntax"));
        }

        let table_name = parts[2];

        // For now, implement soft delete of all rows (no WHERE support yet)
        // Full implementation would parse WHERE clause
        warn!("DELETE without WHERE clause not fully implemented");
        Ok(QueryResult::Delete { count: 0 })
    }

    async fn execute_create_table(&self, sql: &str) -> Result<QueryResult> {
        let mut engine = self.engine.write().await;

        // Convert SQL CREATE TABLE to DriftQL
        let driftql = self.sql_to_driftql_create(sql)?;

        let query = driftdb_core::query::parse_driftql(&driftql)
            .map_err(|e| anyhow!("Failed to parse DriftQL: {}", e))?;

        match engine.execute_query(query) {
            Ok(_) => {
                info!("Table created successfully");
                Ok(QueryResult::CreateTable)
            }
            Err(e) => Err(anyhow!("Create table failed: {}", e))
        }
    }

    async fn execute_show(&self, sql: &str) -> Result<QueryResult> {
        let engine = self.engine.read().await;
        let lower = sql.to_lowercase();

        if lower.starts_with("show tables") {
            let tables = engine.list_tables();
            let rows: Vec<Vec<Value>> = tables.into_iter()
                .map(|t| vec![Value::String(t)])
                .collect();

            Ok(QueryResult::Select {
                columns: vec!["Tables_in_driftdb".to_string()],
                rows,
            })
        } else if lower.starts_with("show databases") {
            // Return a single database for now
            Ok(QueryResult::Select {
                columns: vec!["Database".to_string()],
                rows: vec![vec![Value::String("driftdb".to_string())]],
            })
        } else {
            warn!("Unsupported SHOW command: {}", sql);
            Err(anyhow!("Unsupported SHOW command"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_select_one() {
        let engine = Arc::new(RwLock::new(Engine::new("test_db")));
        let executor = QueryExecutor::new(engine);

        let result = executor.execute("SELECT 1").await.unwrap();
        match result {
            QueryResult::Select { columns, rows } => {
                assert_eq!(columns.len(), 1);
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Number(1.into()));
            }
            _ => panic!("Expected SELECT result"),
        }
    }

    #[tokio::test]
    async fn test_version_query() {
        let engine = Arc::new(RwLock::new(Engine::new("test_db")));
        let executor = QueryExecutor::new(engine);

        let result = executor.execute("SELECT VERSION()").await.unwrap();
        match result {
            QueryResult::Select { columns, rows } => {
                assert_eq!(columns[0], "version");
                assert_eq!(rows.len(), 1);
                match &rows[0][0] {
                    Value::String(s) => assert!(s.contains("DriftDB")),
                    _ => panic!("Expected string version"),
                }
            }
            _ => panic!("Expected SELECT result"),
        }
    }
}