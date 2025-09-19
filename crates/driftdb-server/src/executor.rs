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
        let sql = sql.trim().trim_end_matches(';');
        let lower = sql.to_lowercase();

        // Handle SELECT * FROM table
        if lower.starts_with("select * from") || lower.starts_with("select \\* from") {
            let parts: Vec<&str> = sql.split_whitespace().collect();
            if parts.len() >= 4 {
                let mut table = parts[3];

                // Handle WHERE clause
                if parts.len() > 4 && parts[4].eq_ignore_ascii_case("where") {
                    // For now, we don't support WHERE in DriftQL SELECT
                    // Just get the table name
                    table = parts[3];
                }

                // Handle AS OF for time travel
                if let Some(as_of_pos) = parts.iter().position(|&p| p.eq_ignore_ascii_case("as")) {
                    if as_of_pos + 2 < parts.len() && parts[as_of_pos + 1].eq_ignore_ascii_case("of") {
                        let timestamp = parts[as_of_pos + 2];
                        // Convert SQL timestamp to DriftQL sequence
                        if timestamp.starts_with("@seq:") {
                            return Ok(format!("SELECT * FROM {} AS OF {}", table, timestamp));
                        }
                    }
                }

                return Ok(format!("SELECT * FROM {}", table));
            }
        }

        // Handle SELECT 1 and other simple queries
        if lower == "select 1" || lower == "select 1;" {
            // Return a simple success result
            return Ok("__SELECT_ONE__".to_string());
        }

        // Handle SELECT with specific columns
        if lower.starts_with("select ") {
            // Try to extract table name after FROM
            if let Some(from_pos) = lower.find(" from ") {
                let table_part = sql[from_pos + 6..].trim();
                let table = table_part.split_whitespace().next().unwrap_or("");

                if !table.is_empty() {
                    return Ok(format!("SELECT * FROM {}", table));
                }
            }
        }

        // For unsupported queries, return error
        Err(anyhow!("SQL query not supported yet: {}", sql))
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
        if sql.eq_ignore_ascii_case("SELECT 1") || sql.eq_ignore_ascii_case("SELECT 1;") {
            return Ok(QueryResult::Select {
                columns: vec!["?column?".to_string()],
                rows: vec![vec![Value::Number(1.into())]],
            });
        }

        if sql.to_lowercase().contains("select version()") || sql.to_lowercase().contains("select version();") {
            return Ok(QueryResult::Select {
                columns: vec!["version".to_string()],
                rows: vec![vec![Value::String(
                    "PostgreSQL 14.0 (DriftDB 0.2.0-alpha)".to_string()
                )]],
            });
        }

        if sql.to_lowercase().contains("select current_database()") {
            return Ok(QueryResult::Select {
                columns: vec!["current_database".to_string()],
                rows: vec![vec![Value::String("driftdb".to_string())]],
            });
        }

        // Handle SHOW commands
        if sql.to_lowercase().starts_with("show ") {
            return self.execute_show(sql).await;
        }

        // Handle SET commands (ignore for now)
        if sql.to_lowercase().starts_with("set ") {
            return Ok(QueryResult::Select {
                columns: vec!["result".to_string()],
                rows: vec![vec![Value::String("SET".to_string())]],
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

        // Special handling for SELECT 1
        if driftql == "__SELECT_ONE__" {
            return Ok(QueryResult::Select {
                columns: vec!["?column?".to_string()],
                rows: vec![vec![Value::Number(1.into())]],
            });
        }

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

        // Parse INSERT statement
        // Format: INSERT INTO table_name (columns) VALUES (values)
        // or: INSERT INTO table_name VALUES (values)

        let sql_clean = sql.trim().trim_end_matches(';');
        let lower = sql_clean.to_lowercase();

        if !lower.starts_with("insert into ") {
            return Err(anyhow!("Invalid INSERT syntax"));
        }

        // Extract table name
        let after_into = &sql_clean[12..]; // Skip "INSERT INTO "
        let table_end = after_into.find(' ')
            .or_else(|| after_into.find('('))
            .ok_or_else(|| anyhow!("Cannot find table name"))?;
        let table_name = after_into[..table_end].trim();

        // Find VALUES clause
        let values_pos = lower.find(" values")
            .ok_or_else(|| anyhow!("Missing VALUES clause"))?;

        // Extract column names if present
        let columns_str = sql_clean[12 + table_end..values_pos].trim();
        let columns = if columns_str.starts_with('(') && columns_str.ends_with(')') {
            // Parse column names
            columns_str[1..columns_str.len()-1]
                .split(',')
                .map(|c| c.trim())
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

        // Extract values
        let values_str = sql_clean[values_pos + 7..].trim(); // Skip " VALUES"
        if !values_str.starts_with('(') || !values_str.ends_with(')') {
            return Err(anyhow!("Invalid VALUES syntax"));
        }

        let values = values_str[1..values_str.len()-1].trim();

        // Create JSON document
        // For simplicity, assume id is first value if columns are specified
        let doc = if !columns.is_empty() && !values.is_empty() {
            // Parse values (very simplified - assumes comma-separated)
            let value_parts: Vec<&str> = values.split(',').map(|v| v.trim()).collect();
            let mut json_obj = serde_json::Map::new();

            for (i, col) in columns.iter().enumerate() {
                if i < value_parts.len() {
                    let val = value_parts[i].trim_matches('\'').trim_matches('"');
                    // Try to parse as number, otherwise string
                    let json_val = if let Ok(n) = val.parse::<i64>() {
                        serde_json::Value::Number(n.into())
                    } else {
                        serde_json::Value::String(val.to_string())
                    };
                    json_obj.insert(col.to_string(), json_val);
                }
            }

            serde_json::Value::Object(json_obj)
        } else {
            // No columns specified, create simple object
            serde_json::json!({
                "id": values.split(',').next().unwrap_or("1").trim_matches('\'').trim_matches('"'),
                "data": values
            })
        };

        // Convert to DriftQL INSERT
        let driftql = format!("INSERT INTO {} {}", table_name, doc);
        let query = driftdb_core::query::parse_driftql(&driftql)
            .map_err(|e| anyhow!("Failed to parse DriftQL: {}", e))?;

        match engine.execute_query(query) {
            Ok(_) => Ok(QueryResult::Insert { count: 1 }),
            Err(e) => Err(anyhow!("Insert failed: {}", e))
        }
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