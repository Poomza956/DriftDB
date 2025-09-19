//! Query Executor for PostgreSQL Protocol
//!
//! Executes SQL queries directly against the DriftDB engine

use anyhow::{Result, anyhow};
use driftdb_core::Engine;
use serde_json::Value;
use std::sync::Arc;
use std::collections::HashMap;
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

    /// Parse WHERE clause into conditions
    fn parse_where_clause(&self, where_clause: &str) -> Result<Vec<(String, String, Value)>> {
        let mut conditions = Vec::new();

        // Split by AND (simple parser for now)
        let parts: Vec<&str> = where_clause.split(" AND ").collect();

        for part in parts {
            let trimmed = part.trim();

            // Parse column = value (support =, >, <, >=, <=, !=)
            let operators = ["!=", ">=", "<=", "=", ">", "<"];
            let mut found = false;

            for op in &operators {
                if let Some(op_pos) = trimmed.find(op) {
                    let column = trimmed[..op_pos].trim();
                    let value_str = trimmed[op_pos + op.len()..].trim();

                    // Parse value
                    let value = self.parse_sql_value(value_str)?;

                    conditions.push((column.to_string(), op.to_string(), value));
                    found = true;
                    break;
                }
            }

            if !found {
                return Err(anyhow!("Invalid WHERE condition: {}", trimmed));
            }
        }

        Ok(conditions)
    }

    /// Parse SQL value (string, number, boolean, null)
    fn parse_sql_value(&self, value_str: &str) -> Result<Value> {
        let trimmed = value_str.trim();

        // NULL
        if trimmed.eq_ignore_ascii_case("NULL") {
            return Ok(Value::Null);
        }

        // Boolean
        if trimmed.eq_ignore_ascii_case("TRUE") {
            return Ok(Value::Bool(true));
        }
        if trimmed.eq_ignore_ascii_case("FALSE") {
            return Ok(Value::Bool(false));
        }

        // String (single quotes)
        if trimmed.starts_with('\'') && trimmed.ends_with('\'') {
            let content = &trimmed[1..trimmed.len()-1];
            return Ok(Value::String(content.to_string()));
        }

        // Number
        if let Ok(n) = trimmed.parse::<i64>() {
            return Ok(Value::Number(n.into()));
        }
        if let Ok(f) = trimmed.parse::<f64>() {
            return Ok(Value::Number(serde_json::Number::from_f64(f).unwrap()));
        }

        // Default to string without quotes
        Ok(Value::String(trimmed.to_string()))
    }

    /// Check if a value matches a condition
    fn matches_condition(&self, field_value: &Value, operator: &str, condition_value: &Value) -> bool {
        match operator {
            "=" => field_value == condition_value,
            "!=" => field_value != condition_value,
            ">" => {
                if let (Value::Number(a), Value::Number(b)) = (field_value, condition_value) {
                    a.as_f64().unwrap_or(0.0) > b.as_f64().unwrap_or(0.0)
                } else {
                    false
                }
            },
            "<" => {
                if let (Value::Number(a), Value::Number(b)) = (field_value, condition_value) {
                    a.as_f64().unwrap_or(0.0) < b.as_f64().unwrap_or(0.0)
                } else {
                    false
                }
            },
            ">=" => {
                if let (Value::Number(a), Value::Number(b)) = (field_value, condition_value) {
                    a.as_f64().unwrap_or(0.0) >= b.as_f64().unwrap_or(0.0)
                } else {
                    false
                }
            },
            "<=" => {
                if let (Value::Number(a), Value::Number(b)) = (field_value, condition_value) {
                    a.as_f64().unwrap_or(0.0) <= b.as_f64().unwrap_or(0.0)
                } else {
                    false
                }
            },
            _ => false,
        }
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

        if sql.to_lowercase().contains("select version()") {
            return Ok(QueryResult::Select {
                columns: vec!["version".to_string()],
                rows: vec![vec![Value::String(
                    "PostgreSQL 14.0 (DriftDB 0.3.0-alpha)".to_string()
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
            return Ok(QueryResult::Empty);
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
            info!("Transaction started");
            Ok(QueryResult::Empty)
        } else if lower.starts_with("commit") {
            info!("Transaction committed");
            Ok(QueryResult::Empty)
        } else if lower.starts_with("rollback") {
            info!("Transaction rolled back");
            Ok(QueryResult::Empty)
        } else {
            warn!("Unsupported SQL command: {}", sql);
            Err(anyhow!("Unsupported SQL command"))
        }
    }

    async fn execute_select(&self, sql: &str) -> Result<QueryResult> {
        let engine = self.engine.read().await;
        let lower = sql.to_lowercase();

        // Parse SELECT * FROM table [WHERE ...] [AS OF ...]
        if !lower.starts_with("select * from") && !lower.starts_with("select \\* from") {
            return Err(anyhow!("Only SELECT * queries are currently supported"));
        }

        // Extract table name
        let from_pos = lower.find(" from ").unwrap() + 6;
        let remaining = &sql[from_pos..];

        let table_end = remaining.find(" WHERE ")
            .or_else(|| remaining.find(" where "))
            .or_else(|| remaining.find(" AS OF "))
            .or_else(|| remaining.find(" as of "))
            .unwrap_or(remaining.len());

        let table_name = remaining[..table_end].trim();

        // Get table data
        let all_data = engine.get_table_data(table_name)
            .map_err(|e| anyhow!("Failed to get table data: {}", e))?;

        // Parse WHERE conditions if present
        let mut filtered_data = all_data;

        // Parse WHERE conditions first
        let where_conditions = if let Some(where_pos) = remaining.to_lowercase().find(" where ") {
            let where_start = where_pos + 7;
            let where_clause = &remaining[where_start..];

            let where_end = where_clause.to_lowercase().find(" as of ")
                .unwrap_or(where_clause.len());

            let conditions_str = where_clause[..where_end].trim();
            debug!("Parsing WHERE clause: {}", conditions_str);
            let conditions = self.parse_where_clause(conditions_str)?;
            debug!("Parsed conditions: {:?}", conditions);

            // Filter data based on WHERE conditions
            let initial_count = filtered_data.len();
            filtered_data = filtered_data.into_iter().filter(|row| {
                // Check all conditions (AND logic)
                let matches = conditions.iter().all(|(column, operator, value)| {
                    if let Value::Object(map) = row {
                        if let Some(field_value) = map.get(column) {
                            let result = self.matches_condition(field_value, operator, value);
                            debug!("Checking {} {} {:?} against {:?}: {}", column, operator, value, field_value, result);
                            result
                        } else {
                            debug!("Column {} not found in row", column);
                            false
                        }
                    } else {
                        false
                    }
                });
                matches
            }).collect();
            debug!("Filtered from {} to {} rows", initial_count, filtered_data.len());

            conditions
        } else {
            Vec::new()
        };

        // Handle AS OF for time travel
        let sequence_to_query = if remaining.to_lowercase().contains(" as of ") {
            // Parse AS OF clause: AS OF @seq:N
            if let Some(as_of_idx) = remaining.to_lowercase().find(" as of ") {
                let as_of_part = &remaining[as_of_idx + 7..].trim();
                if as_of_part.starts_with("@seq:") {
                    let seq_str = as_of_part[5..].split_whitespace().next().unwrap_or("");
                    match seq_str.parse::<u64>() {
                        Ok(seq) => {
                            debug!("Time travel query: reconstructing state at sequence {}", seq);
                            Some(seq)
                        }
                        Err(_) => {
                            warn!("Invalid sequence number in AS OF clause: {}", seq_str);
                            None
                        }
                    }
                } else {
                    // Could be timestamp format - not implemented yet
                    warn!("Timestamp-based time travel not yet implemented: {}", as_of_part);
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        // Reconstruct state at the specified sequence if time travel is requested
        let filtered_data = if let Some(seq) = sequence_to_query {
            // Time travel: reconstruct state at specific sequence
            let engine = self.engine.read().await;
            let mut time_travel_data = engine.get_table_data_at(&table_name, seq)
                .map_err(|e| anyhow!("Failed to reconstruct state at sequence {}: {}", seq, e))?;

            // Apply WHERE clause filtering to historical data
            if !where_conditions.is_empty() {
                let initial_count = time_travel_data.len();
                time_travel_data = time_travel_data.into_iter().filter(|record| {
                    where_conditions.iter().all(|(col, op, val)| {
                        if let Some(field_value) = record.get(col) {
                            self.matches_condition(field_value, op, val)
                        } else {
                            false
                        }
                    })
                }).collect();
                debug!("Time travel: Filtered from {} to {} rows", initial_count, time_travel_data.len());
            }

            time_travel_data
        } else {
            // Current data with WHERE filtering (existing logic)
            filtered_data
        };

        // Format results - ensure consistent column ordering
        if !filtered_data.is_empty() {
            let first = &filtered_data[0];
            // Use a deterministic column order (alphabetical for now)
            let mut columns: Vec<String> = if let Value::Object(map) = first {
                map.keys().cloned().collect()
            } else {
                vec!["value".to_string()]
            };
            columns.sort(); // Ensure consistent ordering

            let rows: Vec<Vec<Value>> = filtered_data.into_iter().map(|record| {
                if let Value::Object(map) = record {
                    columns.iter().map(|col| {
                        map.get(col).cloned().unwrap_or(Value::Null)
                    }).collect()
                } else {
                    vec![record]
                }
            }).collect();

            debug!("Returning {} rows with columns: {:?}", rows.len(), columns);
            Ok(QueryResult::Select { columns, rows })
        } else {
            // Return empty result set with proper column names from table schema if possible
            // For now, just return empty columns and rows
            debug!("Query returned empty result set");
            Ok(QueryResult::Select {
                columns: vec![],
                rows: vec![],
            })
        }
    }

    async fn execute_insert(&self, sql: &str) -> Result<QueryResult> {
        let mut engine = self.engine.write().await;

        // Parse INSERT INTO table_name (columns) VALUES (values)
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
        let mut json_obj = serde_json::Map::new();

        if !columns.is_empty() && !values.is_empty() {
            // Parse values (simple comma-separated parser)
            let value_parts = self.parse_values_list(values)?;

            for (i, col) in columns.iter().enumerate() {
                if i < value_parts.len() {
                    json_obj.insert(col.to_string(), value_parts[i].clone());
                }
            }
        } else {
            return Err(anyhow!("Column names and values required"));
        }

        // Insert the document
        let doc = Value::Object(json_obj);
        engine.insert_record(table_name, doc)
            .map_err(|e| anyhow!("Insert failed: {}", e))?;

        Ok(QueryResult::Insert { count: 1 })
    }

    /// Parse comma-separated values, handling quoted strings
    fn parse_values_list(&self, values: &str) -> Result<Vec<Value>> {
        let mut result = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        let mut chars = values.chars().peekable();

        while let Some(ch) = chars.next() {
            match ch {
                '\'' => {
                    if in_quotes && chars.peek() == Some(&'\'') {
                        // Escaped quote
                        current.push('\'');
                        chars.next();
                    } else {
                        in_quotes = !in_quotes;
                        if !in_quotes {
                            // End of string value
                            result.push(Value::String(current.clone()));
                            current.clear();
                        }
                    }
                },
                ',' if !in_quotes => {
                    // End of value
                    if !current.is_empty() {
                        result.push(self.parse_sql_value(&current)?);
                        current.clear();
                    }
                },
                _ => {
                    if in_quotes || ch != ' ' {
                        current.push(ch);
                    } else if !current.is_empty() && ch == ' ' {
                        current.push(ch);
                    }
                }
            }
        }

        // Add last value
        if !current.is_empty() {
            result.push(self.parse_sql_value(&current)?);
        }

        Ok(result)
    }

    async fn execute_update(&self, sql: &str) -> Result<QueryResult> {
        let mut engine = self.engine.write().await;

        // Parse UPDATE table_name SET column = value [WHERE condition]
        let sql_clean = sql.trim().trim_end_matches(';');
        let lower = sql_clean.to_lowercase();

        if !lower.starts_with("update ") {
            return Err(anyhow!("Invalid UPDATE syntax"));
        }

        // Extract table name
        let after_update = &sql_clean[7..].trim(); // Skip "UPDATE "
        let set_pos = lower[7..].find(" set ")
            .ok_or_else(|| anyhow!("Missing SET clause"))?;
        let table_name = after_update[..set_pos].trim();

        // Extract SET clause
        let after_set = &after_update[set_pos + 5..].trim(); // Skip " SET "
        let where_pos = after_set.to_lowercase().find(" where ");

        let set_clause = if let Some(pos) = where_pos {
            &after_set[..pos]
        } else {
            after_set
        };

        // Parse SET assignments (column = value)
        let mut updates = HashMap::new();
        for assignment in set_clause.split(',') {
            let parts: Vec<&str> = assignment.trim().split('=').collect();
            if parts.len() != 2 {
                return Err(anyhow!("Invalid SET assignment: {}", assignment));
            }
            let column = parts[0].trim();
            let value = self.parse_sql_value(parts[1].trim())?;
            updates.insert(column.to_string(), value);
        }

        // Get all data from table
        let all_data = engine.get_table_data(table_name)
            .map_err(|e| anyhow!("Failed to get table data: {}", e))?;

        // Parse WHERE conditions if present
        let conditions = if let Some(pos) = where_pos {
            let where_clause = &after_set[pos + 7..].trim();
            Some(self.parse_where_clause(where_clause)?)
        } else {
            None
        };

        // Count updated rows
        let mut updated_count = 0;

        // Apply updates to matching rows
        for row in all_data {
            if let Value::Object(mut map) = row {
                // Check if row matches WHERE conditions (if any)
                let matches = if let Some(ref conds) = conditions {
                    conds.iter().all(|(column, operator, value)| {
                        if let Some(field_value) = map.get(column) {
                            self.matches_condition(field_value, operator, value)
                        } else {
                            false
                        }
                    })
                } else {
                    true // No WHERE clause means update all
                };

                if matches {
                    // Get primary key for this row
                    let primary_key = map.get("id").cloned()
                        .ok_or_else(|| anyhow!("Row missing primary key"))?;

                    // Apply updates to the row
                    for (col, val) in &updates {
                        map.insert(col.clone(), val.clone());
                    }

                    // Save updated row back to database
                    engine.update_record(table_name, primary_key, Value::Object(map))?;
                    updated_count += 1;
                }
            }
        }

        Ok(QueryResult::Update { count: updated_count })
    }

    async fn execute_delete(&self, sql: &str) -> Result<QueryResult> {
        let mut engine = self.engine.write().await;

        // Parse DELETE FROM table_name [WHERE condition]
        let sql_clean = sql.trim().trim_end_matches(';');
        let lower = sql_clean.to_lowercase();

        if !lower.starts_with("delete from ") {
            return Err(anyhow!("Invalid DELETE syntax"));
        }

        // Extract table name
        let after_delete = &sql_clean[12..].trim(); // Skip "DELETE FROM "
        let where_pos = after_delete.to_lowercase().find(" where ");

        let table_name = if let Some(pos) = where_pos {
            &after_delete[..pos].trim()
        } else {
            after_delete
        };

        // Get all data from table
        let all_data = engine.get_table_data(table_name)
            .map_err(|e| anyhow!("Failed to get table data: {}", e))?;

        // Parse WHERE conditions if present
        let conditions = if let Some(pos) = where_pos {
            let where_clause = &after_delete[pos + 7..].trim();
            Some(self.parse_where_clause(where_clause)?)
        } else {
            None
        };

        // Count deleted rows
        let mut deleted_count = 0;

        // Delete matching rows
        for row in all_data {
            if let Value::Object(map) = row {
                // Check if row matches WHERE conditions (if any)
                let matches = if let Some(ref conds) = conditions {
                    conds.iter().all(|(column, operator, value)| {
                        if let Some(field_value) = map.get(column) {
                            self.matches_condition(field_value, operator, value)
                        } else {
                            false
                        }
                    })
                } else {
                    true // No WHERE clause means delete all (dangerous!)
                };

                if matches {
                    // Get primary key for this row
                    let primary_key = map.get("id").cloned()
                        .ok_or_else(|| anyhow!("Row missing primary key"))?;

                    // Delete the row (soft delete for audit trail)
                    engine.delete_record(table_name, primary_key)?;
                    deleted_count += 1;
                }
            }
        }

        Ok(QueryResult::Delete { count: deleted_count })
    }

    async fn execute_create_table(&self, sql: &str) -> Result<QueryResult> {
        let mut engine = self.engine.write().await;

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

        // Parse column definitions (simplified for now)
        // Default to 'id' as primary key
        engine.create_table(table_name, "id", vec![])
            .map_err(|e| anyhow!("Create table failed: {}", e))?;

        info!("Table {} created successfully", table_name);
        Ok(QueryResult::CreateTable)
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
    async fn test_parse_where_clause() {
        let engine = Arc::new(RwLock::new(Engine::new("test_db")));
        let executor = QueryExecutor::new(engine);

        let conditions = executor.parse_where_clause("name = 'Alice' AND age > 25").unwrap();
        assert_eq!(conditions.len(), 2);
        assert_eq!(conditions[0].0, "name");
        assert_eq!(conditions[0].1, "=");
        assert_eq!(conditions[0].2, Value::String("Alice".to_string()));
        assert_eq!(conditions[1].0, "age");
        assert_eq!(conditions[1].1, ">");
        assert_eq!(conditions[1].2, Value::Number(25.into()));
    }
}