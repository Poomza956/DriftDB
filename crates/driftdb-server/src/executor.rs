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
#[derive(Debug)]
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

/// Order direction
#[derive(Debug, Clone)]
enum OrderDirection {
    Asc,
    Desc,
}

/// Order by specification
#[derive(Debug, Clone)]
struct OrderBy {
    column: String,
    direction: OrderDirection,
}

/// Aggregation function types
#[derive(Debug, Clone, PartialEq)]
enum AggregationFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// Aggregation specification
#[derive(Debug, Clone)]
struct Aggregation {
    function: AggregationFunction,
    column: Option<String>, // None for COUNT(*)
}

/// Group by specification
#[derive(Debug, Clone)]
struct GroupBy {
    columns: Vec<String>,
}

/// Having clause specification (similar to WHERE but for groups)
#[derive(Debug, Clone)]
struct Having {
    conditions: Vec<(String, String, Value)>, // (function_expression, operator, value)
}

/// Select clause specification
#[derive(Debug, Clone)]
enum SelectClause {
    All, // SELECT *
    Columns(Vec<String>), // SELECT column1, column2, etc.
    Aggregations(Vec<Aggregation>), // SELECT COUNT(*), SUM(column), etc.
    Mixed(Vec<String>, Vec<Aggregation>), // SELECT column1, column2, COUNT(*), SUM(column3)
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

    /// Parse GROUP BY clause
    fn parse_group_by_clause(&self, group_by_clause: &str) -> Result<GroupBy> {
        let columns: Vec<String> = group_by_clause
            .split(',')
            .map(|col| col.trim().to_string())
            .collect();

        if columns.is_empty() || columns.iter().any(|col| col.is_empty()) {
            return Err(anyhow!("Invalid GROUP BY clause: {}", group_by_clause));
        }

        Ok(GroupBy { columns })
    }

    /// Parse HAVING clause
    fn parse_having_clause(&self, having_clause: &str) -> Result<Having> {
        let mut conditions = Vec::new();

        // Split by AND (simple parser for now)
        let parts: Vec<&str> = having_clause.split(" AND ").collect();

        for part in parts {
            let trimmed = part.trim();

            // Parse aggregation function conditions like AVG(salary) > 50000
            let operators = ["!=", ">=", "<=", "=", ">", "<"];
            let mut found = false;

            for op in &operators {
                if let Some(op_pos) = trimmed.find(op) {
                    let function_expr = trimmed[..op_pos].trim();
                    let value_str = trimmed[op_pos + op.len()..].trim();

                    // Validate that left side is an aggregation function
                    if !self.is_aggregation_function(function_expr) {
                        return Err(anyhow!("HAVING clause must use aggregation functions: {}", function_expr));
                    }

                    // Parse value
                    let value = self.parse_sql_value(value_str)?;

                    conditions.push((function_expr.to_string(), op.to_string(), value));
                    found = true;
                    break;
                }
            }

            if !found {
                return Err(anyhow!("Invalid HAVING condition: {}", trimmed));
            }
        }

        Ok(Having { conditions })
    }

    /// Check if an expression is an aggregation function
    fn is_aggregation_function(&self, expr: &str) -> bool {
        let expr = expr.trim().to_uppercase();
        expr.starts_with("COUNT(") || expr.starts_with("SUM(") ||
        expr.starts_with("AVG(") || expr.starts_with("MIN(") || expr.starts_with("MAX(")
    }

    /// Parse ORDER BY clause
    fn parse_order_by_clause(&self, order_by_clause: &str) -> Result<OrderBy> {
        let parts: Vec<&str> = order_by_clause.trim().split_whitespace().collect();

        if parts.is_empty() {
            return Err(anyhow!("Empty ORDER BY clause"));
        }

        let column = parts[0].to_string();
        let direction = if parts.len() > 1 {
            match parts[1].to_uppercase().as_str() {
                "ASC" => OrderDirection::Asc,
                "DESC" => OrderDirection::Desc,
                _ => return Err(anyhow!("Invalid ORDER BY direction: {}. Use ASC or DESC", parts[1])),
            }
        } else {
            OrderDirection::Asc // Default to ascending
        };

        Ok(OrderBy { column, direction })
    }

    /// Parse LIMIT clause
    fn parse_limit_clause(&self, limit_clause: &str) -> Result<usize> {
        let trimmed = limit_clause.trim();
        trimmed.parse::<usize>()
            .map_err(|_| anyhow!("Invalid LIMIT value: {}", trimmed))
    }

    /// Parse SELECT clause to determine if it's SELECT *, aggregation functions, or mixed
    fn parse_select_clause(&self, select_part: &str) -> Result<SelectClause> {
        let trimmed = select_part.trim();

        // Check for SELECT *
        if trimmed == "*" {
            return Ok(SelectClause::All);
        }

        // Parse columns and aggregation functions
        let mut columns = Vec::new();
        let mut aggregations = Vec::new();

        // Split by comma (simple parser for now)
        let parts: Vec<&str> = trimmed.split(',').collect();

        for part in parts {
            let part = part.trim();

            // Try to parse as aggregation function first
            if let Some(aggregation) = self.parse_aggregation_function(part)? {
                aggregations.push(aggregation);
            } else {
                // Treat as regular column
                columns.push(part.to_string());
            }
        }

        // Determine the type of SELECT clause
        match (columns.is_empty(), aggregations.is_empty()) {
            (true, true) => Err(anyhow!("No valid columns or aggregation functions found")),
            (true, false) => Ok(SelectClause::Aggregations(aggregations)),
            (false, true) => {
                // Just regular columns - return Columns variant for column selection
                Ok(SelectClause::Columns(columns))
            },
            (false, false) => Ok(SelectClause::Mixed(columns, aggregations)),
        }
    }

    /// Parse a single aggregation function like COUNT(*), SUM(column), etc.
    fn parse_aggregation_function(&self, expr: &str) -> Result<Option<Aggregation>> {
        let expr = expr.trim();

        // Check for function call pattern: FUNCTION(argument)
        if !expr.contains('(') || !expr.ends_with(')') {
            return Ok(None);
        }

        let paren_pos = expr.find('(').unwrap();
        let function_name = expr[..paren_pos].trim().to_uppercase();
        let argument = expr[paren_pos + 1..expr.len() - 1].trim();

        let function = match function_name.as_str() {
            "COUNT" => AggregationFunction::Count,
            "SUM" => AggregationFunction::Sum,
            "AVG" => AggregationFunction::Avg,
            "MIN" => AggregationFunction::Min,
            "MAX" => AggregationFunction::Max,
            _ => return Ok(None),
        };

        let column = if argument == "*" {
            // Only COUNT supports *
            if function != AggregationFunction::Count {
                return Err(anyhow!("{} function does not support * argument", function_name));
            }
            None
        } else {
            Some(argument.to_string())
        };

        Ok(Some(Aggregation { function, column }))
    }

    /// Compute aggregation results from filtered data
    fn compute_aggregations(&self, data: &[Value], aggregations: &[Aggregation]) -> Result<(Vec<String>, Vec<Value>)> {
        let mut columns = Vec::new();
        let mut values = Vec::new();

        for aggregation in aggregations {
            let (column_name, result) = self.compute_single_aggregation(data, aggregation)?;
            columns.push(column_name);
            values.push(result);
        }

        Ok((columns, values))
    }

    /// Group data by specified columns
    fn group_data(&self, data: Vec<Value>, group_by: &GroupBy) -> Result<HashMap<Vec<Value>, Vec<Value>>> {
        let mut groups: HashMap<Vec<Value>, Vec<Value>> = HashMap::new();

        for row in data {
            if let Value::Object(ref map) = row {
                // Extract group key values
                let mut group_key = Vec::new();
                for col in &group_by.columns {
                    let value = map.get(col).cloned().unwrap_or(Value::Null);
                    group_key.push(value);
                }

                // Add row to the appropriate group
                groups.entry(group_key).or_insert_with(Vec::new).push(row);
            } else {
                return Err(anyhow!("Invalid row format for grouping"));
            }
        }

        Ok(groups)
    }

    /// Compute aggregations for grouped data, returning results for each group
    fn compute_grouped_aggregations(
        &self,
        groups: &HashMap<Vec<Value>, Vec<Value>>,
        group_by: &GroupBy,
        aggregations: &[Aggregation]
    ) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
        let mut all_columns = group_by.columns.clone();
        let mut agg_columns = Vec::new();

        // Add aggregation column names
        for aggregation in aggregations {
            let column_name = match (&aggregation.function, &aggregation.column) {
                (AggregationFunction::Count, None) => "count(*)".to_string(),
                (AggregationFunction::Count, Some(col)) => format!("count({})", col),
                (AggregationFunction::Sum, Some(col)) => format!("sum({})", col),
                (AggregationFunction::Avg, Some(col)) => format!("avg({})", col),
                (AggregationFunction::Min, Some(col)) => format!("min({})", col),
                (AggregationFunction::Max, Some(col)) => format!("max({})", col),
                _ => return Err(anyhow!("Invalid aggregation configuration")),
            };
            agg_columns.push(column_name);
        }

        all_columns.extend(agg_columns);

        let mut all_rows = Vec::new();

        // Process each group
        for (group_key, group_data) in groups {
            let mut row = group_key.clone(); // Start with group key values

            // Compute aggregations for this group
            for aggregation in aggregations {
                let (_, result) = self.compute_single_aggregation(group_data, aggregation)?;
                row.push(result);
            }

            all_rows.push(row);
        }

        Ok((all_columns, all_rows))
    }

    /// Apply HAVING clause to filter groups
    fn apply_having_filter(
        &self,
        groups: HashMap<Vec<Value>, Vec<Value>>,
        having: &Having,
        aggregations: &[Aggregation]
    ) -> Result<HashMap<Vec<Value>, Vec<Value>>> {
        let mut filtered_groups = HashMap::new();

        for (group_key, group_data) in groups {
            let mut matches_having = true;

            // Check each HAVING condition
            for (function_expr, operator, expected_value) in &having.conditions {
                // Parse the aggregation function from the expression
                if let Some(aggregation) = self.parse_aggregation_function(function_expr)? {
                    let (_, actual_value) = self.compute_single_aggregation(&group_data, &aggregation)?;

                    if !self.matches_condition(&actual_value, operator, expected_value) {
                        matches_having = false;
                        break;
                    }
                } else {
                    return Err(anyhow!("Invalid aggregation function in HAVING: {}", function_expr));
                }
            }

            if matches_having {
                filtered_groups.insert(group_key, group_data);
            }
        }

        Ok(filtered_groups)
    }

    /// Compute a single aggregation function
    fn compute_single_aggregation(&self, data: &[Value], aggregation: &Aggregation) -> Result<(String, Value)> {
        let column_name = match (&aggregation.function, &aggregation.column) {
            (AggregationFunction::Count, None) => "count(*)".to_string(),
            (AggregationFunction::Count, Some(col)) => format!("count({})", col),
            (AggregationFunction::Sum, Some(col)) => format!("sum({})", col),
            (AggregationFunction::Avg, Some(col)) => format!("avg({})", col),
            (AggregationFunction::Min, Some(col)) => format!("min({})", col),
            (AggregationFunction::Max, Some(col)) => format!("max({})", col),
            _ => return Err(anyhow!("Invalid aggregation configuration")),
        };

        let result = match aggregation.function {
            AggregationFunction::Count => self.compute_count(data, &aggregation.column)?,
            AggregationFunction::Sum => self.compute_sum(data, aggregation.column.as_ref().unwrap())?,
            AggregationFunction::Avg => self.compute_avg(data, aggregation.column.as_ref().unwrap())?,
            AggregationFunction::Min => self.compute_min(data, aggregation.column.as_ref().unwrap())?,
            AggregationFunction::Max => self.compute_max(data, aggregation.column.as_ref().unwrap())?,
        };

        Ok((column_name, result))
    }

    /// Compute COUNT(*) or COUNT(column)
    fn compute_count(&self, data: &[Value], column: &Option<String>) -> Result<Value> {
        match column {
            None => {
                // COUNT(*) - count all rows
                Ok(Value::Number((data.len() as i64).into()))
            }
            Some(col) => {
                // COUNT(column) - count non-null values
                let count = data.iter()
                    .filter_map(|row| {
                        if let Value::Object(map) = row {
                            map.get(col)
                        } else {
                            None
                        }
                    })
                    .filter(|&value| value != &Value::Null)
                    .count();
                Ok(Value::Number((count as i64).into()))
            }
        }
    }

    /// Compute SUM(column)
    fn compute_sum(&self, data: &[Value], column: &str) -> Result<Value> {
        let mut sum = 0.0;
        let mut has_values = false;

        for row in data {
            if let Value::Object(map) = row {
                if let Some(value) = map.get(column) {
                    match value {
                        Value::Number(n) => {
                            if let Some(f) = n.as_f64() {
                                sum += f;
                                has_values = true;
                            }
                        }
                        Value::Null => {
                            // Skip null values
                            continue;
                        }
                        _ => {
                            return Err(anyhow!("Cannot compute SUM on non-numeric column: {}", column));
                        }
                    }
                }
            }
        }

        if !has_values {
            Ok(Value::Null)
        } else if sum.fract() == 0.0 && sum >= i64::MIN as f64 && sum <= i64::MAX as f64 {
            // Return as integer if it's a whole number within i64 range
            Ok(Value::Number((sum as i64).into()))
        } else {
            // Return as float
            Ok(Value::Number(serde_json::Number::from_f64(sum).unwrap_or_else(|| 0.into())))
        }
    }

    /// Compute AVG(column)
    fn compute_avg(&self, data: &[Value], column: &str) -> Result<Value> {
        let mut sum = 0.0;
        let mut count = 0;

        for row in data {
            if let Value::Object(map) = row {
                if let Some(value) = map.get(column) {
                    match value {
                        Value::Number(n) => {
                            if let Some(f) = n.as_f64() {
                                sum += f;
                                count += 1;
                            }
                        }
                        Value::Null => {
                            // Skip null values
                            continue;
                        }
                        _ => {
                            return Err(anyhow!("Cannot compute AVG on non-numeric column: {}", column));
                        }
                    }
                }
            }
        }

        if count == 0 {
            Ok(Value::Null)
        } else {
            let avg = sum / count as f64;
            Ok(Value::Number(serde_json::Number::from_f64(avg).unwrap_or_else(|| 0.into())))
        }
    }

    /// Compute MIN(column)
    fn compute_min(&self, data: &[Value], column: &str) -> Result<Value> {
        let mut min_value: Option<Value> = None;

        for row in data {
            if let Value::Object(map) = row {
                if let Some(value) = map.get(column) {
                    if value == &Value::Null {
                        continue; // Skip null values
                    }

                    match &min_value {
                        None => min_value = Some(value.clone()),
                        Some(current_min) => {
                            if self.compare_values(value, current_min) == std::cmp::Ordering::Less {
                                min_value = Some(value.clone());
                            }
                        }
                    }
                }
            }
        }

        Ok(min_value.unwrap_or(Value::Null))
    }

    /// Compute MAX(column)
    fn compute_max(&self, data: &[Value], column: &str) -> Result<Value> {
        let mut max_value: Option<Value> = None;

        for row in data {
            if let Value::Object(map) = row {
                if let Some(value) = map.get(column) {
                    if value == &Value::Null {
                        continue; // Skip null values
                    }

                    match &max_value {
                        None => max_value = Some(value.clone()),
                        Some(current_max) => {
                            if self.compare_values(value, current_max) == std::cmp::Ordering::Greater {
                                max_value = Some(value.clone());
                            }
                        }
                    }
                }
            }
        }

        Ok(max_value.unwrap_or(Value::Null))
    }

    /// Sort rows based on ORDER BY specification
    fn sort_rows(&self, rows: &mut Vec<Vec<Value>>, columns: &[String], order_by: &OrderBy) -> Result<()> {
        // Find the column index (case-insensitive for aggregation functions)
        let column_index = columns.iter().position(|col| {
            col == &order_by.column || col.to_lowercase() == order_by.column.to_lowercase()
        }).ok_or_else(|| anyhow!("ORDER BY column '{}' not found in result set", order_by.column))?;

        rows.sort_by(|a, b| {
            let val_a = a.get(column_index).unwrap_or(&Value::Null);
            let val_b = b.get(column_index).unwrap_or(&Value::Null);

            let comparison = self.compare_values(val_a, val_b);

            match order_by.direction {
                OrderDirection::Asc => comparison,
                OrderDirection::Desc => comparison.reverse(),
            }
        });

        Ok(())
    }

    /// Compare two JSON values for sorting
    fn compare_values(&self, a: &Value, b: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        match (a, b) {
            // Null values sort first
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,

            // Numbers
            (Value::Number(na), Value::Number(nb)) => {
                let fa = na.as_f64().unwrap_or(0.0);
                let fb = nb.as_f64().unwrap_or(0.0);
                fa.partial_cmp(&fb).unwrap_or(Ordering::Equal)
            },

            // Strings
            (Value::String(sa), Value::String(sb)) => sa.cmp(sb),

            // Booleans
            (Value::Bool(ba), Value::Bool(bb)) => ba.cmp(bb),

            // Mixed types: convert to strings and compare
            _ => {
                let sa = match a {
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => b.to_string(),
                    _ => serde_json::to_string(a).unwrap_or_default(),
                };
                let sb = match b {
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => b.to_string(),
                    _ => serde_json::to_string(b).unwrap_or_default(),
                };
                sa.cmp(&sb)
            },
        }
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

        // Parse SELECT clause to determine what kind of query this is
        let select_start = if lower.starts_with("select ") { 7 } else { 0 };
        let from_pos = lower.find(" from ").ok_or_else(|| anyhow!("Missing FROM clause"))?;
        let select_part = &sql[select_start..from_pos];

        let select_clause = self.parse_select_clause(select_part)?;

        // Extract table name
        let from_pos = lower.find(" from ").unwrap() + 6;
        let remaining = &sql[from_pos..];

        let table_end = remaining.find(" WHERE ")
            .or_else(|| remaining.find(" where "))
            .or_else(|| remaining.find(" GROUP BY "))
            .or_else(|| remaining.find(" group by "))
            .or_else(|| remaining.find(" ORDER BY "))
            .or_else(|| remaining.find(" order by "))
            .or_else(|| remaining.find(" LIMIT "))
            .or_else(|| remaining.find(" limit "))
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

            let where_end = where_clause.to_lowercase().find(" group by ")
                .or_else(|| where_clause.to_lowercase().find(" order by "))
                .or_else(|| where_clause.to_lowercase().find(" limit "))
                .or_else(|| where_clause.to_lowercase().find(" as of "))
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

        // Parse GROUP BY clause if present
        let group_by = if let Some(group_pos) = remaining.to_lowercase().find(" group by ") {
            let group_start = group_pos + 10; // " group by " is 10 characters
            let group_clause = &remaining[group_start..];

            let group_end = group_clause.to_lowercase().find(" having ")
                .or_else(|| group_clause.to_lowercase().find(" order by "))
                .or_else(|| group_clause.to_lowercase().find(" limit "))
                .or_else(|| group_clause.to_lowercase().find(" as of "))
                .unwrap_or(group_clause.len());

            let group_str = group_clause[..group_end].trim();
            debug!("Parsing GROUP BY clause: {}", group_str);
            Some(self.parse_group_by_clause(group_str)?)
        } else {
            None
        };

        // Parse HAVING clause if present
        let having = if let Some(having_pos) = remaining.to_lowercase().find(" having ") {
            let having_start = having_pos + 8; // " having " is 8 characters
            let having_clause = &remaining[having_start..];

            let having_end = having_clause.to_lowercase().find(" order by ")
                .or_else(|| having_clause.to_lowercase().find(" limit "))
                .or_else(|| having_clause.to_lowercase().find(" as of "))
                .unwrap_or(having_clause.len());

            let having_str = having_clause[..having_end].trim();
            debug!("Parsing HAVING clause: {}", having_str);
            Some(self.parse_having_clause(having_str)?)
        } else {
            None
        };

        // Parse ORDER BY clause if present
        let order_by = if let Some(order_pos) = remaining.to_lowercase().find(" order by ") {
            let order_start = order_pos + 10; // " order by " is 10 characters
            let order_clause = &remaining[order_start..];

            let order_end = order_clause.to_lowercase().find(" limit ")
                .or_else(|| order_clause.to_lowercase().find(" as of "))
                .unwrap_or(order_clause.len());

            let order_str = order_clause[..order_end].trim();
            debug!("Parsing ORDER BY clause: {}", order_str);
            Some(self.parse_order_by_clause(order_str)?)
        } else {
            None
        };

        // Parse LIMIT clause if present
        let limit = if let Some(limit_pos) = remaining.to_lowercase().find(" limit ") {
            let limit_start = limit_pos + 7; // " limit " is 7 characters
            let limit_clause = &remaining[limit_start..];

            let limit_end = limit_clause.to_lowercase().find(" as of ")
                .unwrap_or(limit_clause.len());

            let limit_str = limit_clause[..limit_end].trim();
            debug!("Parsing LIMIT clause: {}", limit_str);
            Some(self.parse_limit_clause(limit_str)?)
        } else {
            None
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

            // Note: ORDER BY and LIMIT will be applied later in the common code path
            time_travel_data
        } else {
            // Current data with WHERE filtering (existing logic)
            filtered_data
        };

        // Handle different types of queries based on SELECT clause and GROUP BY
        match (select_clause, group_by) {
            // Pure aggregation without GROUP BY - original behavior
            (SelectClause::Aggregations(aggregations), None) => {
                // Compute aggregation results
                let (agg_columns, agg_values) = self.compute_aggregations(&filtered_data, &aggregations)?;

                debug!("Returning aggregation result with columns: {:?}", agg_columns);
                Ok(QueryResult::Select {
                    columns: agg_columns,
                    rows: vec![agg_values],
                })
            },
            // Mixed columns and aggregations with GROUP BY
            (SelectClause::Mixed(columns, aggregations), Some(group_by_clause)) => {
                // Validate that all non-aggregation columns are in GROUP BY
                for col in &columns {
                    if !group_by_clause.columns.contains(col) {
                        return Err(anyhow!("Column '{}' must be in GROUP BY clause or be an aggregate function", col));
                    }
                }

                // Group the data
                let mut groups = self.group_data(filtered_data, &group_by_clause)?;
                debug!("Grouped data into {} groups", groups.len());

                // Apply HAVING clause if present
                if let Some(having_clause) = having {
                    groups = self.apply_having_filter(groups, &having_clause, &aggregations)?;
                    debug!("After HAVING filter: {} groups", groups.len());
                }

                // Compute aggregations for each group
                let (group_columns, mut group_rows) = self.compute_grouped_aggregations(&groups, &group_by_clause, &aggregations)?;

                // Apply ORDER BY if specified
                if let Some(ref order_by) = order_by {
                    debug!("Applying ORDER BY: {} {:?}", order_by.column, order_by.direction);
                    self.sort_rows(&mut group_rows, &group_columns, order_by)?;
                }

                // Apply LIMIT if specified
                if let Some(limit_count) = limit {
                    debug!("Applying LIMIT: {}", limit_count);
                    group_rows.truncate(limit_count);
                }

                debug!("Returning {} grouped rows with columns: {:?}", group_rows.len(), group_columns);
                Ok(QueryResult::Select {
                    columns: group_columns,
                    rows: group_rows,
                })
            },
            // Aggregations with GROUP BY
            (SelectClause::Aggregations(aggregations), Some(group_by_clause)) => {
                // Group the data
                let mut groups = self.group_data(filtered_data, &group_by_clause)?;
                debug!("Grouped data into {} groups", groups.len());

                // Apply HAVING clause if present
                if let Some(having_clause) = having {
                    groups = self.apply_having_filter(groups, &having_clause, &aggregations)?;
                    debug!("After HAVING filter: {} groups", groups.len());
                }

                // Compute aggregations for each group
                let (group_columns, mut group_rows) = self.compute_grouped_aggregations(&groups, &group_by_clause, &aggregations)?;

                // Apply ORDER BY if specified
                if let Some(ref order_by) = order_by {
                    debug!("Applying ORDER BY: {} {:?}", order_by.column, order_by.direction);
                    self.sort_rows(&mut group_rows, &group_columns, order_by)?;
                }

                // Apply LIMIT if specified
                if let Some(limit_count) = limit {
                    debug!("Applying LIMIT: {}", limit_count);
                    group_rows.truncate(limit_count);
                }

                debug!("Returning {} grouped rows with columns: {:?}", group_rows.len(), group_columns);
                Ok(QueryResult::Select {
                    columns: group_columns,
                    rows: group_rows,
                })
            },
            // Column selection without GROUP BY - new functionality
            (SelectClause::Columns(columns), None) => {
                // Filter and format results for specific column selection
                self.format_column_selection_results(filtered_data, &columns, order_by, limit)
            },
            // Regular SELECT * or error cases
            (SelectClause::All, None) => {
                // Continue with regular SELECT * logic
                self.format_select_all_results(filtered_data, order_by, limit)
            },
            // Error cases
            (SelectClause::All, Some(_)) => {
                Err(anyhow!("SELECT * with GROUP BY is not supported"))
            },
            (SelectClause::Columns(_, ), Some(_)) => {
                Err(anyhow!("Column selection with GROUP BY is not yet supported - use Mixed clause instead"))
            },
            (SelectClause::Mixed(_, _), None) => {
                Err(anyhow!("Column selection without aggregations requires GROUP BY clause"))
            },
        }
    }

    /// Filter columns from the result set and format for specific column selection
    fn format_column_selection_results(&self, filtered_data: Vec<Value>, selected_columns: &[String], order_by: Option<OrderBy>, limit: Option<usize>) -> Result<QueryResult> {
        if filtered_data.is_empty() {
            return Ok(QueryResult::Select {
                columns: selected_columns.to_vec(),
                rows: vec![],
            });
        }

        // Validate that all requested columns exist in at least one record
        let first = &filtered_data[0];
        if let Value::Object(map) = first {
            for col in selected_columns {
                if !map.contains_key(col) {
                    // Check if the column exists in any record (for safety)
                    let column_exists = filtered_data.iter().any(|record| {
                        if let Value::Object(obj) = record {
                            obj.contains_key(col)
                        } else {
                            false
                        }
                    });
                    if !column_exists {
                        return Err(anyhow!("Column '{}' does not exist in the table", col));
                    }
                }
            }
        }

        // Extract only the requested columns from each row
        let mut rows: Vec<Vec<Value>> = filtered_data.into_iter().map(|record| {
            if let Value::Object(map) = record {
                selected_columns.iter().map(|col| {
                    map.get(col).cloned().unwrap_or(Value::Null)
                }).collect()
            } else {
                // For non-object records, return null for all columns
                vec![Value::Null; selected_columns.len()]
            }
        }).collect();

        // Apply ORDER BY if specified
        if let Some(ref order_by) = order_by {
            debug!("Applying ORDER BY: {} {:?}", order_by.column, order_by.direction);
            self.sort_rows(&mut rows, selected_columns, order_by)?;
        }

        // Apply LIMIT if specified
        if let Some(limit_count) = limit {
            debug!("Applying LIMIT: {}", limit_count);
            rows.truncate(limit_count);
        }

        debug!("Returning {} rows with {} selected columns: {:?}", rows.len(), selected_columns.len(), selected_columns);
        Ok(QueryResult::Select {
            columns: selected_columns.to_vec(),
            rows,
        })
    }

    /// Format results for SELECT * queries
    fn format_select_all_results(&self, filtered_data: Vec<Value>, order_by: Option<OrderBy>, limit: Option<usize>) -> Result<QueryResult> {

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

            let mut rows: Vec<Vec<Value>> = filtered_data.into_iter().map(|record| {
                if let Value::Object(map) = record {
                    columns.iter().map(|col| {
                        map.get(col).cloned().unwrap_or(Value::Null)
                    }).collect()
                } else {
                    vec![record]
                }
            }).collect();

            // Apply ORDER BY if specified
            if let Some(ref order_by) = order_by {
                debug!("Applying ORDER BY: {} {:?}", order_by.column, order_by.direction);
                self.sort_rows(&mut rows, &columns, order_by)?;
            }

            // Apply LIMIT if specified
            if let Some(limit_count) = limit {
                debug!("Applying LIMIT: {}", limit_count);
                rows.truncate(limit_count);
            }

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
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
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
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
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

    #[tokio::test]
    async fn test_parse_order_by_clause() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine);

        // Test ascending order (default)
        let order_by = executor.parse_order_by_clause("name").unwrap();
        assert_eq!(order_by.column, "name");
        assert!(matches!(order_by.direction, OrderDirection::Asc));

        // Test explicit ascending
        let order_by = executor.parse_order_by_clause("name ASC").unwrap();
        assert_eq!(order_by.column, "name");
        assert!(matches!(order_by.direction, OrderDirection::Asc));

        // Test descending
        let order_by = executor.parse_order_by_clause("age DESC").unwrap();
        assert_eq!(order_by.column, "age");
        assert!(matches!(order_by.direction, OrderDirection::Desc));
    }

    #[tokio::test]
    async fn test_parse_limit_clause() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine);

        let limit = executor.parse_limit_clause("10").unwrap();
        assert_eq!(limit, 10);

        let limit = executor.parse_limit_clause("100").unwrap();
        assert_eq!(limit, 100);

        // Test invalid limit
        assert!(executor.parse_limit_clause("invalid").is_err());
    }

    #[tokio::test]
    async fn test_compare_values() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine);

        use std::cmp::Ordering;

        // Test number comparison
        let a = Value::Number(10.into());
        let b = Value::Number(20.into());
        assert_eq!(executor.compare_values(&a, &b), Ordering::Less);
        assert_eq!(executor.compare_values(&b, &a), Ordering::Greater);
        assert_eq!(executor.compare_values(&a, &a), Ordering::Equal);

        // Test string comparison
        let a = Value::String("apple".to_string());
        let b = Value::String("banana".to_string());
        assert_eq!(executor.compare_values(&a, &b), Ordering::Less);
        assert_eq!(executor.compare_values(&b, &a), Ordering::Greater);

        // Test null handling
        let null = Value::Null;
        let num = Value::Number(5.into());
        assert_eq!(executor.compare_values(&null, &num), Ordering::Less);
        assert_eq!(executor.compare_values(&num, &null), Ordering::Greater);
        assert_eq!(executor.compare_values(&null, &null), Ordering::Equal);
    }

    #[tokio::test]
    async fn test_order_by_and_limit_integration() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor.execute("CREATE TABLE users (pk = id)").await.unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data
        let users = vec![
            r#"INSERT INTO users (id, name, age, email) VALUES (1, 'Alice', 30, 'alice@example.com')"#,
            r#"INSERT INTO users (id, name, age, email) VALUES (2, 'Bob', 25, 'bob@example.com')"#,
            r#"INSERT INTO users (id, name, age, email) VALUES (3, 'Charlie', 35, 'charlie@example.com')"#,
            r#"INSERT INTO users (id, name, age, email) VALUES (4, 'David', 28, 'david@example.com')"#,
        ];

        for sql in users {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test ORDER BY age ASC
        let result = executor.execute("SELECT * FROM users ORDER BY age ASC").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert!(columns.contains(&"age".to_string()));
            assert_eq!(rows.len(), 4);

            let age_index = columns.iter().position(|c| c == "age").unwrap();
            let ages: Vec<i64> = rows.iter()
                .map(|row| row[age_index].as_i64().unwrap())
                .collect();
            assert_eq!(ages, vec![25, 28, 30, 35]); // Should be sorted ascending
        } else {
            panic!("Expected SELECT result");
        }

        // Test ORDER BY age DESC with LIMIT
        let result = executor.execute("SELECT * FROM users ORDER BY age DESC LIMIT 2").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(rows.len(), 2); // Should be limited to 2 rows

            let age_index = columns.iter().position(|c| c == "age").unwrap();
            let ages: Vec<i64> = rows.iter()
                .map(|row| row[age_index].as_i64().unwrap())
                .collect();
            assert_eq!(ages, vec![35, 30]); // Should be sorted descending and limited
        } else {
            panic!("Expected SELECT result");
        }

        // Test ORDER BY name ASC (string sorting)
        let result = executor.execute("SELECT * FROM users ORDER BY name ASC").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            let name_index = columns.iter().position(|c| c == "name").unwrap();
            let names: Vec<String> = rows.iter()
                .map(|row| row[name_index].as_str().unwrap().to_string())
                .collect();
            assert_eq!(names, vec!["Alice", "Bob", "Charlie", "David"]); // Should be sorted alphabetically
        } else {
            panic!("Expected SELECT result");
        }

        // Test WHERE + ORDER BY + LIMIT
        let result = executor.execute("SELECT * FROM users WHERE age >= 28 ORDER BY age ASC LIMIT 2").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(rows.len(), 2); // Should be limited to 2 rows

            let age_index = columns.iter().position(|c| c == "age").unwrap();
            let ages: Vec<i64> = rows.iter()
                .map(|row| row[age_index].as_i64().unwrap())
                .collect();
            assert_eq!(ages, vec![28, 30]); // Should have ages >= 28, sorted ascending, limited to 2
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_aggregation_count_star() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor.execute("CREATE TABLE users (pk = id)").await.unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data
        let users = vec![
            r#"INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)"#,
            r#"INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)"#,
            r#"INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)"#,
        ];

        for sql in users {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test COUNT(*)
        let result = executor.execute("SELECT COUNT(*) FROM users").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0], "count(*)");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Number(3.into()));
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_aggregation_count_column() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor.execute("CREATE TABLE users (pk = id)").await.unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data with some null ages
        let users = vec![
            r#"INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)"#,
            r#"INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)"#,
            r#"INSERT INTO users (id, name) VALUES (3, 'Charlie')"#, // null age
        ];

        for sql in users {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test COUNT(age) - should exclude null values
        let result = executor.execute("SELECT COUNT(age) FROM users").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0], "count(age)");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Number(2.into())); // Only 2 non-null ages
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_aggregation_sum() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor.execute("CREATE TABLE employees (pk = id)").await.unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data
        let employees = vec![
            r#"INSERT INTO employees (id, name, salary) VALUES (1, 'Alice', 50000)"#,
            r#"INSERT INTO employees (id, name, salary) VALUES (2, 'Bob', 60000)"#,
            r#"INSERT INTO employees (id, name, salary) VALUES (3, 'Charlie', 70000)"#,
        ];

        for sql in employees {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test SUM(salary)
        let result = executor.execute("SELECT SUM(salary) FROM employees").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0], "sum(salary)");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Number(180000.into())); // 50000 + 60000 + 70000
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_aggregation_avg() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor.execute("CREATE TABLE test_scores (pk = id)").await.unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data
        let scores = vec![
            r#"INSERT INTO test_scores (id, student, score) VALUES (1, 'Alice', 85)"#,
            r#"INSERT INTO test_scores (id, student, score) VALUES (2, 'Bob', 92)"#,
            r#"INSERT INTO test_scores (id, student, score) VALUES (3, 'Charlie', 78)"#,
        ];

        for sql in scores {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test AVG(score)
        let result = executor.execute("SELECT AVG(score) FROM test_scores").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0], "avg(score)");
            assert_eq!(rows.len(), 1);
            // Average of 85, 92, 78 = 255/3 = 85.0
            if let Value::Number(n) = &rows[0][0] {
                let avg = n.as_f64().unwrap();
                assert!((avg - 85.0).abs() < 0.0001);
            } else {
                panic!("Expected numeric result for AVG");
            }
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_aggregation_min_max() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor.execute("CREATE TABLE users (pk = id)").await.unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data
        let users = vec![
            r#"INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)"#,
            r#"INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)"#,
            r#"INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)"#,
            r#"INSERT INTO users (id, name, age) VALUES (4, 'David', 28)"#,
        ];

        for sql in users {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test MIN(age)
        let result = executor.execute("SELECT MIN(age) FROM users").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0], "min(age)");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Number(25.into()));
        } else {
            panic!("Expected SELECT result");
        }

        // Test MAX(age)
        let result = executor.execute("SELECT MAX(age) FROM users").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0], "max(age)");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Number(35.into()));
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_aggregation_with_where_clause() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor.execute("CREATE TABLE employees (pk = id)").await.unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data
        let employees = vec![
            r#"INSERT INTO employees (id, name, salary, department) VALUES (1, 'Alice', 50000, 'Engineering')"#,
            r#"INSERT INTO employees (id, name, salary, department) VALUES (2, 'Bob', 60000, 'Engineering')"#,
            r#"INSERT INTO employees (id, name, salary, department) VALUES (3, 'Charlie', 45000, 'Sales')"#,
            r#"INSERT INTO employees (id, name, salary, department) VALUES (4, 'David', 55000, 'Engineering')"#,
        ];

        for sql in employees {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test COUNT(*) with WHERE clause
        let result = executor.execute("SELECT COUNT(*) FROM employees WHERE department = 'Engineering'").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0], "count(*)");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Number(3.into()));
        } else {
            panic!("Expected SELECT result");
        }

        // Test AVG(salary) with WHERE clause
        let result = executor.execute("SELECT AVG(salary) FROM employees WHERE department = 'Engineering'").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0], "avg(salary)");
            assert_eq!(rows.len(), 1);
            // Average of 50000, 60000, 55000 = 165000/3 = 55000.0
            if let Value::Number(n) = &rows[0][0] {
                let avg = n.as_f64().unwrap();
                assert!((avg - 55000.0).abs() < 0.0001);
            } else {
                panic!("Expected numeric result for AVG");
            }
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_multiple_aggregations() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor.execute("CREATE TABLE users (pk = id)").await.unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data
        let users = vec![
            r#"INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)"#,
            r#"INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)"#,
            r#"INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)"#,
        ];

        for sql in users {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test multiple aggregations in one query
        let result = executor.execute("SELECT COUNT(*), MIN(age), MAX(age), AVG(age) FROM users").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 4);
            assert_eq!(columns[0], "count(*)");
            assert_eq!(columns[1], "min(age)");
            assert_eq!(columns[2], "max(age)");
            assert_eq!(columns[3], "avg(age)");
            assert_eq!(rows.len(), 1);

            assert_eq!(rows[0][0], Value::Number(3.into())); // COUNT(*)
            assert_eq!(rows[0][1], Value::Number(25.into())); // MIN(age)
            assert_eq!(rows[0][2], Value::Number(35.into())); // MAX(age)

            // AVG(age) = (30 + 25 + 35) / 3 = 30.0
            if let Value::Number(n) = &rows[0][3] {
                let avg = n.as_f64().unwrap();
                assert!((avg - 30.0).abs() < 0.0001);
            } else {
                panic!("Expected numeric result for AVG");
            }
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_aggregation_empty_table() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table but don't insert any data
        let result = executor.execute("CREATE TABLE empty_table (pk = id)").await.unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Test COUNT(*) on empty table
        let result = executor.execute("SELECT COUNT(*) FROM empty_table").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0], "count(*)");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Number(0.into()));
        } else {
            panic!("Expected SELECT result");
        }

        // Test SUM on empty table - should return NULL
        let result = executor.execute("SELECT SUM(value) FROM empty_table").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0], "sum(value)");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Null);
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_aggregation_string_columns() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor.execute("CREATE TABLE users (pk = id)").await.unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data
        let users = vec![
            r#"INSERT INTO users (id, name) VALUES (1, 'Alice')"#,
            r#"INSERT INTO users (id, name) VALUES (2, 'Bob')"#,
            r#"INSERT INTO users (id, name) VALUES (3, 'Charlie')"#,
        ];

        for sql in users {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test MIN/MAX on string columns
        let result = executor.execute("SELECT MIN(name), MAX(name) FROM users").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0], "min(name)");
            assert_eq!(columns[1], "max(name)");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::String("Alice".to_string())); // MIN alphabetically
            assert_eq!(rows[0][1], Value::String("Charlie".to_string())); // MAX alphabetically
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_group_by_basic() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor.execute("CREATE TABLE employees (pk = id)").await.unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data
        let employees = vec![
            r#"INSERT INTO employees (id, name, department, salary) VALUES (1, 'Alice', 'Engineering', 70000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (2, 'Bob', 'Engineering', 75000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (3, 'Charlie', 'Sales', 60000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (4, 'David', 'Engineering', 80000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (5, 'Eve', 'Sales', 65000)"#,
        ];

        for sql in employees {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test basic GROUP BY with COUNT
        let result = executor.execute("SELECT department, COUNT(*) FROM employees GROUP BY department").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0], "department");
            assert_eq!(columns[1], "count(*)");
            assert_eq!(rows.len(), 2);

            // Sort rows for consistent testing
            let mut sorted_rows = rows.clone();
            sorted_rows.sort_by(|a, b| a[0].as_str().unwrap().cmp(b[0].as_str().unwrap()));

            assert_eq!(sorted_rows[0][0], Value::String("Engineering".to_string()));
            assert_eq!(sorted_rows[0][1], Value::Number(3.into()));
            assert_eq!(sorted_rows[1][0], Value::String("Sales".to_string()));
            assert_eq!(sorted_rows[1][1], Value::Number(2.into()));
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_group_by_with_avg() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor.execute("CREATE TABLE employees (pk = id)").await.unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data
        let employees = vec![
            r#"INSERT INTO employees (id, name, department, salary) VALUES (1, 'Alice', 'Engineering', 70000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (2, 'Bob', 'Engineering', 80000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (3, 'Charlie', 'Sales', 60000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (4, 'David', 'Sales', 70000)"#,
        ];

        for sql in employees {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test GROUP BY with AVG
        let result = executor.execute("SELECT department, AVG(salary) FROM employees GROUP BY department").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0], "department");
            assert_eq!(columns[1], "avg(salary)");
            assert_eq!(rows.len(), 2);

            // Sort rows for consistent testing
            let mut sorted_rows = rows.clone();
            sorted_rows.sort_by(|a, b| a[0].as_str().unwrap().cmp(b[0].as_str().unwrap()));

            assert_eq!(sorted_rows[0][0], Value::String("Engineering".to_string()));
            // Average of 70000, 80000 = 75000
            if let Value::Number(n) = &sorted_rows[0][1] {
                let avg = n.as_f64().unwrap();
                assert!((avg - 75000.0).abs() < 0.0001);
            } else {
                panic!("Expected numeric result for AVG");
            }

            assert_eq!(sorted_rows[1][0], Value::String("Sales".to_string()));
            // Average of 60000, 70000 = 65000
            if let Value::Number(n) = &sorted_rows[1][1] {
                let avg = n.as_f64().unwrap();
                assert!((avg - 65000.0).abs() < 0.0001);
            } else {
                panic!("Expected numeric result for AVG");
            }
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_group_by_multiple_columns() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor.execute("CREATE TABLE employees (pk = id)").await.unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data with department and level
        let employees = vec![
            r#"INSERT INTO employees (id, name, department, level, salary) VALUES (1, 'Alice', 'Engineering', 'Senior', 80000)"#,
            r#"INSERT INTO employees (id, name, department, level, salary) VALUES (2, 'Bob', 'Engineering', 'Junior', 60000)"#,
            r#"INSERT INTO employees (id, name, department, level, salary) VALUES (3, 'Charlie', 'Engineering', 'Senior', 85000)"#,
            r#"INSERT INTO employees (id, name, department, level, salary) VALUES (4, 'David', 'Sales', 'Senior', 70000)"#,
            r#"INSERT INTO employees (id, name, department, level, salary) VALUES (5, 'Eve', 'Sales', 'Junior', 50000)"#,
        ];

        for sql in employees {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test GROUP BY with multiple columns
        let result = executor.execute("SELECT department, level, COUNT(*) FROM employees GROUP BY department, level").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 3);
            assert_eq!(columns[0], "department");
            assert_eq!(columns[1], "level");
            assert_eq!(columns[2], "count(*)");
            assert_eq!(rows.len(), 4); // 4 distinct department-level combinations
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_group_by_with_where() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor.execute("CREATE TABLE employees (pk = id)").await.unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data
        let employees = vec![
            r#"INSERT INTO employees (id, name, department, age, salary) VALUES (1, 'Alice', 'Engineering', 30, 70000)"#,
            r#"INSERT INTO employees (id, name, department, age, salary) VALUES (2, 'Bob', 'Engineering', 25, 60000)"#,
            r#"INSERT INTO employees (id, name, department, age, salary) VALUES (3, 'Charlie', 'Sales', 35, 65000)"#,
            r#"INSERT INTO employees (id, name, department, age, salary) VALUES (4, 'David', 'Engineering', 28, 75000)"#,
            r#"INSERT INTO employees (id, name, department, age, salary) VALUES (5, 'Eve', 'Sales', 22, 50000)"#,
        ];

        for sql in employees {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test GROUP BY with WHERE clause
        let result = executor.execute("SELECT department, COUNT(*) FROM employees WHERE age > 25 GROUP BY department").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0], "department");
            assert_eq!(columns[1], "count(*)");
            assert_eq!(rows.len(), 2);

            // Sort rows for consistent testing
            let mut sorted_rows = rows.clone();
            sorted_rows.sort_by(|a, b| a[0].as_str().unwrap().cmp(b[0].as_str().unwrap()));

            assert_eq!(sorted_rows[0][0], Value::String("Engineering".to_string()));
            assert_eq!(sorted_rows[0][1], Value::Number(2.into())); // Alice(30) and David(28)
            assert_eq!(sorted_rows[1][0], Value::String("Sales".to_string()));
            assert_eq!(sorted_rows[1][1], Value::Number(1.into())); // Charlie(35)
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_group_by_with_having() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor.execute("CREATE TABLE employees (pk = id)").await.unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data
        let employees = vec![
            r#"INSERT INTO employees (id, name, department, salary) VALUES (1, 'Alice', 'Engineering', 70000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (2, 'Bob', 'Engineering', 80000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (3, 'Charlie', 'Engineering', 75000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (4, 'David', 'Sales', 50000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (5, 'Eve', 'Sales', 55000)"#,
        ];

        for sql in employees {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test GROUP BY with HAVING clause
        let result = executor.execute("SELECT department, AVG(salary) FROM employees GROUP BY department HAVING AVG(salary) > 60000").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0], "department");
            assert_eq!(columns[1], "avg(salary)");
            assert_eq!(rows.len(), 1); // Only Engineering should match (avg = 75000)

            assert_eq!(rows[0][0], Value::String("Engineering".to_string()));
            if let Value::Number(n) = &rows[0][1] {
                let avg = n.as_f64().unwrap();
                assert!((avg - 75000.0).abs() < 0.0001);
            } else {
                panic!("Expected numeric result for AVG");
            }
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_group_by_with_order_by_and_limit() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor.execute("CREATE TABLE employees (pk = id)").await.unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data
        let employees = vec![
            r#"INSERT INTO employees (id, name, department, salary) VALUES (1, 'Alice', 'Engineering', 70000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (2, 'Bob', 'Engineering', 80000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (3, 'Charlie', 'Sales', 60000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (4, 'David', 'Marketing', 65000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (5, 'Eve', 'Sales', 55000)"#,
        ];

        for sql in employees {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test GROUP BY with ORDER BY and LIMIT
        let result = executor.execute("SELECT department, AVG(salary) FROM employees GROUP BY department ORDER BY AVG(salary) DESC LIMIT 2").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0], "department");
            assert_eq!(columns[1], "avg(salary)");
            assert_eq!(rows.len(), 2); // Limited to 2 results

            // Should be ordered by avg salary descending: Engineering (75000), Marketing (65000)
            assert_eq!(rows[0][0], Value::String("Engineering".to_string()));
            if let Value::Number(n) = &rows[0][1] {
                let avg = n.as_f64().unwrap();
                assert!((avg - 75000.0).abs() < 0.0001);
            } else {
                panic!("Expected numeric result for AVG");
            }

            assert_eq!(rows[1][0], Value::String("Marketing".to_string()));
            if let Value::Number(n) = &rows[1][1] {
                let avg = n.as_f64().unwrap();
                assert!((avg - 65000.0).abs() < 0.0001);
            } else {
                panic!("Expected numeric result for AVG");
            }
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_group_by_multiple_aggregations() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor.execute("CREATE TABLE employees (pk = id)").await.unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data
        let employees = vec![
            r#"INSERT INTO employees (id, name, department, salary) VALUES (1, 'Alice', 'Engineering', 70000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (2, 'Bob', 'Engineering', 80000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (3, 'Charlie', 'Engineering', 60000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (4, 'David', 'Sales', 50000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (5, 'Eve', 'Sales', 70000)"#,
        ];

        for sql in employees {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test GROUP BY with multiple aggregations
        let result = executor.execute("SELECT department, COUNT(*), MIN(salary), MAX(salary), AVG(salary) FROM employees GROUP BY department").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 5);
            assert_eq!(columns[0], "department");
            assert_eq!(columns[1], "count(*)");
            assert_eq!(columns[2], "min(salary)");
            assert_eq!(columns[3], "max(salary)");
            assert_eq!(columns[4], "avg(salary)");
            assert_eq!(rows.len(), 2);

            // Sort rows for consistent testing
            let mut sorted_rows = rows.clone();
            sorted_rows.sort_by(|a, b| a[0].as_str().unwrap().cmp(b[0].as_str().unwrap()));

            // Check Engineering department
            assert_eq!(sorted_rows[0][0], Value::String("Engineering".to_string()));
            assert_eq!(sorted_rows[0][1], Value::Number(3.into())); // COUNT
            assert_eq!(sorted_rows[0][2], Value::Number(60000.into())); // MIN
            assert_eq!(sorted_rows[0][3], Value::Number(80000.into())); // MAX
            // AVG = (70000 + 80000 + 60000) / 3 = 70000
            if let Value::Number(n) = &sorted_rows[0][4] {
                let avg = n.as_f64().unwrap();
                assert!((avg - 70000.0).abs() < 0.0001);
            } else {
                panic!("Expected numeric result for AVG");
            }

            // Check Sales department
            assert_eq!(sorted_rows[1][0], Value::String("Sales".to_string()));
            assert_eq!(sorted_rows[1][1], Value::Number(2.into())); // COUNT
            assert_eq!(sorted_rows[1][2], Value::Number(50000.into())); // MIN
            assert_eq!(sorted_rows[1][3], Value::Number(70000.into())); // MAX
            // AVG = (50000 + 70000) / 2 = 60000
            if let Value::Number(n) = &sorted_rows[1][4] {
                let avg = n.as_f64().unwrap();
                assert!((avg - 60000.0).abs() < 0.0001);
            } else {
                panic!("Expected numeric result for AVG");
            }
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_column_selection() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor.execute("CREATE TABLE employees (pk = id)").await.unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert some test data
        let employees = vec![
            r#"INSERT INTO employees (id, name, department, salary, age) VALUES (1, 'Alice', 'Engineering', 75000, 30)"#,
            r#"INSERT INTO employees (id, name, department, salary, age) VALUES (2, 'Bob', 'Sales', 65000, 28)"#,
            r#"INSERT INTO employees (id, name, department, salary, age) VALUES (3, 'Charlie', 'Engineering', 85000, 35)"#,
        ];

        for insert_sql in employees {
            let result = executor.execute(insert_sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test single column selection
        let result = executor.execute("SELECT name FROM employees").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns, vec!["name"]);
            assert_eq!(rows.len(), 3);
            // Check that all expected names are present (order may vary)
            let names: Vec<String> = rows.iter().map(|row| {
                if let serde_json::Value::String(name) = &row[0] {
                    name.clone()
                } else {
                    panic!("Expected string name");
                }
            }).collect();
            assert!(names.contains(&"Alice".to_string()));
            assert!(names.contains(&"Bob".to_string()));
            assert!(names.contains(&"Charlie".to_string()));
        } else {
            panic!("Expected Select result");
        }

        // Test multiple column selection
        let result = executor.execute("SELECT name, department FROM employees").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns, vec!["name", "department"]);
            assert_eq!(rows.len(), 3);
            // Check that all expected combinations are present
            let results: Vec<(String, String)> = rows.iter().map(|row| {
                if let (serde_json::Value::String(name), serde_json::Value::String(dept)) = (&row[0], &row[1]) {
                    (name.clone(), dept.clone())
                } else {
                    panic!("Expected string values");
                }
            }).collect();
            assert!(results.contains(&("Alice".to_string(), "Engineering".to_string())));
            assert!(results.contains(&("Bob".to_string(), "Sales".to_string())));
            assert!(results.contains(&("Charlie".to_string(), "Engineering".to_string())));
        } else {
            panic!("Expected Select result");
        }

        // Test column selection with WHERE clause
        let result = executor.execute("SELECT name, salary FROM employees WHERE department = 'Engineering'").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns, vec!["name", "salary"]);
            assert_eq!(rows.len(), 2);
            // Check that both engineering employees are present
            let results: Vec<(String, i64)> = rows.iter().map(|row| {
                if let (serde_json::Value::String(name), serde_json::Value::Number(salary)) = (&row[0], &row[1]) {
                    (name.clone(), salary.as_i64().unwrap())
                } else {
                    panic!("Expected string and number values");
                }
            }).collect();
            assert!(results.contains(&("Alice".to_string(), 75000)));
            assert!(results.contains(&("Charlie".to_string(), 85000)));
        } else {
            panic!("Expected Select result");
        }

        // Test column selection with ORDER BY
        let result = executor.execute("SELECT name, salary FROM employees ORDER BY salary DESC").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns, vec!["name", "salary"]);
            assert_eq!(rows.len(), 3);
            // First row should be Charlie with highest salary (85000)
            if let (serde_json::Value::String(name), serde_json::Value::Number(salary)) = (&rows[0][0], &rows[0][1]) {
                assert_eq!(name, "Charlie");
                assert_eq!(salary.as_i64().unwrap(), 85000);
            } else {
                panic!("Expected string and number values");
            }
        } else {
            panic!("Expected Select result");
        }

        // Test column selection with LIMIT
        let result = executor.execute("SELECT name FROM employees LIMIT 2").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns, vec!["name"]);
            assert_eq!(rows.len(), 2);
        } else {
            panic!("Expected Select result");
        }

        // Test error case: non-existent column
        let result = executor.execute("SELECT nonexistent_column FROM employees").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not exist"));
    }

    #[tokio::test]
    async fn test_group_by_error_cases() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor.execute("CREATE TABLE employees (pk = id)").await.unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Test error: column not in GROUP BY
        let result = executor.execute("SELECT name, department, COUNT(*) FROM employees GROUP BY department").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must be in GROUP BY clause"));

        // Test error: SELECT * with GROUP BY
        let result = executor.execute("SELECT * FROM employees GROUP BY department").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("SELECT * with GROUP BY is not supported"));

        // Test success: columns without GROUP BY should now work
        let result = executor.execute("SELECT name, department FROM employees").await;
        assert!(result.is_ok());
        if let Ok(QueryResult::Select { columns, rows: _ }) = result {
            assert_eq!(columns, vec!["name", "department"]);
        }
    }
}