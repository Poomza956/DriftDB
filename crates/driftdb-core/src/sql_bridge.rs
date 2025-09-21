//! Bridge between SQL and DriftQL for CLI integration

use sqlparser::ast::{Statement, Query as SqlQuery, Select, SetExpr, TableFactor, TableWithJoins, JoinOperator, BinaryOperator};
use sqlparser::parser::Parser;
use sqlparser::dialect::GenericDialect;
use serde_json::{json, Value};

use crate::query::{Query, QueryResult, WhereCondition};
use crate::engine::Engine;
use crate::errors::{DriftError, Result};

/// Convert SQL to DriftQL and execute
pub fn execute_sql(engine: &mut Engine, sql: &str) -> Result<QueryResult> {
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql)
        .map_err(|e| DriftError::Parse(e.to_string()))?;

    if ast.is_empty() {
        return Err(DriftError::InvalidQuery("Empty SQL statement".to_string()));
    }

    match &ast[0] {
        Statement::Query(query) => execute_sql_query(engine, query),
        Statement::Insert { table_name, columns, source, .. } => {
            if let Some(src) = source {
                execute_sql_insert(engine, table_name, columns, src)
            } else {
                Err(DriftError::InvalidQuery("INSERT requires VALUES or SELECT".to_string()))
            }
        }
        Statement::CreateTable { name, columns, .. } => {
            let table_name = name.to_string();

            // Extract primary key from columns
            let mut primary_key = "id".to_string(); // default
            let mut indexed_columns = Vec::new();

            for column in columns {
                // Check for PRIMARY KEY constraint
                for option in &column.options {
                    if let sqlparser::ast::ColumnOption::Unique { is_primary, .. } = option.option {
                        if is_primary {
                            primary_key = column.name.value.clone();
                        }
                    }
                }
                // We could also check for INDEX constraints here in the future
            }

            engine.create_table(&table_name, &primary_key, indexed_columns)?;

            Ok(QueryResult::Success {
                message: format!("Table '{}' created", table_name),
            })
        }
        _ => Err(DriftError::InvalidQuery(
            "SQL statement type not yet supported".to_string()
        )),
    }
}

fn execute_sql_query(engine: &mut Engine, query: &Box<SqlQuery>) -> Result<QueryResult> {
    match query.body.as_ref() {
        SetExpr::Select(select) => {
            if select.from.is_empty() {
                return Err(DriftError::InvalidQuery("No FROM clause".to_string()));
            }

            // Check for JOINs
            if select.from[0].joins.is_empty() {
                // Simple query without JOINs
                execute_simple_select(engine, select)
            } else {
                // Query with JOINs
                execute_join_select(engine, select)
            }
        }
        _ => Err(DriftError::InvalidQuery("Only SELECT queries supported".to_string())),
    }
}

fn execute_simple_select(engine: &mut Engine, select: &Select) -> Result<QueryResult> {
    let table_name = extract_table_name(&select.from[0].relation)?;

    // Convert WHERE clause to WhereConditions
    let conditions = if let Some(selection) = &select.selection {
        parse_where_clause(selection)?
    } else {
        vec![]
    };

    // Execute DriftQL query
    let driftql_query = Query::Select {
        table: table_name,
        conditions,
        as_of: None,
        limit: None,
    };

    engine.execute_query(driftql_query)
}

fn execute_join_select(engine: &mut Engine, select: &Select) -> Result<QueryResult> {
    // Get left table data
    let left_table = extract_table_name(&select.from[0].relation)?;
    let left_query = Query::Select {
        table: left_table.clone(),
        conditions: vec![],
        as_of: None,
        limit: None,
    };

    let left_result = engine.execute_query(left_query)?;
    let left_rows = match left_result {
        QueryResult::Rows { data } => data,
        _ => return Ok(left_result),
    };

    // Process first JOIN
    let join = &select.from[0].joins[0];
    let right_table = extract_table_name_from_join(&join.relation)?;

    let right_query = Query::Select {
        table: right_table.clone(),
        conditions: vec![],
        as_of: None,
        limit: None,
    };

    let right_result = engine.execute_query(right_query)?;
    let right_rows = match right_result {
        QueryResult::Rows { data } => data,
        _ => vec![],
    };

    // Perform the JOIN
    let joined_rows = match &join.join_operator {
        JoinOperator::Inner(constraint) => {
            perform_inner_join(left_rows, right_rows, constraint)?
        }
        JoinOperator::LeftOuter(constraint) => {
            perform_left_join(left_rows, right_rows, constraint)?
        }
        JoinOperator::CrossJoin => {
            perform_cross_join(left_rows, right_rows)
        }
        _ => {
            return Err(DriftError::InvalidQuery(
                "JOIN type not yet supported".to_string()
            ));
        }
    };

    Ok(QueryResult::Rows { data: joined_rows })
}

fn perform_inner_join(
    left_rows: Vec<Value>,
    right_rows: Vec<Value>,
    constraint: &sqlparser::ast::JoinConstraint,
) -> Result<Vec<Value>> {
    let (left_col, right_col) = extract_join_columns(constraint)?;
    let mut result = Vec::new();

    for left_row in &left_rows {
        for right_row in &right_rows {
            if let (Some(left_val), Some(right_val)) =
                (left_row.get(&left_col), right_row.get(&right_col)) {
                if left_val == right_val {
                    // Merge rows
                    let mut merged = left_row.as_object()
                        .cloned()
                        .unwrap_or_default();

                    if let Some(right_obj) = right_row.as_object() {
                        for (key, value) in right_obj {
                            // Prefix right table columns to avoid conflicts
                            merged.insert(format!("right_{}", key), value.clone());
                        }
                    }

                    result.push(json!(merged));
                }
            }
        }
    }

    Ok(result)
}

fn perform_left_join(
    left_rows: Vec<Value>,
    right_rows: Vec<Value>,
    constraint: &sqlparser::ast::JoinConstraint,
) -> Result<Vec<Value>> {
    let (left_col, right_col) = extract_join_columns(constraint)?;
    let mut result = Vec::new();

    for left_row in &left_rows {
        let mut matched = false;

        for right_row in &right_rows {
            if let (Some(left_val), Some(right_val)) =
                (left_row.get(&left_col), right_row.get(&right_col)) {
                if left_val == right_val {
                    // Merge rows
                    let mut merged = left_row.as_object()
                        .cloned()
                        .unwrap_or_default();

                    if let Some(right_obj) = right_row.as_object() {
                        for (key, value) in right_obj {
                            merged.insert(format!("right_{}", key), value.clone());
                        }
                    }

                    result.push(json!(merged));
                    matched = true;
                }
            }
        }

        // If no match found, still include left row (with NULLs for right)
        if !matched {
            result.push(left_row.clone());
        }
    }

    Ok(result)
}

fn perform_cross_join(left_rows: Vec<Value>, right_rows: Vec<Value>) -> Vec<Value> {
    let mut result = Vec::new();

    for left_row in &left_rows {
        for right_row in &right_rows {
            let mut merged = left_row.as_object()
                .cloned()
                .unwrap_or_default();

            if let Some(right_obj) = right_row.as_object() {
                for (key, value) in right_obj {
                    merged.insert(format!("right_{}", key), value.clone());
                }
            }

            result.push(json!(merged));
        }
    }

    result
}

fn extract_join_columns(
    constraint: &sqlparser::ast::JoinConstraint,
) -> Result<(String, String)> {
    match constraint {
        sqlparser::ast::JoinConstraint::On(expr) => {
            // Parse ON condition (simplified - assumes single equality)
            if let sqlparser::ast::Expr::BinaryOp { left, op, right } = expr {
                if matches!(op, BinaryOperator::Eq) {
                    let left_col = extract_column_from_expr(left)?;
                    let right_col = extract_column_from_expr(right)?;
                    return Ok((left_col, right_col));
                }
            }
            Err(DriftError::InvalidQuery(
                "Complex JOIN conditions not yet supported".to_string()
            ))
        }
        sqlparser::ast::JoinConstraint::Using(columns) => {
            if columns.is_empty() {
                Err(DriftError::InvalidQuery("USING clause requires columns".to_string()))
            } else {
                let col = columns[0].value.clone();
                Ok((col.clone(), col))
            }
        }
        _ => Err(DriftError::InvalidQuery(
            "JOIN constraint type not supported".to_string()
        )),
    }
}

fn extract_column_from_expr(expr: &sqlparser::ast::Expr) -> Result<String> {
    match expr {
        sqlparser::ast::Expr::Identifier(ident) => Ok(ident.value.clone()),
        sqlparser::ast::Expr::CompoundIdentifier(idents) => {
            // Take the last part (column name)
            idents.last()
                .map(|i| i.value.clone())
                .ok_or_else(|| DriftError::InvalidQuery("Invalid column reference".to_string()))
        }
        _ => Err(DriftError::InvalidQuery(
            "Expected column reference in JOIN condition".to_string()
        )),
    }
}

fn execute_sql_insert(
    engine: &mut Engine,
    table_name: &sqlparser::ast::ObjectName,
    columns: &[sqlparser::ast::Ident],
    source: &Box<SqlQuery>,
) -> Result<QueryResult> {
    let table = table_name.to_string();

    // Extract values from VALUES clause
    let values = match source.body.as_ref() {
        SetExpr::Values(values) => {
            if values.rows.is_empty() || values.rows[0].is_empty() {
                return Err(DriftError::InvalidQuery("No values provided".to_string()));
            }
            &values.rows[0]
        }
        _ => return Err(DriftError::InvalidQuery("Only VALUES clause supported".to_string())),
    };

    // Build data object
    let mut data = serde_json::Map::new();
    for (i, column) in columns.iter().enumerate() {
        if let Some(value_expr) = values.get(i) {
            let value = expr_to_json_value(value_expr)?;
            data.insert(column.value.clone(), value);
        }
    }

    let driftql_query = Query::Insert {
        table,
        data: json!(data),
    };

    engine.execute_query(driftql_query)
}

fn extract_table_name(table: &TableFactor) -> Result<String> {
    match table {
        TableFactor::Table { name, .. } => Ok(name.to_string()),
        _ => Err(DriftError::InvalidQuery(
            "Complex table expressions not supported".to_string()
        )),
    }
}

fn extract_table_name_from_join(table: &TableFactor) -> Result<String> {
    extract_table_name(table)
}

fn parse_where_clause(expr: &sqlparser::ast::Expr) -> Result<Vec<WhereCondition>> {
    // Simplified WHERE clause parsing
    match expr {
        sqlparser::ast::Expr::BinaryOp { left, op, right } => {
            let column = extract_column_from_expr(left)?;
            let value = expr_to_json_value(right)?;

            let operator = match op {
                BinaryOperator::Eq => "=",
                BinaryOperator::NotEq => "!=",
                BinaryOperator::Lt => "<",
                BinaryOperator::LtEq => "<=",
                BinaryOperator::Gt => ">",
                BinaryOperator::GtEq => ">=",
                _ => return Err(DriftError::InvalidQuery(
                    "Operator not supported in WHERE clause".to_string()
                )),
            };

            Ok(vec![WhereCondition {
                column,
                operator: operator.to_string(),
                value,
            }])
        }
        _ => Ok(vec![]),
    }
}

fn expr_to_json_value(expr: &sqlparser::ast::Expr) -> Result<Value> {
    match expr {
        sqlparser::ast::Expr::Value(val) => sql_value_to_json(val),
        sqlparser::ast::Expr::Identifier(ident) => Ok(json!(ident.value)),
        _ => Ok(Value::Null),
    }
}

fn sql_value_to_json(val: &sqlparser::ast::Value) -> Result<Value> {
    match val {
        sqlparser::ast::Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Ok(json!(i))
            } else if let Ok(f) = n.parse::<f64>() {
                Ok(json!(f))
            } else {
                Ok(json!(n))
            }
        }
        sqlparser::ast::Value::SingleQuotedString(s) |
        sqlparser::ast::Value::DoubleQuotedString(s) => Ok(json!(s)),
        sqlparser::ast::Value::Boolean(b) => Ok(json!(b)),
        sqlparser::ast::Value::Null => Ok(Value::Null),
        _ => Ok(Value::Null),
    }
}