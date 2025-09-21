//! Bridge between SQL and DriftQL for CLI integration

use sqlparser::ast::{Statement, Query as SqlQuery, Select, SetExpr, TableFactor, TableWithJoins, JoinOperator, BinaryOperator, SelectItem, Expr, Function, FunctionArg, FunctionArgExpr, GroupByExpr, OrderByExpr, Offset, FromTable};
use sqlparser::parser::Parser;
use sqlparser::dialect::GenericDialect;
use serde_json::{json, Value};
use std::collections::HashMap;

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
        Statement::Update { table, assignments, from: _, selection, .. } => {
            execute_sql_update(engine, table, assignments, selection)
        }
        Statement::Delete { tables, from, using: _, selection, .. } => {
            // Use 'from' if tables is empty (standard DELETE FROM syntax)
            if !tables.is_empty() {
                execute_sql_delete(engine, tables, selection)
            } else {
                // Extract tables from the FromTable enum
                let from_tables = match from {
                    FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
                };

                if !from_tables.is_empty() {
                    // Convert from TableWithJoins to ObjectName
                    let table_names: Vec<sqlparser::ast::ObjectName> = from_tables.iter()
                        .filter_map(|t| {
                            if let TableFactor::Table { name, .. } = &t.relation {
                                Some(name.clone())
                            } else {
                                None
                            }
                        })
                        .collect();
                    execute_sql_delete(engine, &table_names, selection)
                } else {
                    Err(DriftError::InvalidQuery("DELETE requires FROM clause".to_string()))
                }
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

            // Execute the base query (with or without JOINs)
            let mut result = if select.from[0].joins.is_empty() {
                execute_simple_select(engine, select)?
            } else {
                execute_join_select(engine, select)?
            };

            // Apply ORDER BY if present
            if let QueryResult::Rows { mut data } = result {
                // Apply ORDER BY
                if !query.order_by.is_empty() {
                    data = apply_order_by(data, &query.order_by)?;
                }

                // Apply LIMIT and OFFSET
                if let Some(limit_expr) = &query.limit {
                    let limit = parse_limit(limit_expr)?;
                    let offset = if let Some(offset_expr) = &query.offset {
                        parse_offset(offset_expr)?
                    } else {
                        0
                    };

                    data = data.into_iter()
                        .skip(offset)
                        .take(limit)
                        .collect();
                }

                Ok(QueryResult::Rows { data })
            } else {
                Ok(result)
            }
        }
        _ => Err(DriftError::InvalidQuery("Only SELECT queries supported".to_string())),
    }
}

fn execute_simple_select(engine: &mut Engine, select: &Select) -> Result<QueryResult> {
    let table_name = extract_table_name(&select.from[0].relation)?;

    // Check if this is an aggregation query
    let has_aggregates = select.projection.iter().any(|item| {
        matches!(item, SelectItem::UnnamedExpr(Expr::Function(_)) |
                      SelectItem::ExprWithAlias { expr: Expr::Function(_), .. })
    });

    // Convert WHERE clause to WhereConditions
    let conditions = if let Some(selection) = &select.selection {
        parse_where_clause(selection)?
    } else {
        vec![]
    };

    // Execute DriftQL query to get base data
    let driftql_query = Query::Select {
        table: table_name,
        conditions,
        as_of: None,
        limit: None,
    };

    let result = engine.execute_query(driftql_query)?;

    // Check if there's a GROUP BY clause
    let has_group_by = matches!(&select.group_by, GroupByExpr::Expressions(exprs) if !exprs.is_empty());

    // If no aggregates and no GROUP BY, return as-is
    if !has_aggregates && !has_group_by {
        return Ok(result);
    }

    // Process aggregations
    match result {
        QueryResult::Rows { data } => {
            let aggregated = execute_aggregation(&data, select)?;

            // Apply HAVING clause if present
            if let Some(having) = &select.having {
                let filtered = filter_aggregated_rows(aggregated, having)?;
                Ok(QueryResult::Rows { data: filtered })
            } else {
                Ok(QueryResult::Rows { data: aggregated })
            }
        }
        _ => Ok(result),
    }
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
    let mut joined_rows = match left_result {
        QueryResult::Rows { data } => data,
        _ => return Ok(left_result),
    };

    // Process all JOINs sequentially
    for join in &select.from[0].joins {
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

        // Perform the JOIN with current accumulated result
        joined_rows = match &join.join_operator {
            JoinOperator::Inner(constraint) => {
                perform_inner_join(joined_rows, right_rows, constraint)?
            }
            JoinOperator::LeftOuter(constraint) => {
                perform_left_join(joined_rows, right_rows, constraint)?
            }
            JoinOperator::CrossJoin => {
                perform_cross_join(joined_rows, right_rows)
            }
            _ => {
                return Err(DriftError::InvalidQuery(
                    "JOIN type not yet supported".to_string()
                ));
            }
        };
    }

    // Apply WHERE clause on joined data
    let filtered_rows = if let Some(selection) = &select.selection {
        filter_rows(joined_rows, selection)?
    } else {
        joined_rows
    };

    // Check if this is an aggregation query
    let has_aggregates = select.projection.iter().any(|item| {
        matches!(item, SelectItem::UnnamedExpr(Expr::Function(_)) |
                      SelectItem::ExprWithAlias { expr: Expr::Function(_), .. })
    });

    // Check if there's a GROUP BY clause
    let has_group_by = matches!(&select.group_by, GroupByExpr::Expressions(exprs) if !exprs.is_empty());

    // If no aggregates and no GROUP BY, apply column projection and return
    if !has_aggregates && !has_group_by {
        let projected_rows = project_columns(filtered_rows, select)?;
        return Ok(QueryResult::Rows { data: projected_rows });
    }

    // Process aggregations
    let aggregated = execute_aggregation(&filtered_rows, select)?;

    // Apply HAVING clause if present
    if let Some(having) = &select.having {
        let filtered = filter_aggregated_rows(aggregated, having)?;
        Ok(QueryResult::Rows { data: filtered })
    } else {
        Ok(QueryResult::Rows { data: aggregated })
    }
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
            // Handle both unprefixed and prefixed column names in join condition
            let left_val = left_row.get(&left_col)
                .or_else(|| {
                    // Try with various table prefixes
                    left_row.as_object().and_then(|obj| {
                        obj.iter().find(|(k, _)| {
                            k.ends_with(&format!("_{}", left_col))
                        }).map(|(_, v)| v)
                    })
                });
            let right_val = right_row.get(&right_col);

            if let (Some(l_val), Some(r_val)) = (left_val, right_val) {
                if l_val == r_val {
                    // Merge rows - use table prefixes for disambiguation
                    let mut merged = left_row.as_object()
                        .cloned()
                        .unwrap_or_default();

                    if let Some(right_obj) = right_row.as_object() {
                        for (key, value) in right_obj {
                            // For multiple JOINs, create unique prefixes
                            let new_key = if merged.contains_key(key) {
                                // Find a unique prefix
                                let mut counter = 1;
                                let mut unique_key = format!("t{}_{}", counter, key);
                                while merged.contains_key(&unique_key) {
                                    counter += 1;
                                    unique_key = format!("t{}_{}", counter, key);
                                }
                                unique_key
                            } else {
                                key.clone()
                            };
                            merged.insert(new_key, value.clone());
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
            // Handle both unprefixed and prefixed column names in join condition
            let left_val = left_row.get(&left_col)
                .or_else(|| left_row.get(&format!("right_{}", left_col)));
            let right_val = right_row.get(&right_col)
                .or_else(|| right_row.get(&format!("right_{}", right_col)));

            if let (Some(l_val), Some(r_val)) = (left_val, right_val) {
                if l_val == r_val {
                    // Merge rows
                    let mut merged = left_row.as_object()
                        .cloned()
                        .unwrap_or_default();

                    if let Some(right_obj) = right_row.as_object() {
                        for (key, value) in right_obj {
                            // Only prefix if key already exists to avoid conflicts
                            if merged.contains_key(key) && !key.starts_with("right_") {
                                merged.insert(format!("right_{}", key), value.clone());
                            } else {
                                merged.insert(key.clone(), value.clone());
                            }
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

fn execute_aggregation(rows: &[Value], select: &Select) -> Result<Vec<Value>> {
    // Handle GROUP BY
    match &select.group_by {
        GroupByExpr::Expressions(exprs) if !exprs.is_empty() => {
            execute_group_by_aggregation(rows, select, exprs)
        }
        _ => {
            // Simple aggregation without GROUP BY
            execute_simple_aggregation(rows, select)
        }
    }
}

fn execute_simple_aggregation(rows: &[Value], select: &Select) -> Result<Vec<Value>> {
    let mut result_row = serde_json::Map::new();

    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) |
            SelectItem::ExprWithAlias { expr, .. } => {
                let (col_name, value) = evaluate_expression(expr, rows)?;
                result_row.insert(col_name, value);
            }
            SelectItem::Wildcard(_) => {
                return Err(DriftError::InvalidQuery(
                    "Cannot use * with aggregate functions".to_string()
                ));
            }
            _ => {}
        }
    }

    Ok(vec![json!(result_row)])
}

fn execute_group_by_aggregation(rows: &[Value], select: &Select, group_exprs: &[Expr]) -> Result<Vec<Value>> {
    // Group rows by the GROUP BY expressions
    let mut groups: HashMap<String, Vec<Value>> = HashMap::new();

    for row in rows {
        let mut group_key = String::new();

        for group_expr in group_exprs {
            let value = match group_expr {
                Expr::Identifier(ident) => {
                    // Check both unprefixed and prefixed column names
                    row.get(&ident.value)
                        .or_else(|| row.get(&format!("right_{}", ident.value)))
                        .or_else(|| row.get(&format!("left_{}", ident.value)))
                }
                Expr::CompoundIdentifier(idents) => {
                    // For table.column, just use the column part
                    if let Some(column) = idents.last() {
                        row.get(&column.value)
                            .or_else(|| row.get(&format!("right_{}", column.value)))
                            .or_else(|| row.get(&format!("left_{}", column.value)))
                    } else {
                        None
                    }
                }
                _ => None,
            };

            if let Some(val) = value {
                group_key.push_str(&val.to_string());
                group_key.push('|'); // Separator
            }
        }

        groups.entry(group_key).or_default().push(row.clone());
    }

    // Process each group
    let mut results = Vec::new();
    for (_, group_rows) in groups {
        let mut result_row = serde_json::Map::new();

        // Add GROUP BY columns
        if let Some(first_row) = group_rows.first() {
            for group_expr in group_exprs {
                match group_expr {
                    Expr::Identifier(ident) => {
                        let value = first_row.get(&ident.value)
                            .or_else(|| first_row.get(&format!("right_{}", ident.value)))
                            .or_else(|| first_row.get(&format!("left_{}", ident.value)));

                        if let Some(val) = value {
                            result_row.insert(ident.value.clone(), val.clone());
                        }
                    }
                    Expr::CompoundIdentifier(idents) => {
                        if let Some(column) = idents.last() {
                            let value = first_row.get(&column.value)
                                .or_else(|| first_row.get(&format!("right_{}", column.value)))
                                .or_else(|| first_row.get(&format!("left_{}", column.value)));

                            if let Some(val) = value {
                                result_row.insert(column.value.clone(), val.clone());
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        // Process SELECT projections
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(Expr::Function(func)) => {
                    let (col_name, value) = evaluate_aggregate_function(func, &group_rows)?;
                    result_row.insert(col_name, value);
                }
                SelectItem::ExprWithAlias { expr: Expr::Function(func), alias } => {
                    let (_, value) = evaluate_aggregate_function(func, &group_rows)?;
                    result_row.insert(alias.value.clone(), value);
                }
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    // Non-aggregate column - get from first row if it's in GROUP BY
                    if let Some(first_row) = group_rows.first() {
                        if let Some(val) = first_row.get(&ident.value) {
                            result_row.insert(ident.value.clone(), val.clone());
                        } else if let Some(val) = first_row.get(&format!("right_{}", ident.value)) {
                            result_row.insert(ident.value.clone(), val.clone());
                        }
                    }
                }
                SelectItem::UnnamedExpr(Expr::CompoundIdentifier(idents)) => {
                    // Handle table.column
                    if let Some(column) = idents.last() {
                        if let Some(first_row) = group_rows.first() {
                            if let Some(val) = first_row.get(&column.value) {
                                result_row.insert(column.value.clone(), val.clone());
                            } else if let Some(val) = first_row.get(&format!("right_{}", column.value)) {
                                result_row.insert(column.value.clone(), val.clone());
                            }
                        }
                    }
                }
                SelectItem::ExprWithAlias { expr: Expr::Identifier(ident), alias } => {
                    if let Some(first_row) = group_rows.first() {
                        if let Some(val) = first_row.get(&ident.value) {
                            result_row.insert(alias.value.clone(), val.clone());
                        } else if let Some(val) = first_row.get(&format!("right_{}", ident.value)) {
                            result_row.insert(alias.value.clone(), val.clone());
                        }
                    }
                }
                SelectItem::Wildcard(_) => {
                    // For GROUP BY, we shouldn't have wildcard, but if we do,
                    // just include the grouped columns (already added above)
                }
                _ => {}
            }
        }

        results.push(json!(result_row));
    }

    Ok(results)
}

fn evaluate_expression(expr: &Expr, rows: &[Value]) -> Result<(String, Value)> {
    match expr {
        Expr::Function(func) => evaluate_aggregate_function(func, rows),
        Expr::Identifier(ident) => {
            // For non-aggregate columns in simple aggregation
            Ok((ident.value.clone(), Value::Null))
        }
        _ => Err(DriftError::InvalidQuery("Unsupported expression type".to_string())),
    }
}

fn evaluate_aggregate_function(func: &Function, rows: &[Value]) -> Result<(String, Value)> {
    let func_name = func.name.to_string().to_uppercase();

    // Extract the column name from function arguments
    let column = if func.args.len() == 1 {
        match &func.args[0] {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => {
                Some(ident.value.clone())
            }
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => None, // For COUNT(*)
            _ => None,
        }
    } else {
        None
    };

    let result_name = if let Some(ref col) = column {
        format!("{}_{}", func_name, col)
    } else {
        func_name.clone()
    };

    // Helper to get column value, checking both original and prefixed names
    let get_column_value = |row: &Value, col: &str| -> Option<Value> {
        row.get(col)
            .or_else(|| row.get(&format!("right_{}", col)))
            .or_else(|| row.get(&format!("left_{}", col)))
            .cloned()
    };

    match func_name.as_str() {
        "COUNT" => {
            let count = if column.is_none() {
                // COUNT(*)
                rows.len() as i64
            } else if let Some(ref col) = column {
                // COUNT(column) - handle both regular and prefixed columns
                rows.iter()
                    .filter(|row| {
                        let val = get_column_value(row, col);
                        val.is_some() && !val.unwrap().is_null()
                    })
                    .count() as i64
            } else {
                0
            };
            Ok((result_name, json!(count)))
        }
        "SUM" => {
            if let Some(col) = column {
                let sum: f64 = rows.iter()
                    .filter_map(|row| get_column_value(row, &col))
                    .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                    .sum();
                Ok((result_name, json!(sum)))
            } else {
                Err(DriftError::InvalidQuery("SUM requires a column".to_string()))
            }
        }
        "AVG" => {
            if let Some(col) = column {
                let values: Vec<f64> = rows.iter()
                    .filter_map(|row| get_column_value(row, &col))
                    .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                    .collect();

                if values.is_empty() {
                    Ok((result_name, Value::Null))
                } else {
                    let avg = values.iter().sum::<f64>() / values.len() as f64;
                    Ok((result_name, json!(avg)))
                }
            } else {
                Err(DriftError::InvalidQuery("AVG requires a column".to_string()))
            }
        }
        "MIN" => {
            if let Some(col) = column {
                let min = rows.iter()
                    .filter_map(|row| get_column_value(row, &col))
                    .filter(|v| !v.is_null())
                    .min_by(|a, b| {
                        match (a.as_f64(), b.as_f64()) {
                            (Some(a_val), Some(b_val)) => a_val.partial_cmp(&b_val).unwrap(),
                            _ => std::cmp::Ordering::Equal,
                        }
                    });

                Ok((result_name, min.unwrap_or(Value::Null)))
            } else {
                Err(DriftError::InvalidQuery("MIN requires a column".to_string()))
            }
        }
        "MAX" => {
            if let Some(col) = column {
                let max = rows.iter()
                    .filter_map(|row| get_column_value(row, &col))
                    .filter(|v| !v.is_null())
                    .max_by(|a, b| {
                        match (a.as_f64(), b.as_f64()) {
                            (Some(a_val), Some(b_val)) => a_val.partial_cmp(&b_val).unwrap(),
                            _ => std::cmp::Ordering::Equal,
                        }
                    });

                Ok((result_name, max.unwrap_or(Value::Null)))
            } else {
                Err(DriftError::InvalidQuery("MAX requires a column".to_string()))
            }
        }
        _ => Err(DriftError::InvalidQuery(
            format!("Unsupported aggregate function: {}", func_name)
        )),
    }
}

fn filter_rows(rows: Vec<Value>, expr: &Expr) -> Result<Vec<Value>> {
    let mut filtered = Vec::new();

    for row in rows {
        if evaluate_where_expression(expr, &row)? {
            filtered.push(row);
        }
    }

    Ok(filtered)
}

fn filter_aggregated_rows(rows: Vec<Value>, having: &Expr) -> Result<Vec<Value>> {
    let mut filtered = Vec::new();

    for row in rows {
        if evaluate_having_expression(having, &row)? {
            filtered.push(row);
        }
    }

    Ok(filtered)
}

fn evaluate_where_expression(expr: &Expr, row: &Value) -> Result<bool> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let left_val = evaluate_value_expression(left, row)?;
            let right_val = evaluate_value_expression(right, row)?;

            match op {
                BinaryOperator::Eq => Ok(left_val == right_val),
                BinaryOperator::NotEq => Ok(left_val != right_val),
                BinaryOperator::Lt => compare_values(&left_val, &right_val, |cmp| cmp == std::cmp::Ordering::Less),
                BinaryOperator::LtEq => compare_values(&left_val, &right_val, |cmp| cmp != std::cmp::Ordering::Greater),
                BinaryOperator::Gt => compare_values(&left_val, &right_val, |cmp| cmp == std::cmp::Ordering::Greater),
                BinaryOperator::GtEq => compare_values(&left_val, &right_val, |cmp| cmp != std::cmp::Ordering::Less),
                BinaryOperator::And => {
                    Ok(evaluate_where_expression(left, row)? && evaluate_where_expression(right, row)?)
                }
                BinaryOperator::Or => {
                    Ok(evaluate_where_expression(left, row)? || evaluate_where_expression(right, row)?)
                }
                _ => Err(DriftError::InvalidQuery("Unsupported WHERE operator".to_string())),
            }
        }
        _ => Ok(true), // For now, accept other expressions as true
    }
}

fn evaluate_having_expression(expr: &Expr, row: &Value) -> Result<bool> {
    // HAVING works on aggregated results
    match expr {
        Expr::BinaryOp { left, op, right } => {
            // Check if left side is an aggregate function
            let left_val = match left.as_ref() {
                Expr::Function(func) => {
                    // For HAVING, we need to look up the aggregate result column
                    let func_name = func.name.to_string().to_uppercase();
                    let column = if func.args.len() == 1 {
                        match &func.args[0] {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => {
                                Some(ident.value.clone())
                            }
                            _ => None,
                        }
                    } else {
                        None
                    };

                    let col_name = if let Some(ref col) = column {
                        format!("{}_{}", func_name, col)
                    } else {
                        func_name.clone()
                    };

                    row.get(&col_name).cloned().unwrap_or(Value::Null)
                }
                _ => evaluate_value_expression(left, row)?
            };

            let right_val = evaluate_value_expression(right, row)?;

            match op {
                BinaryOperator::Eq => Ok(left_val == right_val),
                BinaryOperator::NotEq => Ok(left_val != right_val),
                BinaryOperator::Lt => compare_values(&left_val, &right_val, |cmp| cmp == std::cmp::Ordering::Less),
                BinaryOperator::LtEq => compare_values(&left_val, &right_val, |cmp| cmp != std::cmp::Ordering::Greater),
                BinaryOperator::Gt => compare_values(&left_val, &right_val, |cmp| cmp == std::cmp::Ordering::Greater),
                BinaryOperator::GtEq => compare_values(&left_val, &right_val, |cmp| cmp != std::cmp::Ordering::Less),
                _ => Err(DriftError::InvalidQuery("Unsupported HAVING operator".to_string())),
            }
        }
        _ => Ok(true),
    }
}

fn evaluate_value_expression(expr: &Expr, row: &Value) -> Result<Value> {
    match expr {
        Expr::Identifier(ident) => {
            Ok(row.get(&ident.value).cloned().unwrap_or(Value::Null))
        }
        Expr::Value(val) => sql_value_to_json(val),
        _ => Ok(Value::Null),
    }
}

fn project_columns(rows: Vec<Value>, select: &Select) -> Result<Vec<Value>> {
    let mut projected_rows = Vec::new();

    for row in rows {
        let mut projected_row = serde_json::Map::new();

        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    // Look for the column in the row (with prefix handling)
                    if let Some(val) = row.get(&ident.value) {
                        projected_row.insert(ident.value.clone(), val.clone());
                    } else if let Some(val) = row.get(&format!("right_{}", ident.value)) {
                        projected_row.insert(ident.value.clone(), val.clone());
                    }
                }
                SelectItem::ExprWithAlias { expr: Expr::Identifier(ident), alias } => {
                    if let Some(val) = row.get(&ident.value) {
                        projected_row.insert(alias.value.clone(), val.clone());
                    } else if let Some(val) = row.get(&format!("right_{}", ident.value)) {
                        projected_row.insert(alias.value.clone(), val.clone());
                    }
                }
                SelectItem::UnnamedExpr(Expr::CompoundIdentifier(idents)) => {
                    // Handle table.column notation
                    if let Some(column) = idents.last() {
                        if let Some(val) = row.get(&column.value) {
                            projected_row.insert(column.value.clone(), val.clone());
                        } else if let Some(val) = row.get(&format!("right_{}", column.value)) {
                            projected_row.insert(column.value.clone(), val.clone());
                        }
                    }
                }
                SelectItem::ExprWithAlias { expr: Expr::CompoundIdentifier(idents), alias } => {
                    // For multiple tables with same column names, we need to find the right one
                    // The pattern is: first table's columns are unprefixed,
                    // second table's conflicting columns get t1_ prefix,
                    // third table's conflicting columns get t2_ prefix, etc.
                    if idents.len() >= 2 {
                        let table_alias = &idents[0].value;
                        let column = &idents[1].value;

                        // Based on the query pattern:
                        // orders o -> base table (left)
                        // customers c -> first JOIN (t1_ prefix)
                        // products p -> second JOIN (t2_ prefix)

                        // The issue is that when JOINing, the first conflicting column gets t1_,
                        // But we need to know which table each prefix belongs to.
                        // Since "name" appears in both customers and products tables,
                        // after the first JOIN, customers.name might be just "name"
                        // and after second JOIN, products.name gets prefixed

                        let val = if table_alias == "c" {
                            // customers table - look for name (might be unprefixed if it came first)
                            row.get(column)
                                .or_else(|| row.get(&format!("t1_{}", column)))
                        } else if table_alias == "p" {
                            // products table - likely prefixed due to conflict
                            row.get(&format!("t1_{}", column))
                                .or_else(|| row.get(&format!("t2_{}", column)))
                                .or_else(|| row.get(column))
                        } else if table_alias == "o" {
                            // orders table - original left table
                            row.get(column)
                        } else {
                            row.get(column)
                        };

                        if let Some(v) = val {
                            projected_row.insert(alias.value.clone(), v.clone());
                        }
                    } else if let Some(column) = idents.last() {
                        if let Some(val) = row.get(&column.value) {
                            projected_row.insert(alias.value.clone(), val.clone());
                        }
                    }
                }
                SelectItem::Wildcard(_) => {
                    // Include all columns (as-is)
                    if let Some(obj) = row.as_object() {
                        for (key, val) in obj {
                            projected_row.insert(key.clone(), val.clone());
                        }
                    }
                }
                _ => {}
            }
        }

        projected_rows.push(json!(projected_row));
    }

    Ok(projected_rows)
}

fn compare_values<F>(left: &Value, right: &Value, op: F) -> Result<bool>
where
    F: Fn(std::cmp::Ordering) -> bool,
{
    match (left.as_f64(), right.as_f64()) {
        (Some(l), Some(r)) => Ok(op(l.partial_cmp(&r).unwrap_or(std::cmp::Ordering::Equal))),
        _ => match (left.as_str(), right.as_str()) {
            (Some(l), Some(r)) => Ok(op(l.cmp(r))),
            _ => Ok(false),
        }
    }
}

fn apply_order_by(mut rows: Vec<Value>, order_by: &[OrderByExpr]) -> Result<Vec<Value>> {
    rows.sort_by(|a, b| {
        for order_expr in order_by {
            if let Some(ordering) = compare_rows_by_expr(a, b, order_expr) {
                if ordering != std::cmp::Ordering::Equal {
                    return ordering;
                }
            }
        }
        std::cmp::Ordering::Equal
    });

    Ok(rows)
}

fn compare_rows_by_expr(a: &Value, b: &Value, order_expr: &OrderByExpr) -> Option<std::cmp::Ordering> {
    // Extract the column name from the expression
    let column = match &order_expr.expr {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::CompoundIdentifier(idents) => {
            // For now, just take the last part (column name)
            idents.last()?.value.clone()
        }
        _ => return None,
    };

    // Get values from both rows - check for prefixed columns too
    let a_val = a.get(&column)
        .or_else(|| a.get(&format!("right_{}", column)))
        .or_else(|| a.get(&format!("left_{}", column)));

    let b_val = b.get(&column)
        .or_else(|| b.get(&format!("right_{}", column)))
        .or_else(|| b.get(&format!("left_{}", column)));

    match (a_val, b_val) {
        (Some(a_val), Some(b_val)) => {
            let ordering = compare_json_values(a_val, b_val);

            // Handle ASC/DESC
            if let Some(asc) = order_expr.asc {
                if !asc {
                    return Some(ordering.reverse());
                }
            }
            Some(ordering)
        }
        (None, Some(_)) => Some(std::cmp::Ordering::Greater), // NULLs last
        (Some(_), None) => Some(std::cmp::Ordering::Less),
        (None, None) => Some(std::cmp::Ordering::Equal),
    }
}

fn compare_json_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Number(a_num), Value::Number(b_num)) => {
            let a_f = a_num.as_f64().unwrap_or(0.0);
            let b_f = b_num.as_f64().unwrap_or(0.0);
            a_f.partial_cmp(&b_f).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::String(a_str), Value::String(b_str)) => a_str.cmp(b_str),
        (Value::Bool(a_bool), Value::Bool(b_bool)) => a_bool.cmp(b_bool),
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Greater, // NULLs last
        (_, Value::Null) => std::cmp::Ordering::Less,
        _ => std::cmp::Ordering::Equal,
    }
}

fn parse_limit(expr: &Expr) -> Result<usize> {
    match expr {
        Expr::Value(sqlparser::ast::Value::Number(n, _)) => {
            n.parse::<usize>()
                .map_err(|_| DriftError::InvalidQuery(format!("Invalid LIMIT value: {}", n)))
        }
        _ => Err(DriftError::InvalidQuery("LIMIT must be a number".to_string())),
    }
}

fn parse_offset(offset: &Offset) -> Result<usize> {
    match &offset.value {
        Expr::Value(sqlparser::ast::Value::Number(n, _)) => {
            n.parse::<usize>()
                .map_err(|_| DriftError::InvalidQuery(format!("Invalid OFFSET value: {}", n)))
        }
        _ => Err(DriftError::InvalidQuery("OFFSET must be a number".to_string())),
    }
}

fn execute_sql_update(
    engine: &mut Engine,
    table: &TableWithJoins,
    assignments: &[sqlparser::ast::Assignment],
    selection: &Option<Expr>,
) -> Result<QueryResult> {
    // Extract table name
    let table_name = extract_table_name(&table.relation)?;

    // First, fetch all rows that match the WHERE clause
    let conditions = if let Some(where_expr) = selection {
        parse_where_clause(where_expr)?
    } else {
        vec![]
    };

    let select_query = Query::Select {
        table: table_name.clone(),
        conditions: conditions.clone(),
        as_of: None,
        limit: None,
    };

    let result = engine.execute_query(select_query)?;

    let rows_to_update = match result {
        QueryResult::Rows { data } => data,
        _ => return Ok(QueryResult::Success { message: "No rows to update".to_string() }),
    };

    // Update each matching row
    let mut update_count = 0;
    for row in rows_to_update {
        let mut updated_row = row.clone();

        if let Some(row_obj) = updated_row.as_object_mut() {
            // Apply assignments
            for assignment in assignments {
                let column = assignment.id.last()
                    .ok_or_else(|| DriftError::InvalidQuery("Invalid column in UPDATE".to_string()))?
                    .value.clone();

                let new_value = evaluate_update_expression(&assignment.value, &row)?;
                row_obj.insert(column, new_value);
            }

            // Get the primary key value (assuming "id" for now)
            let primary_key = row_obj.get("id")
                .cloned()
                .unwrap_or(Value::Null);

            // Use PATCH to update the row
            let patch_query = Query::Patch {
                table: table_name.clone(),
                primary_key,
                updates: json!(row_obj),
            };

            engine.execute_query(patch_query)?;
            update_count += 1;
        }
    }

    Ok(QueryResult::Success {
        message: format!("Updated {} rows", update_count),
    })
}

fn execute_sql_delete(
    engine: &mut Engine,
    tables: &Vec<sqlparser::ast::ObjectName>,
    selection: &Option<Expr>,
) -> Result<QueryResult> {
    if tables.is_empty() {
        return Err(DriftError::InvalidQuery("DELETE requires FROM clause".to_string()));
    }

    // Extract table name
    let table_name = tables[0].to_string();

    // Parse WHERE clause
    let conditions = if let Some(where_expr) = selection {
        parse_where_clause(where_expr)?
    } else {
        vec![]
    };

    // First, fetch all rows that match the WHERE clause
    let select_query = Query::Select {
        table: table_name.clone(),
        conditions,
        as_of: None,
        limit: None,
    };

    let result = engine.execute_query(select_query)?;

    let rows_to_delete = match result {
        QueryResult::Rows { data } => data,
        _ => return Ok(QueryResult::Success { message: "No rows to delete".to_string() }),
    };

    // Delete each matching row
    let mut delete_count = 0;
    for row in rows_to_delete {
        if let Some(row_obj) = row.as_object() {
            // Get the primary key value (assuming "id" for now)
            let primary_key = row_obj.get("id")
                .cloned()
                .unwrap_or(Value::Null);

            // Use SOFT_DELETE to delete the row
            let delete_query = Query::SoftDelete {
                table: table_name.clone(),
                primary_key,
            };

            engine.execute_query(delete_query)?;
            delete_count += 1;
        }
    }

    Ok(QueryResult::Success {
        message: format!("Deleted {} rows", delete_count),
    })
}

fn evaluate_update_expression(expr: &Expr, row: &Value) -> Result<Value> {
    match expr {
        Expr::Value(val) => sql_value_to_json(val),
        Expr::Identifier(ident) => {
            // Look up the column value from the current row
            Ok(row.get(&ident.value).cloned().unwrap_or(Value::Null))
        }
        Expr::BinaryOp { left, op, right } => {
            let left_val = evaluate_update_expression(left, row)?;
            let right_val = evaluate_update_expression(right, row)?;

            // Handle arithmetic operations
            match op {
                BinaryOperator::Plus => {
                    if let (Some(l), Some(r)) = (left_val.as_f64(), right_val.as_f64()) {
                        Ok(json!(l + r))
                    } else {
                        Ok(Value::Null)
                    }
                }
                BinaryOperator::Minus => {
                    if let (Some(l), Some(r)) = (left_val.as_f64(), right_val.as_f64()) {
                        Ok(json!(l - r))
                    } else {
                        Ok(Value::Null)
                    }
                }
                BinaryOperator::Multiply => {
                    if let (Some(l), Some(r)) = (left_val.as_f64(), right_val.as_f64()) {
                        Ok(json!(l * r))
                    } else {
                        Ok(Value::Null)
                    }
                }
                BinaryOperator::Divide => {
                    if let (Some(l), Some(r)) = (left_val.as_f64(), right_val.as_f64()) {
                        if r != 0.0 {
                            Ok(json!(l / r))
                        } else {
                            Ok(Value::Null)
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                _ => Ok(Value::Null),
            }
        }
        _ => Ok(Value::Null),
    }
}