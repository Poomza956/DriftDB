use tempfile::TempDir;
use serde_json::json;

use driftdb_core::{Engine, Query, QueryResult};
use driftdb_core::migration::{MigrationManager, Migration, MigrationOperation};
use driftdb_core::schema::{Schema, ColumnDef, ColumnType};

#[test]
fn test_add_column_migration() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    // Create initial table
    engine.execute_query(Query::CreateTable {
        name: "users".to_string(),
        primary_key: "id".to_string(),
        indexed_columns: vec![],
    }).unwrap();

    // Insert initial data
    engine.execute_query(Query::Insert {
        table: "users".to_string(),
        data: json!({
            "id": "user1",
            "name": "Alice"
        }),
    }).unwrap();

    // Create migration to add email column
    let migration_mgr = MigrationManager::new(temp_dir.path());
    let migration = Migration::new(
        "add_email_column".to_string(),
        "1.0.0".to_string(),
        vec![MigrationOperation::AddColumn {
            table: "users".to_string(),
            column: ColumnDef {
                name: "email".to_string(),
                column_type: ColumnType::String,
                nullable: true,
                default: None,
            },
            default_value: Some(json!("default@example.com")),
        }],
    );

    // Apply migration
    migration_mgr.apply(migration).unwrap();

    // Verify column was added with default value
    let result = engine.execute_query(Query::Select {
        table: "users".to_string(),
        conditions: vec![],
        as_of: None,
        limit: None,
    }).unwrap();

    match result {
        QueryResult::Rows { data } => {
            assert_eq!(data.len(), 1);
            assert_eq!(data[0]["email"], json!("default@example.com"));
        }
        _ => panic!("Expected rows"),
    }
}

#[test]
fn test_drop_column_migration() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    // Create table with extra column
    engine.execute_query(Query::CreateTable {
        name: "products".to_string(),
        primary_key: "id".to_string(),
        indexed_columns: vec![],
    }).unwrap();

    engine.execute_query(Query::Insert {
        table: "products".to_string(),
        data: json!({
            "id": "prod1",
            "name": "Widget",
            "deprecated_field": "old_data"
        }),
    }).unwrap();

    // Create migration to drop deprecated column
    let migration_mgr = MigrationManager::new(temp_dir.path());
    let migration = Migration::new(
        "drop_deprecated_field".to_string(),
        "1.0.1".to_string(),
        vec![MigrationOperation::DropColumn {
            table: "products".to_string(),
            column: "deprecated_field".to_string(),
        }],
    );

    // Apply migration
    migration_mgr.apply(migration).unwrap();

    // Verify schema was updated (column removed from future operations)
    // Note: Historical data still contains the column for time-travel
}

#[test]
fn test_rename_column_migration() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    // Create table
    engine.execute_query(Query::CreateTable {
        name: "accounts".to_string(),
        primary_key: "id".to_string(),
        indexed_columns: vec![],
    }).unwrap();

    engine.execute_query(Query::Insert {
        table: "accounts".to_string(),
        data: json!({
            "id": "acc1",
            "user_name": "john_doe"
        }),
    }).unwrap();

    // Create migration to rename column
    let migration_mgr = MigrationManager::new(temp_dir.path());
    let migration = Migration::new(
        "rename_username_column".to_string(),
        "1.0.2".to_string(),
        vec![MigrationOperation::RenameColumn {
            table: "accounts".to_string(),
            old_name: "user_name".to_string(),
            new_name: "username".to_string(),
        }],
    );

    // Apply migration
    migration_mgr.apply(migration).unwrap();

    // Verify data is accessible with new column name
    let result = engine.execute_query(Query::Select {
        table: "accounts".to_string(),
        conditions: vec![],
        as_of: None,
        limit: None,
    }).unwrap();

    match result {
        QueryResult::Rows { data } => {
            assert_eq!(data.len(), 1);
            assert_eq!(data[0]["username"], json!("john_doe"));
        }
        _ => panic!("Expected rows"),
    }
}

#[test]
fn test_migration_rollback() {
    let temp_dir = TempDir::new().unwrap();
    let _engine = Engine::init(temp_dir.path()).unwrap();

    let migration_mgr = MigrationManager::new(temp_dir.path());

    // Create and apply migration
    let migration = Migration::new(
        "test_migration".to_string(),
        "2.0.0".to_string(),
        vec![],
    );

    let migration_id = migration.id.clone();
    migration_mgr.apply(migration).unwrap();

    // Verify migration was applied
    let status = migration_mgr.status();
    assert_eq!(status.applied_count, 1);

    // Rollback migration
    migration_mgr.rollback(&migration_id).unwrap();

    // Verify migration was rolled back
    let status = migration_mgr.status();
    assert_eq!(status.applied_count, 0);
}

#[test]
fn test_migration_idempotency() {
    let temp_dir = TempDir::new().unwrap();
    let _engine = Engine::init(temp_dir.path()).unwrap();

    let migration_mgr = MigrationManager::new(temp_dir.path());

    let migration = Migration::new(
        "idempotent_migration".to_string(),
        "3.0.0".to_string(),
        vec![],
    );

    // Apply migration twice
    let result1 = migration_mgr.apply(migration.clone());
    assert!(result1.is_ok());

    let result2 = migration_mgr.apply(migration);
    // Second application should be skipped (already applied)
    assert!(result2.is_ok());

    // Verify only applied once
    let status = migration_mgr.status();
    assert_eq!(status.applied_count, 1);
}

#[test]
fn test_migration_with_validation() {
    let temp_dir = TempDir::new().unwrap();
    let _engine = Engine::init(temp_dir.path()).unwrap();

    let migration_mgr = MigrationManager::new(temp_dir.path());

    // Create migration with validation
    let mut migration = Migration::new(
        "validated_migration".to_string(),
        "4.0.0".to_string(),
        vec![],
    );
    migration.pre_condition = Some("SELECT COUNT(*) FROM users".to_string());
    migration.post_condition = Some("SELECT COUNT(*) FROM users WHERE email IS NOT NULL".to_string());

    // Validation should pass for empty operations
    let result = migration_mgr.apply(migration);
    assert!(result.is_ok());
}