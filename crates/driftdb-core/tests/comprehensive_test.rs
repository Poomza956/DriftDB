use driftdb_core::*;
use tempfile::TempDir;
use std::sync::Arc;
use std::time::{SystemTime, Duration};
use tokio::sync::Barrier;
use serde_json::json;

/// Comprehensive test suite for DriftDB production features
mod comprehensive_tests {
    use super::*;

    #[tokio::test]
    async fn test_full_authentication_flow() {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = Engine::init(temp_dir.path()).unwrap();

        // Enable authentication
        let auth_config = auth::AuthConfig {
            enable_rbac: true,
            session_timeout: Duration::from_secs(3600),
            max_sessions_per_user: 5,
            password_policy: auth::PasswordPolicy {
                min_length: 8,
                require_uppercase: true,
                require_lowercase: true,
                require_numbers: true,
                require_special: false,
                max_age_days: 90,
            },
            enable_mfa: true,
            audit_auth_events: true,
        };

        engine.enable_authentication(auth_config).unwrap();

        // Create roles
        engine.create_role("admin", vec![
            auth::Permission::All,
        ]).unwrap();

        engine.create_role("reader", vec![
            auth::Permission::Select,
            auth::Permission::Describe,
        ]).unwrap();

        engine.create_role("writer", vec![
            auth::Permission::Select,
            auth::Permission::Insert,
            auth::Permission::Update,
            auth::Permission::Delete,
        ]).unwrap();

        // Create users
        engine.create_user("admin_user", "Admin@123", vec!["admin".to_string()]).unwrap();
        engine.create_user("read_user", "Reader@123", vec!["reader".to_string()]).unwrap();
        engine.create_user("write_user", "Writer@123", vec!["writer".to_string()]).unwrap();

        // Test authentication
        let admin_session = engine.authenticate("admin_user", "Admin@123", None).unwrap();
        assert!(admin_session.is_valid());

        // Test authorization
        assert!(engine.check_permission(&admin_session, auth::Permission::CreateTable).unwrap());
        assert!(engine.check_permission(&admin_session, auth::Permission::All).unwrap());

        let reader_session = engine.authenticate("read_user", "Reader@123", None).unwrap();
        assert!(engine.check_permission(&reader_session, auth::Permission::Select).unwrap());
        assert!(!engine.check_permission(&reader_session, auth::Permission::Insert).unwrap());

        // Test session management
        engine.logout(&admin_session).unwrap();
        assert!(!admin_session.is_valid());

        // Test password change
        engine.change_password("write_user", "Writer@123", "NewWriter@456").unwrap();
        let new_session = engine.authenticate("write_user", "NewWriter@456", None).unwrap();
        assert!(new_session.is_valid());

        // Test MFA setup
        let mfa_secret = engine.setup_mfa("write_user").unwrap();
        assert!(!mfa_secret.is_empty());

        // Test role revocation
        engine.revoke_role("write_user", "writer").unwrap();
        assert!(!engine.check_permission(&new_session, auth::Permission::Insert).unwrap());
    }

    #[tokio::test]
    async fn test_encryption_at_rest() {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = Engine::init(temp_dir.path()).unwrap();

        // Enable encryption
        let encryption_config = encryption::EncryptionConfig {
            algorithm: encryption::EncryptionAlgorithm::AesGcm256,
            key_rotation_interval: Duration::from_secs(86400),
            enable_wal_encryption: true,
            enable_backup_encryption: true,
            key_derivation: encryption::KeyDerivation::Argon2id,
        };

        engine.enable_encryption(encryption_config).unwrap();

        // Create encrypted table
        engine.create_table(
            "sensitive_data",
            vec![
                ColumnDef::new("id", DataType::String, true),
                ColumnDef::new("ssn", DataType::String, false),
                ColumnDef::new("credit_card", DataType::String, false),
                ColumnDef::new("balance", DataType::Float, false),
            ],
            vec![],
        ).unwrap();

        // Insert sensitive data
        for i in 0..100 {
            engine.insert("sensitive_data", json!({
                "id": format!("user_{}", i),
                "ssn": format!("123-45-{:04}", i),
                "credit_card": format!("4111-1111-1111-{:04}", i),
                "balance": 1000.0 * i as f64,
            })).unwrap();
        }

        // Verify data is encrypted on disk
        let table_path = temp_dir.path().join("tables").join("sensitive_data");
        let segment_files: Vec<_> = std::fs::read_dir(&table_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("seg"))
            .collect();

        assert!(!segment_files.is_empty());

        // Read raw segment file - should not contain plaintext
        for entry in segment_files {
            let raw_data = std::fs::read(entry.path()).unwrap();
            let raw_string = String::from_utf8_lossy(&raw_data);

            // Should not find sensitive data in plaintext
            assert!(!raw_string.contains("123-45-"));
            assert!(!raw_string.contains("4111-1111-1111-"));
            assert!(!raw_string.contains("user_"));
        }

        // Verify data can be decrypted and read
        let results = engine.query(&Query::select("sensitive_data")).unwrap();
        assert_eq!(results.rows.len(), 100);

        for (i, row) in results.rows.iter().enumerate() {
            assert_eq!(row["ssn"], json!(format!("123-45-{:04}", i)));
            assert_eq!(row["credit_card"], json!(format!("4111-1111-1111-{:04}", i)));
        }

        // Test key rotation
        engine.rotate_encryption_keys().unwrap();

        // Verify data still readable after rotation
        let results_after = engine.query(&Query::select("sensitive_data")).unwrap();
        assert_eq!(results_after.rows.len(), 100);
    }

    #[tokio::test]
    async fn test_distributed_consensus() {
        // Create cluster of 3 nodes
        let temp_dirs: Vec<TempDir> = (0..3).map(|_| TempDir::new().unwrap()).collect();
        let mut nodes = Vec::new();

        for (i, temp_dir) in temp_dirs.iter().enumerate() {
            let mut engine = Engine::init(temp_dir.path()).unwrap();

            let consensus_config = consensus::ConsensusConfig {
                node_id: format!("node_{}", i),
                peers: vec![
                    format!("node_{}", (i + 1) % 3),
                    format!("node_{}", (i + 2) % 3),
                ],
                election_timeout: Duration::from_millis(150),
                heartbeat_interval: Duration::from_millis(50),
                snapshot_interval: 1000,
                max_log_size: 10000,
            };

            engine.enable_consensus(consensus_config).unwrap();
            nodes.push(Arc::new(tokio::sync::RwLock::new(engine)));
        }

        // Wait for leader election
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Find the leader
        let mut leader_idx = None;
        for (i, node) in nodes.iter().enumerate() {
            let engine = node.read().await;
            if engine.is_leader() {
                leader_idx = Some(i);
                break;
            }
        }
        assert!(leader_idx.is_some(), "No leader elected");

        let leader_idx = leader_idx.unwrap();

        // Create table on leader
        {
            let mut leader = nodes[leader_idx].write().await;
            leader.create_table(
                "consensus_test",
                vec![
                    ColumnDef::new("id", DataType::String, true),
                    ColumnDef::new("value", DataType::Integer, false),
                    ColumnDef::new("timestamp", DataType::Timestamp, false),
                ],
                vec![],
            ).unwrap();
        }

        // Insert data on leader
        let num_records = 100;
        {
            let mut leader = nodes[leader_idx].write().await;
            for i in 0..num_records {
                leader.insert("consensus_test", json!({
                    "id": format!("record_{}", i),
                    "value": i,
                    "timestamp": SystemTime::now(),
                })).unwrap();
            }
        }

        // Wait for replication
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Verify all nodes have the same data
        for (i, node) in nodes.iter().enumerate() {
            let engine = node.read().await;
            let results = engine.query(&Query::select("consensus_test")).unwrap();
            assert_eq!(results.rows.len(), num_records, "Node {} has incorrect row count", i);

            for j in 0..num_records {
                assert!(results.rows.iter().any(|row|
                    row["id"] == json!(format!("record_{}", j))
                ), "Node {} missing record_{}", i, j);
            }
        }

        // Test leader failover
        {
            let leader = nodes[leader_idx].write().await;
            leader.shutdown().unwrap();
        }
        nodes.remove(leader_idx);

        // Wait for new leader election
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Find new leader
        let mut new_leader_idx = None;
        for (i, node) in nodes.iter().enumerate() {
            let engine = node.read().await;
            if engine.is_leader() {
                new_leader_idx = Some(i);
                break;
            }
        }
        assert!(new_leader_idx.is_some(), "No new leader elected after failover");

        // Continue operations on new leader
        {
            let mut new_leader = nodes[new_leader_idx.unwrap()].write().await;
            for i in num_records..num_records + 50 {
                new_leader.insert("consensus_test", json!({
                    "id": format!("record_{}", i),
                    "value": i,
                    "timestamp": SystemTime::now(),
                })).unwrap();
            }
        }

        // Verify data consistency after failover
        tokio::time::sleep(Duration::from_millis(500)).await;

        for node in nodes.iter() {
            let engine = node.read().await;
            let results = engine.query(&Query::select("consensus_test")).unwrap();
            assert_eq!(results.rows.len(), num_records + 50);
        }
    }

    #[tokio::test]
    async fn test_transaction_isolation_levels() {
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(tokio::sync::RwLock::new(
            Engine::init(temp_dir.path()).unwrap()
        ));

        // Create test table
        {
            let mut eng = engine.write().await;
            eng.create_table(
                "accounts",
                vec![
                    ColumnDef::new("id", DataType::String, true),
                    ColumnDef::new("balance", DataType::Float, false),
                    ColumnDef::new("locked", DataType::Boolean, false),
                ],
                vec![],
            ).unwrap();

            // Insert initial data
            eng.insert("accounts", json!({
                "id": "acc1",
                "balance": 1000.0,
                "locked": false,
            })).unwrap();
            eng.insert("accounts", json!({
                "id": "acc2",
                "balance": 2000.0,
                "locked": false,
            })).unwrap();
        }

        // Test READ_UNCOMMITTED
        {
            let engine_clone = engine.clone();
            let barrier = Arc::new(Barrier::new(2));
            let barrier_clone = barrier.clone();

            let handle1 = tokio::spawn(async move {
                let mut eng = engine_clone.write().await;
                let tx = eng.begin_transaction(transaction::IsolationLevel::ReadUncommitted).unwrap();

                eng.update_in_transaction(&tx, "accounts",
                    json!({"id": "acc1"}),
                    json!({"balance": 500.0})
                ).unwrap();

                barrier_clone.wait().await;
                tokio::time::sleep(Duration::from_millis(100)).await;

                eng.rollback_transaction(&tx).unwrap();
            });

            let engine_clone = engine.clone();
            let handle2 = tokio::spawn(async move {
                let eng = engine_clone.read().await;

                barrier.wait().await;

                let tx = eng.begin_transaction(transaction::IsolationLevel::ReadUncommitted).unwrap();
                let results = eng.query_in_transaction(&tx, &Query::select("accounts")
                    .where_clause("id = 'acc1'")).unwrap();

                // Should see uncommitted change
                assert_eq!(results.rows[0]["balance"], json!(500.0));
            });

            handle1.await.unwrap();
            handle2.await.unwrap();
        }

        // Test SERIALIZABLE
        {
            let engine_clone = engine.clone();
            let barrier = Arc::new(Barrier::new(2));
            let barrier_clone = barrier.clone();

            let handle1 = tokio::spawn(async move {
                let mut eng = engine_clone.write().await;
                let tx = eng.begin_transaction(transaction::IsolationLevel::Serializable).unwrap();

                let results = eng.query_in_transaction(&tx,
                    &Query::select("accounts").where_clause("id = 'acc1'")
                ).unwrap();
                let balance = results.rows[0]["balance"].as_f64().unwrap();

                barrier_clone.wait().await;
                tokio::time::sleep(Duration::from_millis(50)).await;

                eng.update_in_transaction(&tx, "accounts",
                    json!({"id": "acc1"}),
                    json!({"balance": balance + 100.0})
                ).unwrap();

                eng.commit_transaction(&tx).unwrap();
            });

            let engine_clone = engine.clone();
            let handle2 = tokio::spawn(async move {
                let mut eng = engine_clone.write().await;
                let tx = eng.begin_transaction(transaction::IsolationLevel::Serializable).unwrap();

                let results = eng.query_in_transaction(&tx,
                    &Query::select("accounts").where_clause("id = 'acc1'")
                ).unwrap();
                let balance = results.rows[0]["balance"].as_f64().unwrap();

                barrier.wait().await;

                let update_result = eng.update_in_transaction(&tx, "accounts",
                    json!({"id": "acc1"}),
                    json!({"balance": balance + 200.0})
                );

                // Should fail due to serialization conflict
                assert!(update_result.is_err() || eng.commit_transaction(&tx).is_err());
            });

            handle1.await.unwrap();
            handle2.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_crash_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_path_buf();

        // Phase 1: Create database and insert data
        {
            let mut engine = Engine::init(&db_path).unwrap();

            engine.create_table(
                "recovery_test",
                vec![
                    ColumnDef::new("id", DataType::Integer, true),
                    ColumnDef::new("data", DataType::String, false),
                    ColumnDef::new("timestamp", DataType::Timestamp, false),
                ],
                vec![],
            ).unwrap();

            // Insert data with WAL enabled
            for i in 0..1000 {
                engine.insert("recovery_test", json!({
                    "id": i,
                    "data": format!("test_data_{}", i),
                    "timestamp": SystemTime::now(),
                })).unwrap();
            }

            // Start a transaction but don't commit (simulating crash)
            let tx = engine.begin_transaction(transaction::IsolationLevel::ReadCommitted).unwrap();
            for i in 1000..1100 {
                engine.insert_in_transaction(&tx, "recovery_test", json!({
                    "id": i,
                    "data": format!("uncommitted_{}", i),
                    "timestamp": SystemTime::now(),
                })).unwrap();
            }

            // Simulate crash by dropping engine without commit
            std::mem::drop(engine);
        }

        // Phase 2: Corrupt some data to simulate crash
        {
            let wal_path = db_path.join("wal.log");
            if wal_path.exists() {
                // Truncate WAL to simulate incomplete write
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&wal_path)
                    .unwrap();
                let metadata = file.metadata().unwrap();
                let new_len = metadata.len() - 100; // Remove last 100 bytes
                file.set_len(new_len).unwrap();
            }
        }

        // Phase 3: Recover and verify
        {
            let mut engine = Engine::open(&db_path).unwrap();

            // Run recovery
            let recovery_result = engine.recover().unwrap();
            assert!(recovery_result.success);
            assert_eq!(recovery_result.recovered_transactions, 1000);
            assert_eq!(recovery_result.rolled_back_transactions, 0);

            // Verify committed data is present
            let results = engine.query(&Query::select("recovery_test")).unwrap();
            assert_eq!(results.rows.len(), 1000);

            // Verify uncommitted data was rolled back
            for row in &results.rows {
                let data = row["data"].as_str().unwrap();
                assert!(!data.starts_with("uncommitted_"));
            }

            // Verify we can continue operations
            for i in 1100..1200 {
                engine.insert("recovery_test", json!({
                    "id": i,
                    "data": format!("post_recovery_{}", i),
                    "timestamp": SystemTime::now(),
                })).unwrap();
            }

            let final_results = engine.query(&Query::select("recovery_test")).unwrap();
            assert_eq!(final_results.rows.len(), 1100);
        }
    }

    #[tokio::test]
    async fn test_backup_restore() {
        let temp_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let mut engine = Engine::init(temp_dir.path()).unwrap();

        // Enable backup system
        let backup_config = backup_enhanced::BackupConfig {
            backup_dir: backup_dir.path().to_path_buf(),
            compression: backup_enhanced::CompressionType::Zstd,
            encryption: Some(backup_enhanced::EncryptionConfig {
                algorithm: backup_enhanced::EncryptionAlgorithm::AesGcm256,
                key_management: backup_enhanced::KeyManagement::Automatic,
            }),
            retention_policy: backup_enhanced::RetentionPolicy {
                daily_backups: 7,
                weekly_backups: 4,
                monthly_backups: 12,
                yearly_backups: 5,
            },
            parallel_compression: true,
            verify_after_backup: true,
        };

        engine.enable_backup(backup_config).unwrap();

        // Create tables and insert data
        engine.create_table(
            "customers",
            vec![
                ColumnDef::new("id", DataType::Integer, true),
                ColumnDef::new("name", DataType::String, false),
                ColumnDef::new("email", DataType::String, false),
                ColumnDef::new("created", DataType::Timestamp, false),
            ],
            vec![IndexDef::btree("email", vec!["email".to_string()])],
        ).unwrap();

        for i in 0..10000 {
            engine.insert("customers", json!({
                "id": i,
                "name": format!("Customer {}", i),
                "email": format!("customer{}@example.com", i),
                "created": SystemTime::now(),
            })).unwrap();
        }

        // Take full backup
        let backup_result = engine.create_backup(
            backup_enhanced::BackupType::Full,
            Some("Test full backup".to_string())
        ).unwrap();
        assert!(backup_result.success);
        assert!(backup_result.size_bytes > 0);
        let full_backup_id = backup_result.backup_id.clone();

        // Make some changes
        for i in 10000..11000 {
            engine.insert("customers", json!({
                "id": i,
                "name": format!("New Customer {}", i),
                "email": format!("newcustomer{}@example.com", i),
                "created": SystemTime::now(),
            })).unwrap();
        }

        // Take incremental backup
        let incr_result = engine.create_backup(
            backup_enhanced::BackupType::Incremental,
            Some("Test incremental backup".to_string())
        ).unwrap();
        assert!(incr_result.success);
        assert!(incr_result.size_bytes < backup_result.size_bytes); // Incremental should be smaller

        // Make more changes
        for i in 0..100 {
            engine.update("customers",
                json!({"id": i}),
                json!({"name": format!("Updated Customer {}", i)})
            ).unwrap();
        }

        // Take differential backup
        let diff_result = engine.create_backup(
            backup_enhanced::BackupType::Differential,
            Some("Test differential backup".to_string())
        ).unwrap();
        assert!(diff_result.success);

        // Simulate disaster - destroy current database
        drop(engine);
        std::fs::remove_dir_all(temp_dir.path()).unwrap();

        // Restore from backup
        let restore_dir = TempDir::new().unwrap();
        let mut restored_engine = Engine::init(restore_dir.path()).unwrap();
        restored_engine.enable_backup(backup_config).unwrap();

        // Restore to latest point
        let restore_result = restored_engine.restore_backup(
            backup_enhanced::RestoreOptions {
                backup_id: None, // Use latest
                target_time: None,
                validate_before_restore: true,
                parallel_restore: true,
            }
        ).unwrap();
        assert!(restore_result.success);

        // Verify all data is restored
        let results = restored_engine.query(&Query::select("customers")).unwrap();
        assert_eq!(results.rows.len(), 11000);

        // Verify updates were restored
        let updated = restored_engine.query(
            &Query::select("customers").where_clause("id < 100")
        ).unwrap();
        for row in updated.rows {
            let name = row["name"].as_str().unwrap();
            assert!(name.starts_with("Updated Customer"));
        }

        // Test point-in-time recovery
        let pitr_result = restored_engine.restore_backup(
            backup_enhanced::RestoreOptions {
                backup_id: Some(full_backup_id),
                target_time: None,
                validate_before_restore: true,
                parallel_restore: true,
            }
        ).unwrap();
        assert!(pitr_result.success);

        // Should only have original 10000 records
        let pitr_results = restored_engine.query(&Query::select("customers")).unwrap();
        assert_eq!(pitr_results.rows.len(), 10000);
    }

    #[tokio::test]
    async fn test_security_monitoring() {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = Engine::init(temp_dir.path()).unwrap();

        // Enable security monitoring
        let security_config = security_monitor::SecurityConfig {
            enable_intrusion_detection: true,
            enable_anomaly_detection: true,
            enable_compliance_monitoring: true,
            compliance_frameworks: vec![
                security_monitor::ComplianceFramework::GDPR,
                security_monitor::ComplianceFramework::SOX,
                security_monitor::ComplianceFramework::HIPAA,
            ],
            alert_thresholds: security_monitor::AlertThresholds {
                failed_auth_attempts: 3,
                suspicious_query_score: 80,
                data_exfiltration_rows: 10000,
                privilege_escalation_attempts: 1,
            },
            enable_behavioral_analysis: true,
            quarantine_suspicious_sessions: true,
        };

        engine.enable_security_monitoring(security_config).unwrap();

        // Enable audit system
        let audit_config = audit::AuditConfig {
            enabled: true,
            log_queries: true,
            log_auth_events: true,
            log_ddl_events: true,
            log_dml_events: true,
            log_admin_events: true,
            retention_days: 90,
            compress_old_logs: true,
        };

        engine.enable_auditing(audit_config).unwrap();

        // Create sensitive table
        engine.create_table(
            "sensitive_data",
            vec![
                ColumnDef::new("id", DataType::Integer, true),
                ColumnDef::new("ssn", DataType::String, false),
                ColumnDef::new("medical_record", DataType::String, false),
            ],
            vec![],
        ).unwrap();

        // Simulate normal behavior
        for i in 0..10 {
            engine.insert("sensitive_data", json!({
                "id": i,
                "ssn": format!("123-45-{:04}", i),
                "medical_record": format!("MR{:06}", i),
            })).unwrap();
        }

        // Simulate suspicious activities

        // 1. Multiple failed authentication attempts
        for _ in 0..5 {
            let result = engine.authenticate("hacker", "wrongpass", Some("192.168.1.100".to_string()));
            assert!(result.is_err());
        }

        // Check if intrusion was detected
        let security_stats = engine.get_security_stats().unwrap();
        assert!(security_stats.intrusion_attempts > 0);
        assert!(security_stats.blocked_ips.contains(&"192.168.1.100".to_string()));

        // 2. Data exfiltration attempt
        let session = engine.create_session("test_user", None).unwrap();

        // Try to select large amount of sensitive data
        let result = engine.query_with_session(
            &Query::select("sensitive_data"),
            &session
        );

        // Should be blocked or flagged
        let alerts = engine.get_security_alerts().unwrap();
        assert!(alerts.iter().any(|a| matches!(a.alert_type,
            security_monitor::AlertType::DataExfiltration)));

        // 3. SQL injection attempt
        let injection_query = Query::raw(
            "SELECT * FROM sensitive_data WHERE id = 1 OR 1=1; DROP TABLE sensitive_data; --"
        );

        let result = engine.query(&injection_query);
        assert!(result.is_err() || engine.get_security_alerts().unwrap().iter()
            .any(|a| matches!(a.alert_type, security_monitor::AlertType::SqlInjection)));

        // 4. Privilege escalation attempt
        let user_session = engine.create_session("normal_user", None).unwrap();

        let escalation_result = engine.grant_role_with_session(
            &user_session,
            "normal_user",
            "admin"
        );
        assert!(escalation_result.is_err());

        let alerts = engine.get_security_alerts().unwrap();
        assert!(alerts.iter().any(|a| matches!(a.alert_type,
            security_monitor::AlertType::PrivilegeEscalation)));

        // 5. Verify compliance monitoring
        let compliance_report = engine.generate_compliance_report(
            security_monitor::ComplianceFramework::HIPAA
        ).unwrap();

        assert!(compliance_report.violations.is_empty() ||
            compliance_report.violations.iter().any(|v|
                v.rule == "HIPAA.Security.AccessControl"));

        // 6. Test behavioral analysis
        // Simulate unusual access pattern
        let midnight = SystemTime::now();
        for i in 0..100 {
            engine.query_with_metadata(&Query::select("sensitive_data"),
                json!({
                    "user": "night_user",
                    "ip": "10.0.0.1",
                    "timestamp": midnight,
                    "application": "suspicious_app"
                })
            );
        }

        let anomalies = engine.get_anomalies().unwrap();
        assert!(!anomalies.is_empty());
        assert!(anomalies.iter().any(|a| a.anomaly_type == "unusual_access_time"));

        // Verify audit trail
        let audit_events = engine.get_audit_events(
            Some(SystemTime::now() - Duration::from_secs(3600)),
            None,
            None
        ).unwrap();

        assert!(!audit_events.is_empty());
        assert!(audit_events.iter().any(|e|
            e.event_type == audit::AuditEventType::AuthenticationFailed));
        assert!(audit_events.iter().any(|e|
            e.event_type == audit::AuditEventType::SuspiciousActivity));
    }

    #[tokio::test]
    async fn test_query_optimization() {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = Engine::init(temp_dir.path()).unwrap();

        // Enable query optimization
        let optimization_config = query_performance::OptimizationConfig {
            enable_plan_cache: true,
            enable_result_cache: true,
            enable_adaptive_optimization: true,
            enable_materialized_views: true,
            enable_parallel_execution: true,
            enable_join_reordering: true,
            enable_subquery_optimization: true,
            enable_index_hints: true,
            cost_model_calibration: Default::default(),
            cache_size_mb: 256,
            parallel_threshold: 1000,
            statistics_update_threshold: 0.1,
        };

        engine.enable_query_optimization(optimization_config).unwrap();

        // Create tables for complex queries
        engine.create_table(
            "orders",
            vec![
                ColumnDef::new("order_id", DataType::Integer, true),
                ColumnDef::new("customer_id", DataType::Integer, false),
                ColumnDef::new("product_id", DataType::Integer, false),
                ColumnDef::new("quantity", DataType::Integer, false),
                ColumnDef::new("price", DataType::Float, false),
                ColumnDef::new("order_date", DataType::Timestamp, false),
            ],
            vec![
                IndexDef::btree("customer_idx", vec!["customer_id".to_string()]),
                IndexDef::btree("product_idx", vec!["product_id".to_string()]),
                IndexDef::btree("date_idx", vec!["order_date".to_string()]),
            ],
        ).unwrap();

        engine.create_table(
            "customers",
            vec![
                ColumnDef::new("customer_id", DataType::Integer, true),
                ColumnDef::new("name", DataType::String, false),
                ColumnDef::new("country", DataType::String, false),
                ColumnDef::new("segment", DataType::String, false),
            ],
            vec![
                IndexDef::btree("country_idx", vec!["country".to_string()]),
                IndexDef::btree("segment_idx", vec!["segment".to_string()]),
            ],
        ).unwrap();

        engine.create_table(
            "products",
            vec![
                ColumnDef::new("product_id", DataType::Integer, true),
                ColumnDef::new("name", DataType::String, false),
                ColumnDef::new("category", DataType::String, false),
                ColumnDef::new("supplier_id", DataType::Integer, false),
            ],
            vec![
                IndexDef::btree("category_idx", vec!["category".to_string()]),
            ],
        ).unwrap();

        // Insert test data
        for i in 0..1000 {
            engine.insert("customers", json!({
                "customer_id": i,
                "name": format!("Customer {}", i),
                "country": ["US", "UK", "DE", "FR", "JP"][i % 5],
                "segment": ["Premium", "Standard", "Basic"][i % 3],
            })).unwrap();
        }

        for i in 0..100 {
            engine.insert("products", json!({
                "product_id": i,
                "name": format!("Product {}", i),
                "category": ["Electronics", "Books", "Clothing", "Food"][i % 4],
                "supplier_id": i % 10,
            })).unwrap();
        }

        for i in 0..10000 {
            engine.insert("orders", json!({
                "order_id": i,
                "customer_id": i % 1000,
                "product_id": i % 100,
                "quantity": (i % 10) + 1,
                "price": 10.0 + (i % 100) as f64,
                "order_date": SystemTime::now(),
            })).unwrap();
        }

        // Test 1: Complex join query with optimization
        let complex_query = Query::raw(r#"
            SELECT c.name, c.country, p.name as product, o.quantity, o.price
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            JOIN products p ON o.product_id = p.product_id
            WHERE c.country = 'US' AND p.category = 'Electronics'
            ORDER BY o.price DESC
            LIMIT 100
        "#);

        let start = std::time::Instant::now();
        let result1 = engine.query(&complex_query).unwrap();
        let first_execution = start.elapsed();

        // Second execution should be faster due to plan cache
        let start = std::time::Instant::now();
        let result2 = engine.query(&complex_query).unwrap();
        let second_execution = start.elapsed();

        assert!(second_execution < first_execution);
        assert_eq!(result1.rows.len(), result2.rows.len());

        // Test 2: Subquery optimization
        let subquery = Query::raw(r#"
            SELECT * FROM orders
            WHERE customer_id IN (
                SELECT customer_id FROM customers
                WHERE segment = 'Premium' AND country = 'US'
            )
            AND product_id IN (
                SELECT product_id FROM products
                WHERE category = 'Electronics'
            )
        "#);

        let result = engine.query(&subquery).unwrap();
        assert!(!result.rows.is_empty());

        // Test 3: Create and use materialized view
        engine.create_materialized_view(
            "sales_summary",
            Query::raw(r#"
                SELECT
                    c.country,
                    p.category,
                    COUNT(*) as order_count,
                    SUM(o.quantity) as total_quantity,
                    SUM(o.price * o.quantity) as total_revenue
                FROM orders o
                JOIN customers c ON o.customer_id = c.customer_id
                JOIN products p ON o.product_id = p.product_id
                GROUP BY c.country, p.category
            "#)
        ).unwrap();

        // Query that should use the materialized view
        let summary_query = Query::raw(r#"
            SELECT country, category, total_revenue
            FROM sales_summary
            WHERE country = 'US'
        "#);

        let result = engine.query(&summary_query).unwrap();
        assert!(!result.rows.is_empty());

        // Get optimization statistics
        let stats = engine.get_query_optimizer().unwrap()
            .get_statistics().unwrap();

        assert!(stats.queries_optimized > 0);
        assert!(stats.cache_hits > 0);
        assert!(stats.joins_reordered > 0);

        // Test 4: Parallel execution for large scan
        let large_scan = Query::raw(r#"
            SELECT COUNT(*), AVG(price), MAX(quantity), MIN(order_date)
            FROM orders
            WHERE price > 50
        "#);

        let result = engine.query(&large_scan).unwrap();
        assert_eq!(result.rows.len(), 1);

        let final_stats = engine.get_query_optimizer().unwrap()
            .get_statistics().unwrap();
        assert!(final_stats.parallel_executions > 0);
    }
}