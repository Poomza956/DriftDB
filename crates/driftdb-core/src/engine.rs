use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::errors::{DriftError, Result};
use crate::events::Event;
use crate::index::IndexManager;
use crate::query::{Query, QueryResult};
use crate::schema::{ColumnDef, Schema};
use crate::snapshot::SnapshotManager;
use crate::storage::{Segment, TableStorage};
use crate::transaction::{IsolationLevel, TransactionManager};
use crate::constraints::ConstraintManager;
use crate::sequences::SequenceManager;
use crate::views::{ViewManager, ViewDefinition, ViewBuilder};
use crate::fulltext::{SearchManager, SearchConfig, SearchQuery, SearchResults};
use crate::triggers::{TriggerManager, TriggerDefinition, TriggerBuilder};
use crate::procedures::{ProcedureManager, ProcedureDefinition, ProcedureResult};
use crate::stats::{StatisticsManager, StatsConfig, DatabaseStatistics, QueryExecution};

pub struct Engine {
    base_path: PathBuf,
    pub(crate) tables: HashMap<String, Arc<TableStorage>>,
    indexes: HashMap<String, Arc<RwLock<IndexManager>>>,
    snapshots: HashMap<String, Arc<SnapshotManager>>,
    transaction_manager: Arc<RwLock<TransactionManager>>,
    constraint_manager: Arc<RwLock<ConstraintManager>>,
    sequence_manager: Arc<SequenceManager>,
    view_manager: Arc<ViewManager>,
    search_manager: Arc<SearchManager>,
    trigger_manager: Arc<TriggerManager>,
    procedure_manager: Arc<ProcedureManager>,
    stats_manager: Arc<RwLock<StatisticsManager>>,
}

impl Engine {
    /// Get the base path of the database
    pub fn base_path(&self) -> &Path {
        &self.base_path
    }

    pub fn open<P: AsRef<Path>>(base_path: P) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();

        if !base_path.exists() {
            return Err(DriftError::Other(format!(
                "Database path does not exist: {}",
                base_path.display()
            )));
        }

        let mut engine = Self {
            base_path: base_path.clone(),
            tables: HashMap::new(),
            indexes: HashMap::new(),
            snapshots: HashMap::new(),
            transaction_manager: Arc::new(RwLock::new(TransactionManager::new())),
            constraint_manager: Arc::new(RwLock::new(ConstraintManager::new())),
            view_manager: Arc::new(ViewManager::new()),
            search_manager: Arc::new(SearchManager::new()),
            trigger_manager: Arc::new(TriggerManager::new()),
            procedure_manager: Arc::new(ProcedureManager::new()),
            stats_manager: Arc::new(RwLock::new(StatisticsManager::new(StatsConfig::default()))),
            sequence_manager: Arc::new(SequenceManager::new()),
        };

        let tables_dir = base_path.join("tables");
        if tables_dir.exists() {
            for entry in fs::read_dir(&tables_dir)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    let table_name = entry.file_name().to_string_lossy().to_string();
                    engine.load_table(&table_name)?;
                }
            }
        }

        Ok(engine)
    }

    pub fn init<P: AsRef<Path>>(base_path: P) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();
        fs::create_dir_all(&base_path)?;
        fs::create_dir_all(base_path.join("tables"))?;

        Ok(Self {
            base_path,
            tables: HashMap::new(),
            indexes: HashMap::new(),
            snapshots: HashMap::new(),
            transaction_manager: Arc::new(RwLock::new(TransactionManager::new())),
            constraint_manager: Arc::new(RwLock::new(ConstraintManager::new())),
            sequence_manager: Arc::new(SequenceManager::new()),
        })
    }

    fn load_table(&mut self, table_name: &str) -> Result<()> {
        let storage = Arc::new(TableStorage::open(&self.base_path, table_name)?);

        let mut index_mgr = IndexManager::new(storage.path());
        index_mgr.load_indexes(&storage.schema().indexed_columns())?;

        let snapshot_mgr = SnapshotManager::new(storage.path());

        self.tables.insert(table_name.to_string(), storage.clone());
        self.indexes.insert(table_name.to_string(), Arc::new(RwLock::new(index_mgr)));
        self.snapshots.insert(table_name.to_string(), Arc::new(snapshot_mgr));

        Ok(())
    }

    pub fn create_table(
        &mut self,
        name: &str,
        primary_key: &str,
        indexed_columns: Vec<String>,
    ) -> Result<()> {
        if self.tables.contains_key(name) {
            return Err(DriftError::Other(format!("Table '{}' already exists", name)));
        }

        let mut columns = vec![ColumnDef {
            name: primary_key.to_string(),
            col_type: "string".to_string(),
            index: false,
        }];

        for col in &indexed_columns {
            if col != primary_key {
                columns.push(ColumnDef {
                    name: col.clone(),
                    col_type: "string".to_string(),
                    index: true,
                });
            }
        }

        let schema = Schema::new(name.to_string(), primary_key.to_string(), columns);
        schema.validate()?;

        let storage = Arc::new(TableStorage::create(&self.base_path, schema.clone())?);

        let mut index_mgr = IndexManager::new(storage.path());
        index_mgr.load_indexes(&schema.indexed_columns())?;

        let snapshot_mgr = SnapshotManager::new(storage.path());

        self.tables.insert(name.to_string(), storage);
        self.indexes.insert(name.to_string(), Arc::new(RwLock::new(index_mgr)));
        self.snapshots.insert(name.to_string(), Arc::new(snapshot_mgr));

        Ok(())
    }

    pub fn apply_event(&mut self, event: Event) -> Result<u64> {
        let storage = self
            .tables
            .get(&event.table_name)
            .ok_or_else(|| DriftError::TableNotFound(event.table_name.clone()))?
            .clone();

        let sequence = storage.append_event(event.clone())?;

        if let Some(index_mgr) = self.indexes.get(&event.table_name) {
            let mut index_mgr = index_mgr.write();
            index_mgr.update_indexes(&event, &storage.schema().indexed_columns())?;
            index_mgr.save_all()?;
        }

        Ok(sequence)
    }

    pub fn create_snapshot(&self, table_name: &str) -> Result<()> {
        let storage = self
            .tables
            .get(table_name)
            .ok_or_else(|| DriftError::TableNotFound(table_name.to_string()))?;

        let snapshot_mgr = self
            .snapshots
            .get(table_name)
            .ok_or_else(|| DriftError::TableNotFound(table_name.to_string()))?;

        let meta = storage.path().join("meta.json");
        let table_meta = crate::storage::TableMeta::load_from_file(meta)?;

        snapshot_mgr.create_snapshot(storage, table_meta.last_sequence)?;

        Ok(())
    }

    pub fn compact_table(&self, table_name: &str) -> Result<()> {
        let storage = self
            .tables
            .get(table_name)
            .ok_or_else(|| DriftError::TableNotFound(table_name.to_string()))?;

        let snapshot_mgr = self
            .snapshots
            .get(table_name)
            .ok_or_else(|| DriftError::TableNotFound(table_name.to_string()))?;

        let snapshots = snapshot_mgr.list_snapshots()?;
        if snapshots.is_empty() {
            return Err(DriftError::Other("No snapshots available for compaction".into()));
        }

        let latest_snapshot_seq = *snapshots.last()
            .ok_or_else(|| DriftError::Other("Snapshots list unexpectedly empty".into()))?;
        let latest_snapshot = snapshot_mgr
            .find_latest_before(u64::MAX)?
            .ok_or_else(|| DriftError::Other("Failed to load snapshot".into()))?;

        let segments_dir = storage.path().join("segments");
        let compacted_path = segments_dir.join("compacted.seg");
        let compacted_segment = Segment::new(compacted_path, 0);
        let mut writer = compacted_segment.create()?;

        for (pk, row_str) in latest_snapshot.state {
            // Parse the JSON string back to Value
            let row: serde_json::Value = match serde_json::from_str(&row_str) {
                Ok(val) => val,
                Err(e) => {
                    tracing::error!("Failed to parse row during compaction: {}, pk: {}", e, pk);
                    continue; // Skip corrupted rows instead of defaulting to null
                }
            };
            let event = Event::new_insert(
                table_name.to_string(),
                serde_json::Value::String(pk.clone()),
                row,
            );
            writer.append_event(&event)?;
        }

        writer.sync()?;

        let mut segment_files: Vec<_> = fs::read_dir(&segments_dir)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry.path().extension()
                    .and_then(|s| s.to_str())
                    .map(|s| s == "seg" && !entry.path().to_string_lossy().contains("compacted"))
                    .unwrap_or(false)
            })
            .collect();

        segment_files.sort_by_key(|entry| entry.path());

        for entry in segment_files {
            let segment = Segment::new(entry.path(), 0);
            let mut reader = segment.open_reader()?;
            let events = reader.read_all_events()?;

            let mut has_post_snapshot_events = false;
            for event in events {
                if event.sequence > latest_snapshot_seq {
                    has_post_snapshot_events = true;
                    writer.append_event(&event)?;
                }
            }

            if !has_post_snapshot_events {
                fs::remove_file(entry.path())?;
            }
        }

        writer.sync()?;

        let final_path = segments_dir.join("00000001.seg");
        fs::rename(segments_dir.join("compacted.seg"), final_path)?;

        Ok(())
    }

    pub fn doctor(&self) -> Result<Vec<String>> {
        let mut report = Vec::new();

        for (table_name, storage) in &self.tables {
            report.push(format!("Checking table: {}", table_name));

            let segments_dir = storage.path().join("segments");
            let mut segment_files: Vec<_> = fs::read_dir(&segments_dir)?
                .filter_map(|entry| entry.ok())
                .filter(|entry| {
                    entry.path().extension()
                        .and_then(|s| s.to_str())
                        .map(|s| s == "seg")
                        .unwrap_or(false)
                })
                .collect();

            segment_files.sort_by_key(|entry| entry.path());

            for entry in segment_files {
                let segment = Segment::new(entry.path(), 0);
                let mut reader = segment.open_reader()?;

                if let Some(corrupt_pos) = reader.verify_and_find_corruption()? {
                    report.push(format!(
                        "  Found corruption in {} at position {}, truncating...",
                        entry.path().display(),
                        corrupt_pos
                    ));
                    segment.truncate_at(corrupt_pos)?;
                } else {
                    report.push(format!("  Segment {} is healthy", entry.path().display()));
                }
            }
        }

        Ok(report)
    }

    // Transaction support methods
    pub fn begin_transaction(&self, isolation: IsolationLevel) -> Result<u64> {
        self.transaction_manager.write().simple_begin(isolation)
    }

    pub fn commit_transaction(&mut self, txn_id: u64) -> Result<()> {
        let events = {
            let mut txn_mgr = self.transaction_manager.write();
            txn_mgr.simple_commit(txn_id)?
        };

        // Apply all events from the committed transaction
        for event in events {
            self.apply_event(event)?;
        }

        Ok(())
    }

    pub fn rollback_transaction(&self, txn_id: u64) -> Result<()> {
        self.transaction_manager.write().rollback(txn_id)
    }

    pub fn apply_event_in_transaction(&self, txn_id: u64, event: Event) -> Result<()> {
        self.transaction_manager.write().add_write(txn_id, event)
    }

    pub fn read_in_transaction(&self, txn_id: u64, table: &str, key: &str) -> Result<Option<serde_json::Value>> {
        // First check transaction's write set
        let txn_mgr = self.transaction_manager.read();
        let active_txns = txn_mgr.active_transactions.read();

        if let Some(txn) = active_txns.get(&txn_id) {
            let txn_guard = txn.lock();

            // Check write set first (read-your-writes)
            if let Some(event) = txn_guard.write_set.get(key) {
                return Ok(Some(event.payload.clone()));
            }

            let _snapshot_version = txn_guard.snapshot_version;
            drop(txn_guard);
        } else {
            return Err(DriftError::Other(format!("Transaction {} not found", txn_id)));
        }

        // Read from storage at snapshot version
        let storage = self.tables.get(table)
            .ok_or_else(|| DriftError::TableNotFound(table.to_string()))?;

        // Get state at snapshot version (simplified - in production would use snapshot version)
        let state = storage.reconstruct_state_at(None)?;

        Ok(state.get(key).cloned())
    }

    pub fn query(&self, query: &Query) -> Result<QueryResult> {
        match query {
            Query::Select { table, conditions, as_of, .. } => {
                let storage = self.tables.get(table)
                    .ok_or_else(|| DriftError::TableNotFound(table.clone()))?;

                // Determine the target sequence number based on as_of clause
                let target_sequence = match as_of {
                    Some(crate::query::AsOf::Sequence(seq)) => Some(*seq),
                    Some(crate::query::AsOf::Timestamp(_)) => {
                        // Would need to map timestamp to sequence in production
                        None
                    }
                    Some(crate::query::AsOf::Now) | None => None,
                };

                // Reconstruct state at the target point in time
                let state = storage.reconstruct_state_at(target_sequence)?;

                // Apply WHERE conditions if any
                let mut results = Vec::new();
                for (_key, value) in state {
                    // Check if row matches conditions
                    let matches = if !conditions.is_empty() {
                        conditions.iter().all(|cond| {
                            // Simple equality check for now
                            if let Some(field_value) = value.get(&cond.column) {
                                field_value == &cond.value
                            } else {
                                false
                            }
                        })
                    } else {
                        true // No conditions means select all
                    };

                    if matches {
                        results.push(value);
                    }
                }

                Ok(QueryResult::Rows { data: results })
            }
            _ => Ok(QueryResult::Error { message: "Query type not supported in this method".to_string() })
        }
    }

    /// List all tables in the database
    pub fn list_tables(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    /// Get storage size information for a table
    pub fn get_table_size(&self, table_name: &str) -> Result<u64> {
        let storage = self.tables.get(table_name)
            .ok_or_else(|| DriftError::TableNotFound(table_name.to_string()))?;

        storage.calculate_size_bytes()
    }

    /// Get storage breakdown for a table by component
    pub fn get_table_storage_breakdown(&self, table_name: &str) -> Result<HashMap<String, u64>> {
        let storage = self.tables.get(table_name)
            .ok_or_else(|| DriftError::TableNotFound(table_name.to_string()))?;

        storage.get_storage_breakdown()
    }

    /// Get statistics for a table
    pub fn get_table_stats(&self, table_name: &str) -> Result<crate::storage::table_storage::TableStats> {
        let storage = self.tables.get(table_name)
            .ok_or_else(|| DriftError::TableNotFound(table_name.to_string()))?;

        Ok(storage.get_table_stats())
    }

    /// Get total database size across all tables
    pub fn get_total_database_size(&self) -> u64 {
        let mut total_size = 0u64;

        for (_, storage) in &self.tables {
            if let Ok(size) = storage.calculate_size_bytes() {
                total_size += size;
            }
        }

        total_size
    }

    /// Create a view
    pub fn create_view(&self, definition: ViewDefinition) -> Result<()> {
        self.view_manager.create_view(definition)
    }

    /// Create a view using builder pattern
    pub fn create_view_from_sql(&self, name: &str, sql: &str) -> Result<()> {
        let view = ViewBuilder::new(name, sql).build()?;
        self.view_manager.create_view(view)
    }

    /// Drop a view
    pub fn drop_view(&self, view_name: &str, cascade: bool) -> Result<()> {
        self.view_manager.drop_view(view_name, cascade)
    }

    /// Query a view
    pub fn query_view(
        &self,
        view_name: &str,
        conditions: Vec<crate::query::WhereCondition>,
        as_of: Option<crate::query::AsOf>,
        limit: Option<usize>,
    ) -> Result<Vec<serde_json::Value>> {
        self.view_manager.query_view(view_name, conditions, as_of, limit)
    }

    /// List all views
    pub fn list_views(&self) -> Vec<ViewDefinition> {
        self.view_manager.list_views()
    }

    /// Refresh a materialized view
    pub fn refresh_materialized_view(&self, view_name: &str) -> Result<()> {
        self.view_manager.refresh_materialized_view(view_name)
    }

    /// Get view statistics
    pub fn get_view_stats(&self) -> crate::views::ViewStatistics {
        self.view_manager.statistics()
    }

    /// Create a full-text search index
    pub fn create_search_index(
        &self,
        name: String,
        table: String,
        column: String,
        config: SearchConfig,
    ) -> Result<()> {
        self.search_manager.create_index(name, table, column, config)
    }

    /// Drop a search index
    pub fn drop_search_index(&self, name: &str) -> Result<()> {
        self.search_manager.drop_index(name)
    }

    /// Index a document for full-text search
    pub fn index_document(&self, index_name: &str, document_id: String, content: String) -> Result<()> {
        self.search_manager.index_document(index_name, document_id, content)
    }

    /// Perform a full-text search
    pub fn search(
        &self,
        index_name: &str,
        query: SearchQuery,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<SearchResults> {
        self.search_manager.search(index_name, query, limit, offset)
    }

    /// Get search statistics
    pub fn get_search_stats(&self) -> crate::fulltext::GlobalSearchStats {
        self.search_manager.statistics()
    }

    /// List all search indexes
    pub fn list_search_indexes(&self) -> Vec<String> {
        self.search_manager.list_indexes()
    }

    /// Create a trigger
    pub fn create_trigger(&self, definition: TriggerDefinition) -> Result<()> {
        self.trigger_manager.create_trigger(definition)
    }

    /// Drop a trigger
    pub fn drop_trigger(&self, trigger_name: &str) -> Result<()> {
        self.trigger_manager.drop_trigger(trigger_name)
    }

    /// Enable or disable a trigger
    pub fn set_trigger_enabled(&self, trigger_name: &str, enabled: bool) -> Result<()> {
        self.trigger_manager.set_trigger_enabled(trigger_name, enabled)
    }

    /// List all triggers
    pub fn list_triggers(&self) -> Vec<TriggerDefinition> {
        self.trigger_manager.list_triggers()
    }

    /// List triggers for a specific table
    pub fn list_table_triggers(&self, table_name: &str) -> Vec<TriggerDefinition> {
        self.trigger_manager.list_table_triggers(table_name)
    }

    /// Get trigger statistics
    pub fn get_trigger_stats(&self) -> crate::triggers::TriggerStatistics {
        self.trigger_manager.statistics()
    }

    /// Create a stored procedure
    pub fn create_procedure(&self, definition: ProcedureDefinition) -> Result<()> {
        self.procedure_manager.create_procedure(definition)
    }

    /// Drop a stored procedure
    pub fn drop_procedure(&self, name: &str) -> Result<()> {
        self.procedure_manager.drop_procedure(name)
    }

    /// Execute a stored procedure
    pub fn execute_procedure(
        &self,
        name: &str,
        arguments: std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<ProcedureResult> {
        self.procedure_manager.execute_procedure(name, arguments)
    }

    /// List all stored procedures
    pub fn list_procedures(&self) -> Vec<String> {
        self.procedure_manager.list_procedures()
    }

    /// Get procedure definition
    pub fn get_procedure(&self, name: &str) -> Option<ProcedureDefinition> {
        self.procedure_manager.get_procedure(name)
    }

    /// Get procedure statistics
    pub fn get_procedure_stats(&self) -> crate::procedures::GlobalProcedureStats {
        self.procedure_manager.statistics()
    }

    /// Collect statistics for a table
    pub fn collect_table_stats(&self, table_name: &str) -> Result<crate::optimizer::TableStatistics> {
        let storage = self.tables.get(table_name)
            .ok_or_else(|| DriftError::TableNotFound(table_name.to_string()))?;

        // Get current state of the table
        let state = storage.reconstruct_state_at(None)?;
        let data: Vec<serde_json::Value> = state.into_values().collect();

        let stats_manager = self.stats_manager.read();
        stats_manager.collect_table_statistics(table_name, &data)
    }

    /// Get database statistics
    pub fn get_database_statistics(&self) -> DatabaseStatistics {
        self.stats_manager.read().get_statistics()
    }

    /// Record query execution for statistics
    pub fn record_query_execution(&self, execution: QueryExecution) {
        self.stats_manager.read().record_query_execution(execution);
    }

    /// Update statistics configuration
    pub fn update_stats_config(&self, config: StatsConfig) {
        self.stats_manager.write().update_config(config);
    }

    /// Check if automatic statistics collection is due
    pub fn should_collect_stats(&self) -> bool {
        self.stats_manager.read().should_collect_stats()
    }

    /// Perform automatic statistics collection
    pub fn auto_collect_statistics(&self) -> Result<()> {
        if !self.should_collect_stats() {
            return Ok(());
        }

        debug!("Performing automatic statistics collection");

        // Collect stats for all tables
        for table_name in self.list_tables() {
            if let Err(e) = self.collect_table_stats(&table_name) {
                warn!("Failed to collect stats for table '{}': {}", table_name, e);
            }
        }

        // Mark collection as complete
        self.stats_manager.read().mark_collection_complete();

        info!("Automatic statistics collection completed");
        Ok(())
    }

    /// Collect real statistics for a table
    pub fn collect_table_statistics(&self, table_name: &str) -> Result<crate::optimizer::TableStatistics> {
        use crate::optimizer::{TableStatistics, ColumnStatistics, IndexStatistics, Histogram, HistogramBucket};

        let storage = self.tables.get(table_name)
            .ok_or_else(|| DriftError::TableNotFound(table_name.to_string()))?;

        // Get current state to analyze
        let current_state = storage.reconstruct_state_at(None)?;
        let row_count = current_state.len();

        // Calculate average row size
        let total_size: usize = current_state.values()
            .map(|v| v.to_string().len())
            .sum();
        let avg_row_size = if row_count > 0 { total_size / row_count } else { 0 };

        // Get actual storage size
        let total_size_bytes = storage.calculate_size_bytes()?;

        // Collect column statistics
        let mut column_stats = HashMap::new();
        let schema = storage.schema();

        for column in &schema.columns {
            let mut values = Vec::new();
            let mut null_count = 0;

            // Collect all values for this column
            for row in current_state.values() {
                if let Some(value) = row.get(&column.name) {
                    if value.is_null() {
                        null_count += 1;
                    } else {
                        values.push(value.clone());
                    }
                } else {
                    null_count += 1;
                }
            }

            // Calculate statistics
            let distinct_values: HashSet<_> = values.iter().collect();
            let distinct_count = distinct_values.len();

            // Find min/max values
            let (min_value, max_value) = if !values.is_empty() {
                let sorted: Vec<String> = values.iter()
                    .filter_map(|v| {
                        v.as_str()
                            .map(String::from)
                            .or_else(|| v.as_i64().map(|n| n.to_string()))
                            .or_else(|| v.as_f64().map(|n| n.to_string()))
                    })
                    .collect();

                if !sorted.is_empty() {
                    let min = sorted.iter().min().map(|s| serde_json::json!(s));
                    let max = sorted.iter().max().map(|s| serde_json::json!(s));
                    (min, max)
                } else {
                    (None, None)
                }
            } else {
                (None, None)
            };

            // Create histogram with up to 10 buckets
            let histogram = if values.len() > 10 {
                let bucket_size = values.len() / 10;
                let mut buckets = Vec::new();

                for i in 0..10 {
                    let start = i * bucket_size;
                    let end = ((i + 1) * bucket_size).min(values.len());
                    if start < values.len() {
                        let bucket_values = &values[start..end];
                        if !bucket_values.is_empty() {
                            buckets.push(HistogramBucket {
                                lower_bound: bucket_values.first().unwrap().clone(),
                                upper_bound: bucket_values.last().unwrap().clone(),
                                frequency: bucket_values.len(),
                            });
                        }
                    }
                }

                Some(Histogram { buckets })
            } else {
                None
            };

            column_stats.insert(column.name.clone(), ColumnStatistics {
                distinct_values: distinct_count,
                null_count,
                min_value,
                max_value,
                histogram,
            });
        }

        // Collect index statistics
        let mut index_stats = HashMap::new();
        if let Some(index_mgr) = self.indexes.get(table_name) {
            let index_mgr = index_mgr.read();
            for index_name in schema.indexed_columns() {
                // Get index metadata
                if let Some(index) = index_mgr.get_index(&index_name) {
                    // Count unique keys in the index
                    let unique_keys = index.len();

                    index_stats.insert(index_name.clone(), IndexStatistics {
                        index_name: index_name.clone(),
                        unique_keys,
                        depth: 3, // B-tree typical depth
                        size_bytes: unique_keys as u64 * 64, // Estimate
                    });
                }
            }
        }

        Ok(TableStatistics {
            table_name: table_name.to_string(),
            row_count,
            avg_row_size,
            total_size_bytes,
            column_stats,
            index_stats,
            last_updated: chrono::Utc::now().timestamp() as u64,
        })
    }

    /// Analyze all tables and update optimizer statistics
    pub fn analyze_all_tables(&self, optimizer: &crate::optimizer::QueryOptimizer) -> Result<()> {
        for table_name in self.list_tables() {
            let stats = self.collect_table_statistics(&table_name)?;
            optimizer.update_statistics(&table_name, stats);
        }
        Ok(())
    }

    /// Get all data from a table (for SQL SELECT support)
    pub fn get_table_data(&self, table_name: &str) -> Result<Vec<serde_json::Value>> {
        let storage = self.tables.get(table_name)
            .ok_or_else(|| DriftError::TableNotFound(table_name.to_string()))?;

        // Get the current state of the table (None means latest)
        let state = storage.reconstruct_state_at(None)?;

        // Convert to Vec of JSON values
        let mut results = Vec::new();
        for (_key, value) in state {
            results.push(value);
        }

        Ok(results)
    }

    /// Get indexed columns for a table
    pub fn get_indexed_columns(&self, table_name: &str) -> HashSet<String> {
        if let Some(storage) = self.tables.get(table_name) {
            storage.schema().indexed_columns()
        } else {
            HashSet::new()
        }
    }

    /// Look up rows using an index
    pub fn lookup_by_index(&self, table_name: &str, column: &str, value: &serde_json::Value) -> Result<Vec<String>> {
        let index_mgr = self.indexes.get(table_name)
            .ok_or_else(|| DriftError::Other(format!("No indexes for table: {}", table_name)))?;

        let mgr_guard = index_mgr.read();

        // Convert value to string for index lookup
        let value_str = if let Some(s) = value.as_str() {
            s.to_string()
        } else {
            value.to_string()
        };

        if let Some(index) = mgr_guard.get_index(column) {
            if let Some(keys) = index.find(&value_str) {
                Ok(keys.iter().cloned().collect())
            } else {
                Ok(Vec::new())
            }
        } else {
            Err(DriftError::Other(format!("No index on column: {}", column)))
        }
    }

    /// Get a single row by primary key
    pub fn get_row(&self, table_name: &str, primary_key: &str) -> Result<Option<serde_json::Value>> {
        let storage = self.tables.get(table_name)
            .ok_or_else(|| DriftError::TableNotFound(table_name.to_string()))?;

        let state = storage.reconstruct_state_at(None)?;
        Ok(state.get(primary_key).cloned())
    }

    /// Insert a record into a table (for SQL INSERT support)
    pub fn insert_record(&mut self, table_name: &str, mut record: serde_json::Value) -> Result<()> {
        let storage = self.tables.get(table_name)
            .ok_or_else(|| DriftError::TableNotFound(table_name.to_string()))?
            .clone();

        // Get the schema for validation
        let schema = storage.schema();

        // Validate constraints and apply defaults
        {
            let mut constraint_mgr = self.constraint_manager.write();
            constraint_mgr.validate_insert(schema, &mut record, self)
                .map_err(|e| DriftError::Other(format!("Constraint violation: {}", e)))?;
        }

        // Check for auto-increment columns and generate values
        for column in &schema.columns {
            if let Some(obj) = record.as_object_mut() {
                // If column is missing or null, check if it has auto-increment
                if obj.get(&column.name).map_or(true, |v| v.is_null()) {
                    // Try to get auto-increment value
                    if let Ok(next_val) = self.sequence_manager.next_auto_increment(table_name, &column.name) {
                        obj.insert(column.name.clone(), serde_json::json!(next_val));
                    }
                }
            }
        }

        // Extract primary key from record
        let primary_key_field = &storage.schema().primary_key;
        let primary_key = record.get(primary_key_field)
            .ok_or_else(|| DriftError::Other(format!("Missing primary key field: {}", primary_key_field)))?
            .clone();

        let event = Event::new_insert(table_name.to_string(), primary_key, record);
        self.apply_event(event)?;

        Ok(())
    }

    /// Update a record in a table (for SQL UPDATE support)
    pub fn update_record(&mut self, table_name: &str, primary_key: serde_json::Value, record: serde_json::Value) -> Result<()> {
        let storage = self.tables.get(table_name)
            .ok_or_else(|| DriftError::TableNotFound(table_name.to_string()))?
            .clone();

        // Use PATCH event for updates
        let event = Event::new_patch(table_name.to_string(), primary_key, record);
        self.apply_event(event)?;

        Ok(())
    }

    /// Delete a record from a table (for SQL DELETE support)
    pub fn delete_record(&mut self, table_name: &str, primary_key: serde_json::Value) -> Result<()> {
        let storage = self.tables.get(table_name)
            .ok_or_else(|| DriftError::TableNotFound(table_name.to_string()))?
            .clone();

        // Use SOFT_DELETE event for deletes (preserves audit trail)
        let event = Event::new_soft_delete(table_name.to_string(), primary_key);
        self.apply_event(event)?;

        Ok(())
    }

    // Migration support methods

    /// Apply a schema migration to add a column with optional default value
    pub fn migrate_add_column(
        &mut self,
        table: &str,
        column: &crate::schema::ColumnDef,
        default_value: Option<serde_json::Value>,
    ) -> Result<()> {
        // Get the table storage
        let storage = self.tables.get(table)
            .ok_or_else(|| DriftError::TableNotFound(table.to_string()))?
            .clone();

        // Update the schema file
        let table_path = self.base_path.join("tables").join(table);
        let schema_path = table_path.join("schema.yaml");

        let mut schema = crate::schema::Schema::load_from_file(&schema_path)?;

        // Check if column already exists
        if schema.columns.iter().any(|c| c.name == column.name) {
            return Err(DriftError::Other(format!("Column {} already exists", column.name)));
        }

        // Add the new column to schema
        schema.columns.push(column.clone());

        // Save updated schema
        let updated_schema = serde_yaml::to_string(&schema)?;
        fs::write(&schema_path, updated_schema)?;

        // If there's a default value, backfill existing records
        if let Some(default) = default_value {
            // Get current state to find all records
            let current_state = storage.reconstruct_state_at(None)?;

            // Create patch events for each existing record
            for (key, _value) in current_state {
                // The key is the stringified version of the primary key value (e.g., "\"user1\"")
                // We need to parse it back to the original JSON value
                let primary_key: serde_json::Value = serde_json::from_str(&key)
                    .unwrap_or_else(|_| serde_json::Value::String(key.clone()));

                let patch_event = crate::events::Event::new_patch(
                    table.to_string(),
                    primary_key,
                    serde_json::json!({
                        &column.name: default.clone()
                    })
                );

                // Use the engine's apply_event method to ensure proper handling
                self.apply_event(patch_event)?;
            }
        }

        Ok(())
    }

    /// Apply a schema migration to drop a column
    pub fn migrate_drop_column(&mut self, table: &str, column: &str) -> Result<()> {
        // Update the schema file
        let table_path = self.base_path.join("tables").join(table);
        let schema_path = table_path.join("schema.yaml");

        let mut schema = crate::schema::Schema::load_from_file(&schema_path)?;

        // Remove the column from schema
        schema.columns.retain(|c| c.name != column);

        // Save updated schema
        let updated_schema = serde_yaml::to_string(&schema)?;
        fs::write(&schema_path, updated_schema)?;

        // Note: We don't remove the data from existing events (append-only)
        // The column will just be ignored in future queries

        Ok(())
    }

    /// Apply a schema migration to rename a column
    pub fn migrate_rename_column(&mut self, table: &str, old_name: &str, new_name: &str) -> Result<()> {
        let storage = self.tables.get(table)
            .ok_or_else(|| DriftError::TableNotFound(table.to_string()))?
            .clone();

        // Update the schema file
        let table_path = self.base_path.join("tables").join(table);
        let schema_path = table_path.join("schema.yaml");

        let mut schema = crate::schema::Schema::load_from_file(&schema_path)?;

        // Find and rename the column
        let mut found = false;
        for column in &mut schema.columns {
            if column.name == old_name {
                column.name = new_name.to_string();
                found = true;
                break;
            }
        }

        if !found {
            return Err(DriftError::Other(format!("Column {} not found", old_name)));
        }

        // Save updated schema
        let updated_schema = serde_yaml::to_string(&schema)?;
        fs::write(&schema_path, updated_schema)?;

        // Create patch events to rename the field in existing records
        let current_state = storage.reconstruct_state_at(None)?;

        for (key, value) in current_state {
            if let Some(old_value) = value.get(old_name) {
                // Parse the stringified primary key back to JSON value
                let primary_key: serde_json::Value = serde_json::from_str(&key)
                    .unwrap_or_else(|_| serde_json::Value::String(key.clone()));

                // Create a patch that adds the new field and removes the old
                let mut patch = serde_json::Map::new();
                patch.insert(new_name.to_string(), old_value.clone());

                let patch_event = crate::events::Event::new_patch(
                    table.to_string(),
                    primary_key,
                    serde_json::Value::Object(patch)
                );

                self.apply_event(patch_event)?;
            }
        }

        Ok(())
    }

    /// Get table data at a specific sequence number (time travel)
    pub fn get_table_data_at(&self, table_name: &str, sequence: u64) -> Result<Vec<serde_json::Value>> {
        let storage = self.tables.get(table_name)
            .ok_or_else(|| DriftError::TableNotFound(table_name.to_string()))?;

        // Get the state at the specified sequence
        let state = storage.reconstruct_state_at(Some(sequence))?;

        // Convert to Vec of JSON values
        let mut results = Vec::new();
        for (_key, value) in state {
            results.push(value);
        }

        Ok(results)
    }

    /// Get the current global sequence number
    pub fn get_current_sequence(&self) -> u64 {
        // Get the maximum sequence number across all tables
        let mut max_seq = 0u64;
        for storage in self.tables.values() {
            if let Ok(events) = storage.read_all_events() {
                for event in events {
                    max_seq = max_seq.max(event.sequence);
                }
            }
        }
        max_seq
    }

    /// Find the sequence number for a given timestamp
    pub fn find_sequence_for_timestamp(&self, timestamp: time::OffsetDateTime) -> Result<u64> {
        let mut closest_sequence = 0u64;
        let mut closest_time_diff = i128::MAX;

        // Search all tables for the closest event before or at the timestamp
        for storage in self.tables.values() {
            if let Ok(events) = storage.read_all_events() {
                for event in events {
                    // Check if this event is before or at the requested timestamp
                    if event.timestamp <= timestamp {
                        let time_diff = (timestamp - event.timestamp).whole_nanoseconds().abs();
                        if time_diff < closest_time_diff {
                            closest_time_diff = time_diff;
                            closest_sequence = event.sequence;
                        }
                    }
                }
            }
        }

        if closest_sequence == 0 {
            // No events found before this timestamp
            return Err(DriftError::InvalidQuery(
                format!("No data found before timestamp {}", timestamp)
            ));
        }

        Ok(closest_sequence)
    }

    /// Get table data at a specific timestamp (time travel)
    pub fn get_table_data_at_timestamp(&self, table_name: &str, timestamp: time::OffsetDateTime) -> Result<Vec<serde_json::Value>> {
        // Find the sequence number for this timestamp
        let sequence = self.find_sequence_for_timestamp(timestamp)?;

        // Use the existing get_table_data_at method
        self.get_table_data_at(table_name, sequence)
    }

    /// Begin a migration transaction
    pub fn begin_migration_transaction(&mut self) -> Result<u64> {
        self.begin_transaction(IsolationLevel::Serializable)
    }

    /// Commit a migration transaction
    pub fn commit_migration_transaction(&mut self, txn_id: u64) -> Result<()> {
        self.commit_transaction(txn_id)
    }

    /// Rollback a migration transaction
    pub fn rollback_migration_transaction(&mut self, txn_id: u64) -> Result<()> {
        self.rollback_transaction(txn_id)
    }
}