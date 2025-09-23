use crate::storage::*;
use crate::events::{Event, EventType};
use crate::schema::{Schema, ColumnDef};
use tempfile::TempDir;
use serde_json::json;
use std::time::SystemTime;

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_schema() -> Schema {
        Schema {
            name: "test_table".to_string(),
            primary_key: "id".to_string(),
            columns: vec![
                ColumnDef { name: "id".to_string(), col_type: "integer".to_string(), index: false },
                ColumnDef { name: "name".to_string(), col_type: "string".to_string(), index: false },
                ColumnDef { name: "value".to_string(), col_type: "float".to_string(), index: false },
            ],
        }
    }

    #[test]
    fn test_segment_creation() {
        let temp_dir = TempDir::new().unwrap();
        let segment_path = temp_dir.path().join("test.seg");

        let segment = Segment::create(&segment_path).unwrap();
        assert!(segment_path.exists());
        assert_eq!(segment.entry_count(), 0);
    }

    #[test]
    fn test_segment_append() {
        let temp_dir = TempDir::new().unwrap();
        let segment_path = temp_dir.path().join("test.seg");

        let mut segment = Segment::create(&segment_path).unwrap();

        let event = Event {
            sequence: 1,
            timestamp: SystemTime::now(),
            event_type: EventType::Insert,
            key: "key1".to_string(),
            payload: json!({"id": 1, "name": "Test", "value": 42.0}),
        };

        segment.append(&event).unwrap();
        assert_eq!(segment.entry_count(), 1);

        // Append more events
        for i in 2..=10 {
            let event = Event {
                sequence: i,
                timestamp: SystemTime::now(),
                event_type: EventType::Insert,
                key: format!("key{}", i),
                payload: json!({"id": i, "name": format!("Test{}", i), "value": i as f64}),
            };
            segment.append(&event).unwrap();
        }

        assert_eq!(segment.entry_count(), 10);
    }

    #[test]
    fn test_segment_read() {
        let temp_dir = TempDir::new().unwrap();
        let segment_path = temp_dir.path().join("test.seg");

        let mut segment = Segment::create(&segment_path).unwrap();

        // Write events
        let mut original_events = Vec::new();
        for i in 1..=5 {
            let event = Event {
                sequence: i,
                timestamp: SystemTime::now(),
                event_type: EventType::Insert,
                key: format!("key{}", i),
                payload: json!({"id": i, "name": format!("Test{}", i)}),
            };
            segment.append(&event).unwrap();
            original_events.push(event);
        }

        // Read back
        drop(segment);
        let segment = Segment::open(&segment_path).unwrap();
        let events = segment.read_all().unwrap();

        assert_eq!(events.len(), 5);
        for (i, event) in events.iter().enumerate() {
            assert_eq!(event.sequence, original_events[i].sequence);
            assert_eq!(event.key, original_events[i].key);
            assert_eq!(event.payload, original_events[i].payload);
        }
    }

    #[test]
    fn test_segment_crc_validation() {
        let temp_dir = TempDir::new().unwrap();
        let segment_path = temp_dir.path().join("test.seg");

        let mut segment = Segment::create(&segment_path).unwrap();

        let event = Event {
            sequence: 1,
            timestamp: SystemTime::now(),
            event_type: EventType::Insert,
            key: "key1".to_string(),
            payload: json!({"data": "test"}),
        };

        segment.append(&event).unwrap();
        drop(segment);

        // Corrupt the file
        let mut data = std::fs::read(&segment_path).unwrap();
        if data.len() > 20 {
            data[20] ^= 0xFF; // Flip some bits
        }
        std::fs::write(&segment_path, data).unwrap();

        // Try to read - should detect corruption
        let result = Segment::open(&segment_path);
        assert!(result.is_err() || {
            let segment = result.unwrap();
            segment.read_all().is_err()
        });
    }

    #[test]
    fn test_table_storage_create() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let storage = TableStorage::create(temp_dir.path(), "test_table", schema.clone()).unwrap();
        assert_eq!(storage.table_name(), "test_table");
        assert_eq!(storage.schema().columns.len(), schema.columns.len());
    }

    #[test]
    fn test_table_storage_append_and_query() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut storage = TableStorage::create(temp_dir.path(), "test_table", schema).unwrap();

        // Append events
        for i in 1..=100 {
            let event = Event {
                sequence: i,
                timestamp: SystemTime::now(),
                event_type: EventType::Insert,
                key: format!("key{}", i),
                payload: json!({
                    "id": i,
                    "name": format!("Item{}", i),
                    "value": i as f64 * 1.5
                }),
            };
            storage.append(event).unwrap();
        }

        // Query all
        let results = storage.query(None, None).unwrap();
        assert_eq!(results.len(), 100);

        // Query with sequence range
        let results = storage.query(Some(10), Some(20)).unwrap();
        assert_eq!(results.len(), 11); // sequences 10-20 inclusive

        // Query from sequence
        let results = storage.query(Some(90), None).unwrap();
        assert_eq!(results.len(), 11); // sequences 90-100
    }

    #[test]
    fn test_table_storage_segments() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut storage = TableStorage::create(temp_dir.path(), "test_table", schema).unwrap();

        // Set small segment size to force multiple segments
        storage.set_max_segment_size(1024); // 1KB

        // Append many events to create multiple segments
        for i in 1..=1000 {
            let event = Event {
                sequence: i,
                timestamp: SystemTime::now(),
                event_type: EventType::Insert,
                key: format!("key{}", i),
                payload: json!({
                    "id": i,
                    "name": format!("Item{}", i),
                    "value": i as f64,
                    "description": format!("This is a longer description for item {} to increase size", i)
                }),
            };
            storage.append(event).unwrap();
        }

        // Should have multiple segments
        let segment_count = storage.segment_count();
        assert!(segment_count > 1, "Expected multiple segments, got {}", segment_count);

        // Query across segments
        let results = storage.query(None, None).unwrap();
        assert_eq!(results.len(), 1000);
    }

    #[test]
    fn test_table_storage_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path();
        let schema = create_test_schema();

        // Create and populate
        {
            let mut storage = TableStorage::create(table_path, "test_table", schema.clone()).unwrap();

            for i in 1..=50 {
                let event = Event {
                    sequence: i,
                    timestamp: SystemTime::now(),
                    event_type: EventType::Insert,
                    key: format!("key{}", i),
                    payload: json!({"id": i, "name": format!("Item{}", i), "value": i as f64}),
                };
                storage.append(event).unwrap();
            }
        }

        // Reopen and verify
        {
            let storage = TableStorage::open(table_path, "test_table").unwrap();
            assert_eq!(storage.schema().columns.len(), schema.columns.len());

            let results = storage.query(None, None).unwrap();
            assert_eq!(results.len(), 50);

            // Verify data integrity
            for (i, event) in results.iter().enumerate() {
                let expected_id = i + 1;
                assert_eq!(event.sequence, expected_id as u64);
                assert_eq!(event.payload["id"], json!(expected_id));
            }
        }
    }

    #[test]
    fn test_table_storage_patch_events() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut storage = TableStorage::create(temp_dir.path(), "test_table", schema).unwrap();

        // Insert
        let insert_event = Event {
            sequence: 1,
            timestamp: SystemTime::now(),
            event_type: EventType::Insert,
            key: "key1".to_string(),
            payload: json!({"id": 1, "name": "Original", "value": 100.0}),
        };
        storage.append(insert_event).unwrap();

        // Patch
        let patch_event = Event {
            sequence: 2,
            timestamp: SystemTime::now(),
            event_type: EventType::Patch,
            key: "key1".to_string(),
            payload: json!({"name": "Updated", "value": 200.0}),
        };
        storage.append(patch_event).unwrap();

        let events = storage.query(None, None).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].event_type, EventType::Patch);
    }

    #[test]
    fn test_table_storage_soft_delete() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut storage = TableStorage::create(temp_dir.path(), "test_table", schema).unwrap();

        // Insert
        let insert_event = Event {
            sequence: 1,
            timestamp: SystemTime::now(),
            event_type: EventType::Insert,
            key: "key1".to_string(),
            payload: json!({"id": 1, "name": "ToDelete", "value": 42.0}),
        };
        storage.append(insert_event).unwrap();

        // Soft delete
        let delete_event = Event {
            sequence: 2,
            timestamp: SystemTime::now(),
            event_type: EventType::SoftDelete,
            key: "key1".to_string(),
            payload: json!({}),
        };
        storage.append(delete_event).unwrap();

        let events = storage.query(None, None).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].event_type, EventType::SoftDelete);
    }

    #[test]
    fn test_metadata_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let meta_path = temp_dir.path().join("meta.json");

        let schema = create_test_schema();
        let metadata = TableMetadata {
            schema: schema.clone(),
            created_at: SystemTime::now(),
            last_sequence: 42,
            segment_count: 3,
            total_events: 1000,
            table_version: 1,
        };

        // Write metadata
        metadata.save(&meta_path).unwrap();
        assert!(meta_path.exists());

        // Read back
        let loaded = TableMetadata::load(&meta_path).unwrap();
        assert_eq!(loaded.last_sequence, 42);
        assert_eq!(loaded.segment_count, 3);
        assert_eq!(loaded.total_events, 1000);
        assert_eq!(loaded.table_version, 1);
        assert_eq!(loaded.schema.columns.len(), schema.columns.len());
    }

    #[test]
    fn test_compaction() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut storage = TableStorage::create(temp_dir.path(), "test_table", schema).unwrap();

        // Insert, update, and delete to create a complex history
        for i in 1..=20 {
            // Insert
            storage.append(Event {
                sequence: i * 3 - 2,
                timestamp: SystemTime::now(),
                event_type: EventType::Insert,
                key: format!("key{}", i),
                payload: json!({"id": i, "name": format!("Item{}", i), "value": i as f64}),
            }).unwrap();

            // Update
            storage.append(Event {
                sequence: i * 3 - 1,
                timestamp: SystemTime::now(),
                event_type: EventType::Patch,
                key: format!("key{}", i),
                payload: json!({"value": i as f64 * 2.0}),
            }).unwrap();

            // Delete some
            if i % 3 == 0 {
                storage.append(Event {
                    sequence: i * 3,
                    timestamp: SystemTime::now(),
                    event_type: EventType::SoftDelete,
                    key: format!("key{}", i),
                    payload: json!({}),
                }).unwrap();
            }
        }

        let before_compact = storage.query(None, None).unwrap();
        let before_size = storage.total_size().unwrap();

        // Compact
        storage.compact().unwrap();

        let after_compact = storage.query(None, None).unwrap();
        let after_size = storage.total_size().unwrap();

        // Should have fewer events after compaction (consolidated updates)
        assert!(after_compact.len() <= before_compact.len());

        // Size should be reduced or similar
        assert!(after_size <= before_size * 110 / 100); // Allow 10% variance
    }

    #[test]
    fn test_concurrent_reads() {
        use std::sync::Arc;
        use std::thread;

        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut storage = TableStorage::create(temp_dir.path(), "test_table", schema).unwrap();

        // Populate with data
        for i in 1..=100 {
            storage.append(Event {
                sequence: i,
                timestamp: SystemTime::now(),
                event_type: EventType::Insert,
                key: format!("key{}", i),
                payload: json!({"id": i, "name": format!("Item{}", i), "value": i as f64}),
            }).unwrap();
        }

        let storage = Arc::new(storage);

        // Spawn multiple reader threads
        let mut handles = Vec::new();
        for thread_id in 0..10 {
            let storage_clone = storage.clone();
            let handle = thread::spawn(move || {
                for _ in 0..10 {
                    let results = storage_clone.query(
                        Some(thread_id * 10 + 1),
                        Some(thread_id * 10 + 10)
                    ).unwrap();
                    assert!(!results.is_empty());
                }
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_segment_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let segment_path = temp_dir.path().join("test.seg");

        // Create segment with valid data
        {
            let mut segment = Segment::create(&segment_path).unwrap();
            for i in 1..=10 {
                segment.append(&Event {
                    sequence: i,
                    timestamp: SystemTime::now(),
                    event_type: EventType::Insert,
                    key: format!("key{}", i),
                    payload: json!({"id": i}),
                }).unwrap();
            }
        }

        // Append garbage to simulate incomplete write
        {
            use std::fs::OpenOptions;
            use std::io::Write;

            let mut file = OpenOptions::new()
                .append(true)
                .open(&segment_path)
                .unwrap();
            file.write_all(b"garbage data that's not a valid frame").unwrap();
        }

        // Try to recover
        let segment = Segment::open_with_recovery(&segment_path).unwrap();
        let events = segment.read_all().unwrap();

        // Should recover the 10 valid events
        assert_eq!(events.len(), 10);
        for (i, event) in events.iter().enumerate() {
            assert_eq!(event.sequence, (i + 1) as u64);
        }
    }
}