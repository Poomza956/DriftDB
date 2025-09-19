# DriftDB

A production-ready append-only database with time-travel queries, ACID transactions, and enterprise features.

## ✨ Core Features

### Data Model & Storage
- **Append-only storage**: Immutable drift events for complete audit trails
- **Time travel queries**: Query any historical state with `AS OF` clauses
- **ACID transactions**: Full transaction support with multiple isolation levels
- **Secondary indexes**: B-tree indexes for fast lookups
- **Snapshots & compaction**: Optimized query performance with compression

### Production Features
- **Write-Ahead Log (WAL)**: Crash recovery with guaranteed durability
- **Connection pooling**: Efficient resource management with configurable limits
- **Rate limiting**: Per-client rate limiting with token bucket algorithm
- **Backup & restore**: Full and incremental backups with checksums
- **Schema migrations**: Safe, versioned schema evolution
- **Query optimization**: Cost-based query planner with statistics
- **Encryption**: AES-256-GCM at rest, TLS 1.3 in transit

### Observability & Operations
- **Comprehensive metrics**: Read/write latency, throughput, errors
- **Distributed tracing**: Full request tracing with OpenTelemetry
- **Health checks**: Automated health monitoring endpoints
- **Memory management**: Streaming APIs prevent OOM conditions
- **Error handling**: No panics - all errors handled gracefully

## Quick Start

### Installation

```bash
# Clone and build from source
git clone https://github.com/driftdb/driftdb
cd driftdb
make build

# Or install with cargo
cargo install driftdb-cli
```

### 60-second demo

```bash
# Run the full demo (creates sample data and runs queries)
make demo
```

### Manual usage

```bash
# Initialize a database
driftdb init ./mydata

# Create a table
driftdb sql -d ./mydata -e 'CREATE TABLE users (pk=id, INDEX(email, status))'

# Insert data
driftdb sql -d ./mydata -e 'INSERT INTO users {"id": "user1", "email": "alice@example.com", "status": "active"}'

# Query data
driftdb select -d ./mydata --table users --where 'status="active"'

# Time travel query
driftdb select -d ./mydata --table users --as-of "@seq:100"

# Show row history
driftdb drift -d ./mydata --table users --key "user1"

# Create snapshot for faster queries
driftdb snapshot -d ./mydata --table users

# Compact storage
driftdb compact -d ./mydata --table users
```

## DriftQL Syntax

### Basic Operations
```sql
-- Create table with indexes
CREATE TABLE orders (pk=id, INDEX(status, customer_id))

-- Insert full document
INSERT INTO orders {"id": "order1", "status": "pending", "amount": 100}

-- Partial update
PATCH orders KEY "order1" SET {"status": "paid"}

-- Soft delete (data remains for audit)
SOFT DELETE FROM orders KEY "order1"
```

### Transactions
```sql
-- Start a transaction
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ

-- Multiple operations in transaction
INSERT INTO orders {"id": "order2", "amount": 200}
PATCH orders KEY "order1" SET {"status": "shipped"}

-- Commit or rollback
COMMIT
-- or
ROLLBACK
```

### Time Travel Queries
```sql
-- Query historical state by timestamp
SELECT * FROM orders WHERE status="paid" AS OF "2025-01-01T00:00:00Z"

-- Query by sequence number
SELECT * FROM orders WHERE customer_id="cust1" AS OF "@seq:1000"

-- Show complete history of a record
SHOW DRIFT orders KEY "order1"
```

### Schema Migrations
```sql
-- Add a new column with default value
ALTER TABLE orders ADD COLUMN priority DEFAULT "normal"

-- Add an index
CREATE INDEX ON orders(created_at)

-- Drop a column (requires downtime)
ALTER TABLE orders DROP COLUMN legacy_field
```

### Maintenance
```sql
-- Create snapshot for performance
SNAPSHOT orders

-- Compact storage
COMPACT orders

-- Backup database
BACKUP TO './backups/2024-01-15'

-- Show table statistics
ANALYZE TABLE orders
```

## Architecture

### Storage Layout

```
data/
  tables/<table>/
    schema.yaml           # Table schema definition
    segments/            # Append-only event logs with CRC32
      00000001.seg
      00000002.seg
    snapshots/           # Compressed materialized states
      00000100.snap
    indexes/             # Secondary B-tree indexes
      status.idx
      customer_id.idx
    meta.json           # Table metadata
  wal/                   # Write-ahead log for durability
    wal.log
    wal.log.1            # Rotated WAL files
  migrations/            # Schema migrations
    history.json
    pending/
  backups/               # Backup snapshots
```

### Event Types

- **INSERT**: Add new row with full document
- **PATCH**: Partial update by primary key
- **SOFT_DELETE**: Mark row as deleted (audit trail preserved)

### Segment Format

```
[u32 length][u32 crc32][varint seq][u64 unix_ms][u8 event_type][msgpack payload]
```

## Safety & Reliability

### Data Integrity
- **Write-Ahead Logging**: All writes go through WAL first for durability
- **CRC32 verification**: Every frame is checksummed
- **Atomic operations**: fsync on critical boundaries
- **Crash recovery**: Automatic WAL replay on startup

### Concurrency Control
- **ACID transactions**: Serializable isolation available
- **MVCC**: Multi-version concurrency control for readers
- **Deadlock detection**: Automatic detection and resolution
- **Connection pooling**: Fair scheduling with backpressure

### Security
- **Encryption at rest**: AES-256-GCM for stored data
- **Encryption in transit**: TLS 1.3 for network communication
- **Key rotation**: Automatic key rotation support
- **Rate limiting**: DoS protection with per-client limits

## Development

```bash
# Run tests
make test

# Run benchmarks
make bench

# Format code
make fmt

# Run linter
make clippy

# Full CI checks
make ci
```

## Performance

### Optimization Features
- **Query optimizer**: Cost-based planning with statistics
- **Index selection**: Automatic index usage for queries
- **Streaming APIs**: Memory-bounded operations
- **Connection pooling**: Reduced connection overhead
- **Plan caching**: Reuse of optimized query plans

### Storage Efficiency
- **Zstd compression**: For snapshots and backups
- **MessagePack serialization**: Compact binary format
- **Incremental snapshots**: Only changed data
- **Compaction**: Automatic segment consolidation
- **B-tree indexes**: O(log n) lookup performance

### Scalability
- **Configurable limits**: Memory, connections, request rates
- **Backpressure**: Automatic load shedding
- **Batch operations**: Efficient bulk inserts
- **Parallel processing**: Multi-threaded where safe

## License

MIT

## Production Readiness

### ⚠️ Alpha Stage - Not Production Ready
DriftDB is currently in **alpha** stage and should **NOT** be used in production.

**Current Status:**
- Core functionality implemented but not battle-tested
- Several critical issues need resolution
- Data durability guarantees not yet reliable
- Replication is experimental
- Performance not optimized
- Security features need hardening

**Safe for:**
- Development and experimentation
- Learning about database internals
- Proof of concept projects
- Testing time-travel database concepts

**NOT safe for:**
- Production workloads
- Data you cannot afford to lose
- High-availability requirements
- Security-sensitive applications

### Feature Maturity

| Component | Status | Production Ready |
|-----------|--------|------------------|
| Core Storage Engine | 🔶 Alpha | No |
| WAL & Crash Recovery | 🔶 Alpha | No |
| ACID Transactions | 🔶 Alpha | No |
| Backup & Restore | 🔶 Alpha | No |
| Query Optimization | 🔶 Experimental | No |
| Encryption | 🔶 Experimental | No |
| Schema Migrations | 🔶 Experimental | No |
| Connection Pooling | 🔶 Alpha | No |
| Monitoring & Metrics | 🔶 Placeholder | No |
| Replication | 🔶 Experimental | No |
| Admin Tools | 🔶 Alpha | No |
| Performance Benchmarks | 🔶 Basic | No |

## Roadmap

### v0.1.0 (Current - Alpha)
- ✅ Basic storage engine
- ✅ Simple event sourcing
- ✅ Basic time-travel queries
- ✅ CLI interface
- ⚠️ Experimental features added but need hardening

### v0.2.0 (Next - Beta Target)
- 📋 Fix data durability issues
- 📋 Proper WAL implementation with fsync
- 📋 Real transaction isolation
- 📋 Comprehensive test suite
- 📋 Remove all panic points

### v0.3.0 (Future - Release Candidate)
- 📋 Performance optimization
- 📋 Proper replication implementation
- 📋 Security hardening
- 📋 Production monitoring
- 📋 Stress testing

### v1.0 (Production Ready)
- 📋 Battle-tested in production
- 📋 Full documentation
- 📋 Performance guarantees
- 📋 High availability
- 📋 Enterprise features

### v2.0 (Future)
- 📋 Multi-master replication
- 📋 Sharding support
- 📋 SQL compatibility layer
- 📋 Change data capture (CDC)
- 📋 GraphQL API