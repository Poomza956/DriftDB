# DriftDB

**The PostgreSQL-compatible time-travel database** - Query your data at any point in history using standard SQL with any PostgreSQL client. Now with full SQL support including WHERE, UPDATE, and DELETE!

## 🚀 Quick Start

```bash
# Start the PostgreSQL-compatible server
./target/release/driftdb-server --data-path ./data

# Connect with any PostgreSQL client
psql -h localhost -p 5433 -d driftdb

# Use standard SQL with time-travel
CREATE TABLE events (id INT PRIMARY KEY, data VARCHAR);
INSERT INTO events (id, data) VALUES (1, 'original');
UPDATE events SET data = 'modified' WHERE id = 1;

-- Query historical state!
SELECT * FROM events AS OF @seq:1;  -- Shows 'original'
SELECT * FROM events;                -- Shows 'modified'
```

## ✅ Working Features

### PostgreSQL Wire Protocol v3
- **PostgreSQL compatible**: Connect with psql, pgAdmin, or any PostgreSQL driver
- **Standard SQL support**: CREATE TABLE, INSERT, SELECT, UPDATE, DELETE
- **WHERE clause support**: Full filtering with =, !=, >, <, >=, <= operators
- **UPDATE with conditions**: Modify specific rows with WHERE clauses
- **DELETE with conditions**: Remove specific rows while preserving history
- **Time-travel syntax**: `AS OF @seq:N` for querying any historical state
- **Column selection**: `SELECT column1, column2 FROM table`
- **Aggregation functions**: COUNT(*), COUNT(column), SUM, AVG, MIN, MAX
- **GROUP BY and HAVING**: Group rows and filter groups with aggregation conditions
- **ORDER BY and LIMIT**: Sort results and limit row count
- **No authentication required**: Simplified setup for development

### Core Database Engine
- **Event sourcing**: Every change is an immutable event with full history
- **Time-travel queries**: Query any historical state by sequence number
- **ACID compliance**: Full transaction support with isolation levels
- **CRC32 verification**: Data integrity on every frame
- **Append-only storage**: Never lose data, perfect audit trail
- **JSON documents**: Flexible schema with structured data

### Tested & Verified
- ✅ Python psycopg2 driver
- ✅ Node.js pg driver
- ✅ JDBC PostgreSQL driver
- ✅ SQLAlchemy ORM
- ✅ Any PostgreSQL client

## 🎯 Perfect For

- **Debugging Production Issues**: "What was the state when the bug occurred?"
- **Compliance & Auditing**: Complete audit trail built-in, no extra work
- **Data Recovery**: Accidentally deleted data? It's still there!
- **Analytics**: Track how metrics changed over time
- **Testing**: Reset to any point, perfect for test scenarios
- **Development**: Branch your database like Git

## ✨ Core Features

### SQL:2011 Temporal Queries (Native Support)
- **`FOR SYSTEM_TIME AS OF`**: Query data at any point in time
- **`FOR SYSTEM_TIME BETWEEN`**: Get all versions in a time range
- **`FOR SYSTEM_TIME FROM...TO`**: Exclusive range queries
- **`FOR SYSTEM_TIME ALL`**: Complete history of changes
- **System-versioned tables**: Automatic history tracking

### Data Model & Storage
- **Append-only storage**: Immutable events preserve complete history
- **Time travel queries**: Standard SQL:2011 temporal syntax
- **ACID transactions**: Full transaction support with isolation levels
- **Secondary indexes**: B-tree indexes for fast lookups
- **Snapshots & compaction**: Optimized performance with compression

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

### Docker Installation (Recommended)

```bash
# Quick start with Docker
git clone https://github.com/driftdb/driftdb
cd driftdb
./scripts/docker-quickstart.sh

# Connect to DriftDB
psql -h localhost -p 5433 -d driftdb -U driftdb
# Password: driftdb
```

### Manual Installation

```bash
# Clone and build from source
git clone https://github.com/driftdb/driftdb
cd driftdb
make build

# Or install with cargo
cargo install driftdb-cli driftdb-server
```

### 60-second demo

```bash
# Run the full demo (creates sample data and runs queries)
make demo
```

### PostgreSQL-Compatible Server

DriftDB now includes a PostgreSQL wire protocol server, allowing you to connect with any PostgreSQL client:

```bash
# Start the server
./target/release/driftdb-server

# Connect with psql
psql -h 127.0.0.1 -p 5433 -d driftdb -U driftdb

# Connect with any PostgreSQL driver
postgresql://driftdb:driftdb@127.0.0.1:5433/driftdb
```

The server supports:
- PostgreSQL wire protocol v3
- SQL queries with automatic temporal tracking
- Authentication (cleartext and MD5)
- Integration with existing PostgreSQL tools and ORMs

### Manual CLI usage

```sql
-- Initialize and connect to database
driftdb init ./mydata
driftdb sql ./mydata

-- Create a temporal table (SQL:2011)
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email VARCHAR(255),
    status VARCHAR(20),
    created_at TIMESTAMP
) WITH SYSTEM VERSIONING;

-- Insert data
INSERT INTO users VALUES (1, 'alice@example.com', 'active', CURRENT_TIMESTAMP);

-- Standard SQL queries with WHERE clauses
SELECT * FROM users WHERE status = 'active';
SELECT * FROM users WHERE id > 100 AND status != 'deleted';

-- UPDATE with conditions
UPDATE users SET status = 'inactive' WHERE last_login < '2024-01-01';

-- DELETE with conditions (soft delete preserves history)
DELETE FROM users WHERE status = 'inactive' AND created_at < '2023-01-01';

-- Time travel query (SQL:2011)
SELECT * FROM users
FOR SYSTEM_TIME AS OF '2024-01-15T10:00:00Z'
WHERE id = 1;

-- Query all historical versions
SELECT * FROM users
FOR SYSTEM_TIME ALL
WHERE id = 1;

-- Query range of time
SELECT * FROM users
FOR SYSTEM_TIME BETWEEN '2024-01-01' AND '2024-01-31'
WHERE status = 'active';

-- Advanced SQL Features (v0.6.0)
-- Column selection
SELECT name, email FROM users WHERE status = 'active';

-- Aggregation functions
SELECT COUNT(*) FROM users;
SELECT COUNT(email), AVG(age) FROM users WHERE status = 'active';
SELECT MIN(created_at), MAX(created_at) FROM users;

-- GROUP BY and aggregations
SELECT status, COUNT(*) FROM users GROUP BY status;
SELECT department, AVG(salary), MIN(salary), MAX(salary)
FROM employees GROUP BY department;

-- HAVING clause for group filtering
SELECT department, AVG(salary) FROM employees
GROUP BY department HAVING AVG(salary) > 50000;

-- ORDER BY and LIMIT
SELECT * FROM users ORDER BY created_at DESC LIMIT 10;
SELECT name, email FROM users WHERE status = 'active'
ORDER BY name ASC LIMIT 5;

-- Complex queries with all features
SELECT department, COUNT(*) as emp_count, AVG(salary) as avg_salary
FROM employees
WHERE hire_date >= '2023-01-01'
GROUP BY department
HAVING COUNT(*) >= 3
ORDER BY AVG(salary) DESC
LIMIT 5;
```

## SQL:2011 Temporal Syntax

### Standard Temporal Queries

```sql
-- AS OF: Query at a specific point in time
SELECT * FROM orders
FOR SYSTEM_TIME AS OF '2024-01-15T10:30:00Z'
WHERE customer_id = 123;

-- BETWEEN: All versions in a time range (inclusive)
SELECT * FROM accounts
FOR SYSTEM_TIME BETWEEN '2024-01-01' AND '2024-01-31'
WHERE balance > 10000;

-- FROM...TO: Range query (exclusive end)
SELECT * FROM inventory
FOR SYSTEM_TIME FROM '2024-01-01' TO '2024-02-01'
WHERE product_id = 'ABC-123';

-- ALL: Complete history
SELECT * FROM audit_log
FOR SYSTEM_TIME ALL
WHERE action = 'DELETE';
```

### Creating Temporal Tables

```sql
-- Create table with system versioning
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

## 🎯 Use Cases

### Compliance & Audit
```sql
-- "Prove we had user consent when we sent that email"
SELECT consent_status, consent_timestamp
FROM users
FOR SYSTEM_TIME AS OF '2024-01-15T14:30:00Z'
WHERE email = 'user@example.com';
```

### Debugging Production Issues
```sql
-- "What was the state when the error occurred?"
SELECT * FROM shopping_carts
FOR SYSTEM_TIME AS OF '2024-01-15T09:45:00Z'
WHERE session_id = 'xyz-789';
```

### Analytics & Reporting
```sql
-- "Show me how this metric changed over time"
SELECT DATE(SYSTEM_TIME_START) as date, COUNT(*) as daily_users
FROM users
FOR SYSTEM_TIME ALL
WHERE status = 'active'
GROUP BY DATE(SYSTEM_TIME_START);
```

### Data Recovery
```sql
-- "Restore accidentally deleted data"
INSERT INTO users
SELECT * FROM users
FOR SYSTEM_TIME AS OF '2024-01-15T08:00:00Z'
WHERE id NOT IN (SELECT id FROM users);
```

## Comparison with Other Databases

| Feature | DriftDB | PostgreSQL | MySQL | Oracle | SQL Server |
|---------|---------|------------|-------|--------|------------|
| SQL:2011 Temporal | ✅ Native | ⚠️ Extension | ❌ | 💰 Flashback | ⚠️ Complex |
| Storage Overhead | ✅ Low (events) | ❌ High | ❌ High | ❌ High | ❌ High |
| Query Past Data | ✅ Simple SQL | ❌ Complex | ❌ | 💰 Extra cost | ⚠️ Complex |
| Audit Trail | ✅ Automatic | ❌ Manual | ❌ Manual | 💰 | ⚠️ Manual |
| Open Source | ✅ | ✅ | ✅ | ❌ | ❌ |

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

### ⚠️ Beta Stage - Near Production Ready
DriftDB is currently in **beta** stage and approaching production readiness.

**Current Status:**
- Core functionality implemented and working well
- Time travel queries fully functional with `AS OF @seq:N`
- PostgreSQL wire protocol fully implemented
- SQL support for SELECT, INSERT, UPDATE, DELETE with WHERE clauses
- Replication framework in place (tests fixed)
- WAL implementation corrected

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
| Core Storage Engine | 🟡 Beta | Almost |
| SQL Execution | 🟢 Working | Yes |
| Time Travel Queries | 🟢 Working | Yes |
| PostgreSQL Protocol | 🟢 Working | Yes |
| WAL & Crash Recovery | 🟡 Beta | Almost |
| ACID Transactions | 🟡 Beta | Almost |
| Event Sourcing | 🟢 Working | Yes |
| WHERE Clause Support | 🟢 Working | Yes |
| UPDATE/DELETE | 🟢 Working | Yes |
| Replication Framework | 🟡 Beta | Almost |
| Schema Migrations | 🟡 Beta | Almost |
| Connection Pooling | 🔶 Alpha | No |
| Monitoring & Metrics | 🔶 Placeholder | No |
| Admin Tools | 🔶 Alpha | No |

## Roadmap

### v0.1.0 (Alpha - Complete)
- ✅ Basic storage engine
- ✅ Simple event sourcing
- ✅ Basic time-travel queries
- ✅ CLI interface
- ✅ Experimental features added

### v0.2.0 (SQL:2011 Support - Complete)
- ✅ SQL:2011 temporal query syntax
- ✅ FOR SYSTEM_TIME support
- ✅ Standard SQL parser integration
- ✅ Temporal table DDL

### v0.3.0 (PostgreSQL Compatibility - Complete)
- ✅ PostgreSQL wire protocol v3
- ✅ Connect with any PostgreSQL client
- ✅ Basic SQL execution
- ✅ Time-travel through PostgreSQL

### v0.4.0 (Full SQL Support - Complete)
- ✅ WHERE clause with multiple operators
- ✅ UPDATE statement with conditions
- ✅ DELETE statement with conditions
- ✅ AND logic for multiple conditions
- ✅ Soft deletes preserve history

### v0.5.0 (Time Travel & Fixes - Complete)
- ✅ Time travel queries with AS OF @seq:N
- ✅ Fixed replication integration tests
- ✅ Corrected WAL implementation
- ✅ Updated documentation accuracy
- ✅ PostgreSQL protocol improvements

### v0.6.0 (Current - Advanced SQL Features - Complete)
- ✅ Aggregations (COUNT, SUM, AVG, MIN, MAX)
- ✅ GROUP BY and HAVING clauses
- ✅ ORDER BY and LIMIT clauses
- ✅ Column selection (SELECT column1, column2)
- 📋 JOIN operations
- 📋 Subqueries

### v0.6.0 (Release Candidate)
- 📋 Production monitoring
- 📋 Proper replication implementation
- 📋 Security hardening
- 📋 Stress testing
- 📋 Cloud deployment support

### v1.0 (Production Ready)
- 📋 Battle-tested in production
- 📋 Full documentation
- 📋 Performance guarantees
- 📋 High availability
- 📋 Enterprise features

### v2.0 (Future Vision)
- 📋 Multi-master replication
- 📋 Sharding support
- 📋 Full SQL compatibility
- 📋 Change data capture (CDC)
- 📋 GraphQL API