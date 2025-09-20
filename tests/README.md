# DriftDB Test Suite

A comprehensive test suite for DriftDB organized by category and complexity.

## Structure

```
tests/
├── README.md                    # This file
├── requirements.txt             # Test dependencies
├── conftest.py                 # Shared test fixtures
├── run_all_tests.py           # Master test runner
│
├── unit/                       # Unit tests for individual features
│   ├── test_basic_sql.py      # Basic SQL operations
│   ├── test_constraints.py    # Constraint validation
│   ├── test_sequences.py      # AUTO_INCREMENT/sequences
│   └── test_transactions.py   # ACID transaction tests
│
├── integration/                # Integration tests
│   ├── test_temporal.py       # Time-travel queries
│   ├── test_backup_restore.py # Backup/restore functionality
│   ├── test_replication.py    # Replication testing
│   └── test_wal.py           # WAL and recovery
│
├── sql/                        # SQL compatibility tests
│   ├── test_select.py         # SELECT variations
│   ├── test_joins.py          # JOIN operations
│   ├── test_aggregates.py     # COUNT, SUM, AVG, etc.
│   ├── test_set_operations.py # UNION, INTERSECT, EXCEPT
│   └── test_prepared.py       # Prepared statements
│
├── performance/                # Performance benchmarks
│   ├── test_insert_perf.py    # Insert throughput
│   ├── test_query_perf.py     # Query performance
│   └── test_index_perf.py     # Index optimization
│
├── stress/                     # Stress and load tests
│   ├── test_concurrent.py     # Concurrent connections
│   ├── test_large_data.py     # Large dataset handling
│   └── test_long_running.py   # Long-running operations
│
└── utils/                      # Test utilities
    ├── __init__.py
    ├── server.py              # Server management utilities
    ├── data_generator.py      # Test data generation
    └── assertions.py          # Custom assertions
```

## Running Tests

### Prerequisites

```bash
pip install -r tests/requirements.txt
```

### Run All Tests

```bash
python tests/run_all_tests.py
```

### Run Specific Category

```bash
# Unit tests only
python tests/run_all_tests.py --category unit

# SQL compatibility tests
python tests/run_all_tests.py --category sql

# Performance benchmarks
python tests/run_all_tests.py --category performance
```

### Run Individual Test

```bash
python -m pytest tests/unit/test_basic_sql.py
```

## Test Categories

### Unit Tests
- Fast, isolated tests
- Test individual features
- No external dependencies
- Run on every commit

### Integration Tests
- Test feature interactions
- Require running DriftDB server
- Test real-world scenarios
- Run before releases

### SQL Tests
- PostgreSQL compatibility
- SQL standard compliance
- Complex query validation
- Edge cases

### Performance Tests
- Throughput benchmarks
- Latency measurements
- Resource usage monitoring
- Regression detection

### Stress Tests
- Concurrent load
- Large datasets
- Memory leaks
- Crash recovery

## Writing New Tests

### Test Template

```python
import pytest
from tests.utils import DriftDBTestCase

class TestFeatureName(DriftDBTestCase):
    """Test description"""

    @classmethod
    def setUpClass(cls):
        """One-time setup for all tests in class"""
        super().setUpClass()
        cls.create_test_tables()

    def test_feature_basic(self):
        """Test basic functionality"""
        result = self.execute_query("SELECT 1")
        self.assert_result_count(result, 1)

    def test_feature_edge_case(self):
        """Test edge cases"""
        with self.assertRaises(ExpectedError):
            self.execute_query("INVALID SQL")
```

## CI/CD Integration

The test suite is integrated with CI/CD:

1. **Pre-commit**: Unit tests (fast)
2. **Pull Request**: Unit + Integration tests
3. **Main Branch**: Full test suite
4. **Release**: Full suite + Performance benchmarks

## Coverage Goals

- Unit Tests: 80% code coverage
- Integration Tests: All major features
- SQL Tests: PostgreSQL compatibility
- Performance: No regressions from baseline