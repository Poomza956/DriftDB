# Changelog

All notable changes to DriftDB will be documented in this file.

## [1.0.0] - 2025-01-21

### Added
- 100% SQL compatibility achieved!
- Recursive Common Table Expressions (WITH RECURSIVE)
- Support for parenthesized arithmetic expressions
- Proper CTE table references in JOIN operations
- Complete expression evaluation in all contexts

### Fixed
- Fixed arithmetic evaluation in recursive CTEs (n + 1 was evaluating as n + 2)
- Fixed CTE table references in recursive JOIN operations
- Fixed parenthesized expressions returning null in simple SELECT
- Fixed column name preservation in CTE results
- Consolidated arithmetic evaluation to single consistent function

### Removed
- Removed all DriftQL references - now 100% SQL compatible
- Removed duplicate arithmetic evaluation functions

## [0.4.0] - 2024-01-20

### Added
- Full SQL aggregation support (SUM, COUNT, AVG, MIN, MAX)
- GROUP BY with HAVING clause filtering
- ORDER BY with multi-column sorting (ASC/DESC)
- LIMIT and OFFSET for pagination
- UPDATE with arithmetic expressions (e.g., `price * 0.9`)
- Support for multiple sequential JOINs
- Proper column name resolution in complex JOIN queries
- DELETE FROM syntax support

### Fixed
- Fixed UPDATE statement expression evaluation returning null
- Fixed GROUP BY not grouping correctly after JOINs
- Fixed compound identifier handling (table.column notation)
- Fixed multiple JOIN column projection showing wrong values
- Fixed DELETE statement parsing with FROM clause

### Improved
- Enhanced SQL to DriftQL query translation
- Better handling of table aliases in JOIN operations
- Improved column prefix management for conflicting names

## [0.3.0] - 2024-01-19

### Added
- SQL to DriftQL bridge for SQL query execution
- INNER JOIN, LEFT JOIN, and CROSS JOIN support
- Basic SQL parsing and execution framework

## [0.2.0] - Previous

### Added
- Core append-only storage engine
- Time-travel query capabilities
- B-tree secondary indexes
- CRC32 data verification
- Basic DriftQL query language

## [0.1.0] - Initial Release

### Added
- Initial DriftDB implementation
- Event sourcing architecture
- Basic table storage
- Simple query execution