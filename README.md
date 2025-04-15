# lib_lsm: Log-Structured Merge Tree Database

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

A high-performance key-value store built on the Log-Structured Merge Tree architecture with
PostgreSQL wire protocol compatibility.

## Features

- **Log-Structured Merge Tree**: Optimized for high write throughput
- **PostgreSQL Compatible**: Connect using standard PostgreSQL clients
- **Multi-language Support**: C FFI interface 

## Quick Start

### Prerequisites

- Zig 0.13.0+
- Go 1.22+ (for PostgreSQL interface)
- Cargo

### Building

```bash
# Build the core library
make build
```

### Running

Start the PostgreSQL-compatible server:

```bash
make go
```

Connect with any PostgreSQL client:

```bash
psql -h localhost -p 54321 -d postgres
```

## Usage Examples

### Creating a Table

```sql
CREATE TABLE users (id int, name text, email text);
```

### Inserting Data

```sql
INSERT INTO users VALUES (1, 'John Doe', 'john@example.com');
```

### Querying Data

```sql
SELECT id, name FROM users;
```

## Configuration

The database can be configured with various options:

- `--data_dir`: Directory for storing data files
- `--sst_capacity`: SSTable capacity (default: 1,000,000)

## Architecture

lib_lsm implements a Log-Structured Merge Tree with the following components:

- **MemTable**: In-memory storage using a skiplist
- **WAL**: Write-ahead log for durability
- **SSTables**: Sorted string tables for persistent storage
- **Bloom Filters**: Efficient negative lookups
- **Compaction**: Background merging of SSTables

## Development

```bash
# Format code
make fmt

# Run tests
make test

# Debug mode
make debug

# Profile performance
make profile
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
