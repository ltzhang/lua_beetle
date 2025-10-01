# Lua Beetle vs TigerBeetle Stress Test

Multi-threaded stress testing framework for comparing Lua Beetle (Redis-based) and TigerBeetle performance under various workload conditions.

## Features

- **Multi-threaded**: Configurable number of concurrent workers
- **Workload Mix**: Adjustable read/write ratio
- **Hot Account Skew**: Support for uniform and Zipf-distributed account access patterns
- **Batch Operations**: Configurable batch sizes for optimal throughput
- **Comprehensive Metrics**: Throughput, latency, success rates, and operation counts
- **Side-by-Side Comparison**: Run both systems with identical workloads

## Installation

### Prerequisites

1. **Go 1.21+**
2. **Redis** (for Lua Beetle testing)
   ```bash
   # Install Redis
   sudo apt-get install redis-server  # Ubuntu/Debian
   brew install redis                  # macOS

   # Start Redis
   redis-server
   ```

3. **TigerBeetle** (for TigerBeetle testing)
   ```bash
   # Download and start TigerBeetle
   curl -Lo tigerbeetle.zip https://linux.tigerbeetle.com && unzip tigerbeetle.zip
   ./tigerbeetle format --cluster=0 --replica=0 --replica-count=1 --development ./0_0.tigerbeetle
   ./tigerbeetle start --addresses=3000 --development ./0_0.tigerbeetle
   ```

### Build

```bash
cd stress_test
go mod download
go build -o stress_test
```

## Usage

### Basic Examples

**Test Redis (Lua Beetle) with defaults:**
```bash
./stress_test -mode=redis
```

**Test TigerBeetle:**
```bash
./stress_test -mode=tigerbeetle -tb-address=3000
```

**Compare both systems:**
```bash
./stress_test -mode=both
```

### Configuration Parameters

| Flag | Default | Description |
|------|---------|-------------|
| `-mode` | `redis` | Test mode: `redis`, `tigerbeetle`, or `both` |
| `-accounts` | `10000` | Number of accounts to create |
| `-workers` | `10` | Number of concurrent worker threads |
| `-duration` | `60` | Test duration in seconds |
| `-read-ratio` | `0.5` | Ratio of read operations (0.0-1.0) |
| `-skew` | `0.0` | Hot account skew (0=uniform, 0.99=very skewed) |
| `-batch` | `100` | Operations per batch |
| `-ledger` | `700` | Ledger ID for accounts/transfers |
| `-verbose` | `false` | Enable verbose output |
| `-tb-address` | `3000` | TigerBeetle server address |
| `-no-cleanup` | `false` | Skip cleanup after test |

### Advanced Examples

**High contention workload (hot accounts with Zipf distribution):**
```bash
./stress_test -mode=both \
  -accounts=10000 \
  -workers=50 \
  -duration=120 \
  -read-ratio=0.3 \
  -skew=0.99 \
  -batch=100
```

**Read-heavy workload:**
```bash
./stress_test -mode=both \
  -accounts=50000 \
  -workers=20 \
  -read-ratio=0.9 \
  -skew=0.5
```

**Write-heavy workload with uniform distribution:**
```bash
./stress_test -mode=both \
  -accounts=20000 \
  -workers=30 \
  -read-ratio=0.1 \
  -skew=0.0
```

**Large batch sizes (TigerBeetle optimized):**
```bash
./stress_test -mode=both \
  -workers=10 \
  -batch=1000 \
  -duration=300
```

## Workload Characteristics

### Read/Write Ratio

- `0.0`: 100% writes (create transfers)
- `0.5`: 50% reads, 50% writes
- `1.0`: 100% reads (lookup accounts, get transfers)

### Hot Account Skew (Zipf Distribution)

Controls transaction contention by concentrating operations on fewer accounts:

- `0.0`: **Uniform** - All accounts equally likely (no contention)
- `0.5`: **Mild skew** - Some accounts more popular
- `0.99`: **Moderate skew** - Significant concentration on hot accounts
- `1.5+`: **Heavy skew** - Extreme concentration (high contention)

The skew parameter maps to Zipf's s parameter: `s = 1.0 + skew * 10.0`

## Operations

### Read Operations (50/50 split)
- **Lookup Accounts**: Batch lookup of random accounts
- **Get Account Transfers**: Retrieve transfer history for an account

### Write Operations
- **Create Transfers**: Batch creation of transfers between random accounts

## Output Metrics

```
=== Redis (Lua Beetle) Results ===
Duration: 60.00 seconds
Operations Completed: 12450
Operations Failed: 23
Transfers Created: 621500
Accounts Looked Up: 3725
Throughput: 207.50 ops/sec
Average Latency: 4.82 ms
Success Rate: 99.82%
```

### Metrics Explained

- **Operations Completed**: Total successful batch operations
- **Operations Failed**: Failed operations (network errors, validation errors)
- **Transfers Created**: Individual transfers successfully created
- **Accounts Looked Up**: Individual accounts read
- **Throughput**: Operations per second
- **Average Latency**: Average time per operation (ms)
- **Success Rate**: Percentage of successful operations

## Benchmarking Scenarios

### 1. Low Contention (Uniform Distribution)
Tests maximum throughput with minimal lock contention:
```bash
./stress_test -mode=both -accounts=100000 -workers=50 -skew=0.0 -duration=300
```

### 2. High Contention (Hot Accounts)
Tests performance under heavy contention:
```bash
./stress_test -mode=both -accounts=10000 -workers=100 -skew=1.2 -duration=300
```

### 3. Mixed Workload
Simulates realistic production workload:
```bash
./stress_test -mode=both -accounts=50000 -workers=30 -read-ratio=0.7 -skew=0.6 -duration=600
```

### 4. Peak Load
Tests system limits:
```bash
./stress_test -mode=both -accounts=20000 -workers=200 -batch=500 -duration=300
```

## Architecture

### Redis (Lua Beetle)
- **Storage**: Redis hashes for accounts/transfers, sorted sets for indexes
- **Atomicity**: Lua scripts executed via EVALSHA
- **Concurrency**: Redis single-threaded event loop with pipelining
- **Indexing**: Secondary indexes for efficient queries

### TigerBeetle
- **Storage**: Custom LSM-tree database
- **Atomicity**: ACID transactions with deterministic simulation
- **Concurrency**: Lock-free data structures, async I/O
- **Indexing**: Native support for account/transfer queries

## Troubleshooting

### Redis Connection Error
```
failed to connect to Redis: dial tcp [::1]:6379: connect: connection refused
```
**Solution**: Ensure Redis is running: `redis-server`

### TigerBeetle Connection Error
```
failed to create TigerBeetle client: connection refused
```
**Solution**:
1. Check TigerBeetle is running: `ps aux | grep tigerbeetle`
2. Verify address matches: `-tb-address=3000`

### High Operation Failure Rate
- **Redis**: Check Lua script errors with `-verbose`
- **TigerBeetle**: Check for account/transfer validation errors
- **Both**: Reduce batch size or worker count

### Low Throughput
- Increase batch size: `-batch=1000`
- Adjust worker count based on CPU cores
- For Redis: Ensure scripts are loaded (not using EVAL)
- For TigerBeetle: Use maximum batch size (8000)

## Performance Tips

### For Redis (Lua Beetle)
1. Use script loading (SCRIPT LOAD + EVALSHA) - implemented by default
2. Batch operations when possible
3. Consider Redis pipelining for multiple independent operations
4. Monitor Redis memory usage with `redis-cli INFO memory`

### For TigerBeetle
1. Maximize batch sizes (up to 8191)
2. Share a single client across workers
3. Use multiple replicas for production
4. Avoid creating many small batches

## Example Comparison Run

```bash
./stress_test -mode=both -accounts=20000 -workers=20 -duration=120 -read-ratio=0.5 -skew=0.8

=== Stress Test Configuration ===
Mode: both
Accounts: 20000
Workers: 20
Duration: 120 seconds
Read Ratio: 0.50
Hot Account Skew: 0.80 (moderate skew)
Batch Size: 100

[... Redis test results ...]

============================================================

[... TigerBeetle test results ...]

✅ Stress test completed successfully!
```

## Notes

- **Cleanup**: By default, Redis data is flushed after tests. TigerBeetle has an immutable ledger.
- **Account IDs**: Start at 1 and increment sequentially during setup
- **Transfer IDs**: Generated using timestamp + worker ID + counter for uniqueness
- **Deterministic**: Use same random seed for reproducible workloads (not currently exposed)

## License

MIT
