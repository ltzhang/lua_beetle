# Comprehensive Benchmark Suite

## Overview

This benchmark suite comprehensively tests **Lua Beetle (Redis)** vs **TigerBeetle** across multiple dimensions:

- **Workloads**: transfer, lookup, twophase, mixed
- **Hot Account Ratios**: 1, 10, 50, 100, 1000, 10000 (out of 100,000 total accounts)
- **Worker Threads**: 1, 2, 4, 8
- **Duration**: 30 seconds per test
- **Total Tests**: 192 (2 modes × 4 workloads × 6 hot configs × 4 worker counts)

## Running the Benchmark

### Start the benchmark suite:

```bash
cd /home/lintaoz/work/lua_beetle/stress_test
./run_benchmarks.sh
```

This will:
1. Run all 192 test configurations
2. Reset the database between each test
3. Save results to `benchmark_results_<timestamp>/`
4. Generate a `summary.csv` with all metrics
5. Estimated runtime: ~2 hours

### Monitor progress:

```bash
./monitor_benchmarks.sh
```

Or check the log:

```bash
tail -f benchmark_run.log
```

## Analyzing Results

After the benchmark completes:

```bash
python3 analyze_results.py benchmark_results_<timestamp>/
```

This generates:
- **Throughput comparisons** (Redis vs TigerBeetle)
- **Worker scaling analysis** (1/2/4/8 workers)
- **Hot account impact** (how hot account ratio affects performance)
- **Latency comparisons**
- **Summary statistics** (average, max, speedup)

### Quick view of results:

```bash
cat benchmark_results_*/summary.csv | column -t -s','
```

## Test Configuration Details

### Workload Types

1. **transfer** - Pure transfer workload
   - Creates regular transfers between 1 hot + 1 random account
   - Tests write-heavy performance

2. **lookup** - Pure lookup workload
   - 50% lookups on hot accounts
   - 50% lookups on random accounts
   - Tests read-heavy performance

3. **twophase** - Two-phase transfer workload
   - 50% create pending transfers
   - 25% post pending → posted
   - 25% void pending
   - Tests complex transaction workflows

4. **mixed** - Mixed workload
   - 70% transfers (20% of which are two-phase)
   - 30% lookups
   - Tests realistic mixed read/write patterns

### Hot/Cold Account Model

Transfers always involve:
- 1 hot account (randomly selected from hot pool)
- 1 random account (from entire account pool)

This simulates realistic workloads where certain accounts (e.g., exchange wallets, popular merchants) are "hot" and see disproportionate traffic.

### Key Metrics

Each test collects:
- **Throughput** (ops/sec)
- **Average Latency** (ms)
- **Transfer Count**
- **Lookup Count**
- **Two-Phase Transfer Stats** (pending/posted/voided)

## Expected Insights

The benchmark suite is designed to reveal:

1. **Raw Performance**: How does Lua Beetle (Redis) compare to TigerBeetle?

2. **Scalability**: How well does each system scale with more workers?

3. **Hot Spot Performance**: How does performance degrade with contention on hot accounts?

4. **Workload Characteristics**: Which system excels at which workload types?

5. **Latency vs Throughput**: Trade-offs between batch size and latency

## Directory Structure

```
benchmark_results_<timestamp>/
├── summary.csv                          # All results in CSV format
├── redis_transfer_hot1_workers1.txt     # Individual test outputs
├── redis_transfer_hot1_workers2.txt
├── ...
├── tigerbeetle_transfer_hot1_workers1.txt
└── ...
```

## Manual Testing

Run individual configurations:

```bash
# Redis transfer test with 100 hot accounts, 4 workers
./stress_test -mode=redis -workload=transfer -accounts=100000 \
  -hot-accounts=100 -workers=4 -duration=30 -batch=100

# TigerBeetle mixed workload with 1000 hot accounts, 8 workers
./stress_test -mode=tigerbeetle -workload=mixed -accounts=100000 \
  -hot-accounts=1000 -workers=8 -duration=30 -batch=100 \
  -transfer-ratio=0.7 -twophase-ratio=0.2
```

## Prerequisites

### Redis
```bash
# Start Redis
cd /home/lintaoz/database/redis
./src/redis-server --daemonize yes

# Verify
./src/redis-cli ping
```

### TigerBeetle
```bash
# Format and start TigerBeetle
cd /home/lintaoz/work/lua_beetle/third_party
./tigerbeetle format --cluster=0 --replica=0 --replica-count=1 \
  --development ./0_0.tigerbeetle
./tigerbeetle start --addresses=3000 --development ./0_0.tigerbeetle &
```

## Cleanup

Kill all processes:
```bash
killall -9 tigerbeetle
redis-cli shutdown
```

## Notes

- Database is reset between each test to ensure clean slate
- TigerBeetle uses smaller batch size (100) due to message size limits
- Redis uses FLUSHDB for cleanup
- TigerBeetle requires kill + delete data file + reformat for cleanup
- All tests use batch size 100 for consistency
- Ledger ID is 700 for all tests
