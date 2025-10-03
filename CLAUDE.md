# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Lua Beetle is a Redis Lua implementation of TigerBeetle's core financial transaction APIs. It provides functionally equivalent operations for account management and atomic transfers using Redis-compatible backends (Redis, DragonflyDB, EloqKV), matching TigerBeetle's exact error codes, flags, and semantics.

**Key Features:**
- Binary encoding (128-byte fixed format)
- Two-phase transfers (pending/post/void)
- Hot/cold account workload modeling
- Multiple workload types (transfer, lookup, twophase, mixed)
- Comprehensive functional and stress testing

## Data Encoding

**ALL operations use binary encoding (128 bytes fixed-size format):**

```
Account (128 bytes):
  [0:16]    ID (uint128, little-endian)
  [16:32]   debits_pending (uint128)
  [32:48]   debits_posted (uint128)
  [48:64]   credits_pending (uint128)
  [64:80]   credits_posted (uint128)
  [80:96]   user_data_128 (uint128)
  [96:112]  user_data_64/user_data_32 (uint64/uint32)
  [112:116] ledger (uint32)
  [116:118] code (uint16)
  [118:120] flags (uint16)
  [120:128] timestamp/reserved (uint64)

Transfer (128 bytes):
  [0:16]    ID (uint128)
  [16:32]   debit_account_id (uint128)
  [32:48]   credit_account_id (uint128)
  [48:64]   amount (uint128)
  [64:80]   pending_id (uint128) - for post/void operations
  [80:96]   user_data_128 (uint128)
  [96:112]  user_data_64/user_data_32 (uint64/uint32)
  [112:116] ledger (uint32)
  [116:118] code (uint16)
  [118:120] flags (uint16)
  [120:128] timestamp (uint64)
```

**Flags:**
- `0x0001` - LINKED (not currently used, reserved)
- `0x0002` - PENDING (create pending transfer)
- `0x0004` - POST_PENDING_TRANSFER (commit pending)
- `0x0008` - VOID_PENDING_TRANSFER (cancel pending)

## Test Data Directory

**IMPORTANT**: All test executables MUST run with data stored in `/mnt/ramdisk/tests/` for performance:

```bash
# Ensure ramdisk directory exists
mkdir -p /mnt/ramdisk/tests

# All databases should use this directory for data files
# This avoids disk I/O and provides consistent performance
```

## Running Services

### Redis (Port 6379)

```bash
# Start Redis with data in ramdisk
cd /mnt/ramdisk/tests
/home/lintaoz/database/redis/src/redis-server \
  --dir /mnt/ramdisk/tests \
  --daemonize yes

# Verify
/home/lintaoz/database/redis/src/redis-cli ping  # Should return: PONG

# Stop and clean
/home/lintaoz/database/redis/src/redis-cli shutdown
rm -f /mnt/ramdisk/tests/dump.rdb

# Check if running
pgrep redis-server
```

### EloqKV (Port 6379 - Redis Compatible)

**IMPORTANT**: EloqKV uses the same port as Redis. Ensure Redis is NOT running before starting EloqKV.

```bash
# Kill any running Redis first
killall redis-server 2>/dev/null

# Start EloqKV with data in ramdisk
cd /mnt/ramdisk/tests
/path/to/eloqkv \
  --port 6379

# Verify (use redis-cli - it's compatible)
redis-cli ping  # Should return: PONG

# Stop and clean
killall eloqkv
rm -rf /mnt/ramdisk/tests/eloqkv_data

# Check if running
pgrep eloqkv
```

### DragonflyDB (Port 6380)

**Important**: Must use `--default_lua_flags=allow-undeclared-keys` because Lua Beetle scripts access keys dynamically.

```bash
# Start DragonflyDB with data in ramdisk
cd /mnt/ramdisk/tests
/home/lintaoz/work/lua_beetle/third_party/dragonfly-x86_64 \
  --logtostderr \
  --port=6380 \
  --dir=/mnt/ramdisk/tests \
  --dbfilename=dragonfly.db \
  --default_lua_flags=allow-undeclared-keys > dragonfly.log 2>&1 &

# Verify
redis-cli -p 6380 ping  # Should return: PONG

# Stop and clean
killall dragonfly-x86_64
rm -f /mnt/ramdisk/tests/dragonfly.db /mnt/ramdisk/tests/dragonfly.log

# Check if running
pgrep -f dragonfly
```

### TigerBeetle (Port 3000)

```bash
# Format and start with data in ramdisk
cd /mnt/ramdisk/tests
/home/lintaoz/work/lua_beetle/third_party/tigerbeetle format \
  --cluster=0 --replica=0 --replica-count=1 --development \
  ./0_0.tigerbeetle

/home/lintaoz/work/lua_beetle/third_party/tigerbeetle start \
  --addresses=3000 --development \
  ./0_0.tigerbeetle > tigerbeetle.log 2>&1 &

# Verify
pgrep tigerbeetle

# Stop and clean (TigerBeetle has immutable ledger - must delete to reset)
killall tigerbeetle
rm -f /mnt/ramdisk/tests/0_0.tigerbeetle /mnt/ramdisk/tests/tigerbeetle.log

# Check if running
pgrep tigerbeetle
```

## Functional Tests

### Python Tests

```bash
# Start Redis first
cd /mnt/ramdisk/tests
/home/lintaoz/database/redis/src/redis-server --dir /mnt/ramdisk/tests --daemonize yes

# Run tests
cd /home/lintaoz/work/lua_beetle/tests
source ~/venv/bin/activate
python3 test_functional.py

# Expected output:
# ✅ All tests passed!
```

**Test Coverage:**
- Account creation and lookup
- Duplicate detection
- Simple transfers
- Multiple transfers
- Two-phase transfers (pending/post/void)
- Error handling

### Go Tests

```bash
# Start Redis first
cd /mnt/ramdisk/tests
/home/lintaoz/database/redis/src/redis-server --dir /mnt/ramdisk/tests --daemonize yes

# Run tests
cd /home/lintaoz/work/lua_beetle/stress_test
go test -v functional_test.go common.go

# Expected output:
# PASS
# ok  	command-line-arguments	0.059s
```

## Stress Tests

**CRITICAL**: Always clean data before running stress tests!

### Quick Test Examples

```bash
# Redis transfer workload
./stress_test -mode=redis -workload=transfer \
  -accounts=10000 -hot-accounts=100 -workers=4 -duration=30 -batch=100

# TigerBeetle lookup workload
./stress_test -mode=tigerbeetle -workload=lookup \
  -accounts=10000 -hot-accounts=100 -workers=4 -duration=30 -batch=100

# Mixed workload (70% transfers, 20% two-phase)
./stress_test -mode=redis -workload=mixed \
  -transfer-ratio=0.7 -twophase-ratio=0.2 \
  -accounts=10000 -hot-accounts=100 -workers=4 -duration=30 -batch=100
```

### Workload Types

1. **`transfer`** - Pure transfer workload
   - Transfers between 1 hot account + 1 random account
   - Tests write-heavy performance

2. **`lookup`** - Pure lookup workload
   - 50% lookups on hot accounts
   - 50% lookups on random accounts
   - Tests read-heavy performance

3. **`twophase`** - Two-phase transfer workload
   - 50% create pending
   - 25% post pending
   - 25% void pending
   - Tests complex workflows

4. **`mixed`** - Configurable mix
   - `-transfer-ratio=0.7` - 70% transfers, 30% lookups
   - `-twophase-ratio=0.2` - 20% of transfers are two-phase

### Hot/Cold Account Model

**Key Concept**: Simulates realistic workloads where certain accounts see disproportionate traffic.

- Transfers always involve: **1 hot account + 1 random account**
- Lookups: **50% hot, 50% random**

This models real scenarios like exchange wallets, popular merchants, etc.

### Comprehensive Benchmark Suite

```bash
# Run full benchmark suite (192 tests, ~2 hours)
cd /home/lintaoz/work/lua_beetle/stress_test
./run_benchmarks.sh

# Monitor progress
./monitor_benchmarks.sh

# Analyze results
python3 analyze_results.py benchmark_results_<timestamp>/
```

**Test Matrix:**
- **Backends**: Redis, TigerBeetle
- **Workloads**: transfer, lookup, twophase, mixed
- **Hot Accounts**: 1, 10, 50, 100, 1000, 10000 (out of 100K total)
- **Workers**: 1, 2, 4, 8
- **Total**: 192 test configurations

### Cleaning Data Between Tests

**Redis/DragonflyDB:**
```bash
redis-cli FLUSHDB
# or
redis-cli -p 6380 FLUSHDB  # DragonflyDB
```

**EloqKV:**
```bash
killall eloqkv
rm -rf /mnt/ramdisk/tests/eloqkv_data
# Restart EloqKV
```

**TigerBeetle:**
```bash
killall tigerbeetle
rm -f /mnt/ramdisk/tests/0_0.tigerbeetle
# Re-format and restart
```

## Script Architecture

```
scripts/
├── create_account.lua         # Create 1 account (binary format)
├── create_transfer.lua        # Create 1 transfer (binary format)
│                              # Supports two-phase (PENDING/POST/VOID)
├── lookup_account.lua         # Lookup 1 account by ID
├── lookup_transfer.lua        # Lookup 1 transfer by ID
├── get_account_transfers.lua  # Get transfers for 1 account
└── get_account_balances.lua   # Get balance history for 1 account
```

**All scripts use binary encoding (128 bytes fixed-size format).**

## Data Storage Schema

**Primary Storage:**
- Accounts: `account:{id}` - Binary blob (128 bytes)
- Transfers: `transfer:{hex_id}` - Binary blob (128 bytes)
  - Note: Transfer IDs are stored as hex strings for Redis key compatibility

**Secondary Indexes:**
- Transfer index: `account:{id}:transfers` - Sorted set (score=timestamp)
- Balance history: `account:{id}:balance_history` - Sorted set (if HISTORY flag set)

## Two-Phase Transfer Implementation

### Creating Pending Transfer

```go
transferData := encoder.EncodeTransfer(
    "pending_tx_1",     // transfer ID
    accountID1,         // debit account
    accountID2,         // credit account
    500,                // amount
    ledger, code,
    0x0002,            // PENDING flag
)
client.EvalSha(ctx, createTransferSHA, []string{}, transferData)
```

**Effect:**
- Increases `debits_pending` on debit account
- Increases `credits_pending` on credit account
- No change to `*_posted` fields

### Posting Pending Transfer

```go
transferData := encoder.EncodeTransferWithPending(
    "post_tx_1",        // new transfer ID
    accountID1,         // same accounts
    accountID2,
    500,                // same amount
    "pending_tx_1",     // reference to pending transfer
    ledger, code,
    0x0004,            // POST_PENDING flag
)
client.EvalSha(ctx, createTransferSHA, []string{}, transferData)
```

**Effect:**
- Decreases `debits_pending`, increases `debits_posted`
- Decreases `credits_pending`, increases `credits_posted`
- Completes the transfer

### Voiding Pending Transfer

```go
transferData := encoder.EncodeTransferWithPending(
    "void_tx_1",        // new transfer ID
    accountID1,
    accountID2,
    500,
    "pending_tx_1",     // reference to pending transfer
    ledger, code,
    0x0008,            // VOID_PENDING flag
)
client.EvalSha(ctx, createTransferSHA, []string{}, transferData)
```

**Effect:**
- Decreases `debits_pending` to 0
- Decreases `credits_pending` to 0
- Cancels the transfer

## Error Codes

**Success:**
- `0` - OK

**Account Errors:**
- `21` - ID already exists
- `38` - Debit account not found
- `39` - Credit account not found
- `40` - Accounts must be different

**Transfer Errors:**
- `34` - Pending transfer not found
- `35` - Pending transfer already posted
- `36` - Pending transfer already voided
- `42` - Exceeds credits
- `43` - Exceeds debits

## Performance Considerations

1. **Always use SCRIPT LOAD + EVALSHA** (never EVAL)
2. **Use pipelining** for independent operations
3. **Batch sizes**:
   - Redis/DragonflyDB: 100-1000 operations
   - TigerBeetle: up to 8189 operations
4. **Connection reuse**: Reuse Redis connections
5. **Ramdisk**: Always use `/mnt/ramdisk/tests/` for test data

## Common Debugging Practices

When debugging:
1. Add intrusive sanity checks directly in Lua scripts to verify constraints
2. Insert debug print statements liberally to track variable changes
3. Check consistency immediately after operations (e.g., verify balances match expected)
4. Always crash/exit tests immediately on unexpected behavior - never continue on errors
5. Remove all debug code and verbose prints after debugging is complete

## Testing Strategy

Tests MUST:
- **Always clean data first** before running stress tests
- Exit immediately on any failure (no continuing after errors)
- Verify consistency at each step
- Use `/mnt/ramdisk/tests/` for all test data
- Ensure no port conflicts (e.g., Redis vs EloqKV both use 6379)

## Key Differences from TigerBeetle

- **Binary encoding**: Fixed 128-byte format (vs. TigerBeetle's native format)
- **ID storage**: Transfer IDs stored as hex strings in Redis keys
- **Timestamps**: Uses Redis TIME for nanosecond precision
- **No built-in timeout**: Must be handled externally
- **Manual indexes**: Secondary indexes maintained by scripts

## Common Pitfalls

1. ❌ **Don't forget to clean data** before stress tests
2. ❌ **Don't run Redis and EloqKV simultaneously** (same port)
3. ❌ **Don't use disk for test data** - always use `/mnt/ramdisk/tests/`
4. ❌ **Don't mix binary and JSON encoding** - only binary is supported
5. ❌ **Don't forget TigerBeetle needs reformat** to reset data

## Reference Documentation

- TigerBeetle docs: https://docs.tigerbeetle.com/
- Two-phase transfers: https://docs.tigerbeetle.com/coding/two-phase-transfers/
- Redis Lua scripting: https://redis.io/docs/manual/programmability/eval-intro/
- Benchmark documentation: `stress_test/README_BENCHMARKS.md`
- Test documentation: `tests/README.md`
