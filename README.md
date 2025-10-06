# Lua Beetle

A Redis Lua implementation of TigerBeetle's core financial transaction APIs. Lua Beetle provides functionally equivalent operations for account management and atomic transfers using Redis-compatible backends (Redis, DragonflyDB, EloqKV).

## Features

- ✅ **Binary Encoding**: Fixed 128-byte format for accounts/transfers (matches TigerBeetle)
- ✅ **Account Management**: Create and lookup accounts with balance tracking
- ✅ **Atomic Transfers**: Single-phase and two-phase (pending/post/void) transfers
- ✅ **Double-Entry Bookkeeping**: Maintains debit/credit balance invariants
- ✅ **Balance Constraints**: Support for overdraft protection and balance limits
- ✅ **Linked Operations**: All-or-nothing atomic batches with rollback
- ✅ **Account Filters**: Full TigerBeetle AccountFilter support (128-byte binary)
- ✅ **Transfer Queries**: Get transfers with filtering, sorting, pagination
- ✅ **Balance History**: Track balance snapshots (64-byte binary, HISTORY flag)

## Project Structure

```
lua_beetle/
├── scripts/
│   ├── create_account.lua           # Create 1 account (binary 128-byte)
│   ├── create_linked_accounts.lua   # Create linked accounts with rollback
│   ├── create_transfer.lua          # Create 1 transfer (binary 128-byte)
│   ├── create_linked_transfers.lua  # Create linked transfers with rollback
│   ├── lookup_account.lua           # Lookup 1 account by ID
│   ├── lookup_transfer.lua          # Lookup 1 transfer by ID
│   ├── get_account_transfers.lua    # Query transfers with AccountFilter
│   └── get_account_balances.lua     # Query balance history with AccountFilter
├── tests/
│   ├── functional_tests.py          # Comprehensive Python functional tests
│   ├── functional_tests.go          # Comprehensive Go functional tests
│   ├── main.go                      # Stress test entry point
│   ├── common.go                    # Binary encoder and shared utilities
│   ├── luabeetle_stress.go          # Lua Beetle stress test implementation
│   ├── tigerbeetle_stress.go        # TigerBeetle stress test
│   ├── run_benchmarks.sh            # Comprehensive benchmark suite
│   ├── analyze_results.py           # Results analysis
│   └── go.mod                       # Go module
├── third_party/
│   ├── redis-server                 # Redis executable
│   ├── redis-cli                    # Redis CLI
│   ├── dragonfly-x86_64            # DragonflyDB executable
│   ├── eloqkv                       # EloqKV executable
│   └── tigerbeetle                  # TigerBeetle executable
└── README.md
```

## Installation

### Prerequisites
- Python 3.8+ with `redis` client: `pip install redis`
- Go 1.19+ (for stress tests)

### Setting Up Executables

**IMPORTANT**: The `third_party/` directory is **NOT** included in the repository. You must download and place the following executables in `third_party/` yourself:

- **Redis**: Download from [redis.io](https://redis.io/download/) → Place `redis-server` and `redis-cli` in `third_party/`
- **DragonflyDB**: Download from [dragonflydb.io](https://www.dragonflydb.io/) → Place `dragonfly-x86_64` in `third_party/`
- **EloqKV**: Download from [EloqKV releases](https://github.com/c3exchange/eloqkv) → Place `eloqkv` in `third_party/`
- **TigerBeetle**: Download from [tigerbeetle.com](https://tigerbeetle.com/) → Place `tigerbeetle` in `third_party/`

```bash
# Example setup
mkdir -p third_party
cd third_party
# Download and place executables here
chmod +x redis-server redis-cli dragonfly-x86_64 eloqkv tigerbeetle
```

All tests and benchmarks will use executables from `third_party/` by default.

### Quick Start

```bash
# 1. Start Redis with data in ramdisk
mkdir -p /mnt/ramdisk/tests
./third_party/redis-server --dir /mnt/ramdisk/tests --daemonize yes

# 2. Run functional tests
cd tests
python3 functional_tests.py
go run functional_tests.go common.go

# 3. Run stress tests
cd tests
go build -o stress_test main.go common.go luabeetle_stress.go tigerbeetle_stress.go
./stress_test -mode=redis -workload=transfer -accounts=10000 -workers=4 -duration=30
```

## Data Encoding

**ALL operations use binary encoding with fixed-size formats:**

### Account (128 bytes)
```
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
```

**Account Flags:**
- `0x0001` - LINKED (chain operations)
- `0x0002` - DEBITS_MUST_NOT_EXCEED_CREDITS
- `0x0004` - CREDITS_MUST_NOT_EXCEED_DEBITS
- `0x0008` - HISTORY (enable balance history tracking)

### Transfer (128 bytes)
```
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

**Transfer Flags:**
- `0x0001` - LINKED (chain transfers)
- `0x0002` - PENDING (create pending transfer)
- `0x0004` - POST_PENDING_TRANSFER (commit pending)
- `0x0008` - VOID_PENDING_TRANSFER (cancel pending)

### AccountFilter (128 bytes)
```
[0:16]    account_id (uint128)
[16:32]   user_data_128 (uint128) - optional filter
[32:40]   user_data_64 (uint64) - optional filter
[40:44]   user_data_32 (uint32) - optional filter
[44:46]   reserved
[46:48]   code (uint16) - optional filter
[48:56]   timestamp_min (uint64)
[56:64]   timestamp_max (uint64)
[64:68]   limit (uint32)
[68:72]   flags (uint32)
[72:128]  reserved
```

**AccountFilter Flags:**
- `0x01` - DEBITS (include debit transfers)
- `0x02` - CREDITS (include credit transfers)
- `0x04` - REVERSED (reverse sort order)

### AccountBalance (128 bytes)
```
[0:8]     timestamp (uint64)
[8:24]    debits_pending (uint128)
[24:40]   debits_posted (uint128)
[40:56]   credits_pending (uint128)
[56:72]   credits_posted (uint128)
[72:128]  reserved (56 bytes, must be 0)
```

## API Usage

See comprehensive usage examples in:
- **Python**: `tests/functional_tests.py` - Complete implementation with binary encoding helpers
- **Go**: `tests/functional_tests.go` - Full Go implementation with BinaryEncoder

Both demonstrate:
- Account creation and lookup
- Single-phase and two-phase transfers
- Query operations (get_account_transfers, get_account_balances)
- AccountFilter usage (128-byte binary)
- Error handling and sanity checks

### Error Codes

**Note:** Error codes and names now match TigerBeetle's specification exactly. Results are returned in a `result` field (not `error`).

#### CreateAccountsResult

| Code | Error Name | Description |
|------|------------|-------------|
| 0 | ok | Success |
| 1 | linked_event_failed | Linked operation failed |
| 6 | id_must_not_be_zero | Account ID must not be zero |
| 8 | flags_are_mutually_exclusive | Conflicting flags set |
| 9 | debits_pending_must_be_zero | Initial debits_pending must be zero |
| 10 | debits_posted_must_be_zero | Initial debits_posted must be zero |
| 11 | credits_pending_must_be_zero | Initial credits_pending must be zero |
| 12 | credits_posted_must_be_zero | Initial credits_posted must be zero |
| 13 | ledger_must_not_be_zero | Ledger must not be zero |
| 14 | code_must_not_be_zero | Code must not be zero |
| 21 | exists | Account already exists |

#### CreateTransfersResult

| Code | Error Name | Description |
|------|------------|-------------|
| 0 | ok | Success |
| 1 | linked_event_failed | Linked operation failed |
| 5 | id_must_not_be_zero | Transfer ID must not be zero |
| 12 | accounts_must_be_different | Debit and credit accounts are same |
| 19 | ledger_must_not_be_zero | Ledger must not be zero |
| 21 | debit_account_not_found | Debit account not found |
| 22 | credit_account_not_found | Credit account not found |
| 24 | transfer_must_have_the_same_ledger_as_accounts | Ledger mismatch |
| 25 | pending_transfer_not_found | Pending transfer not found |
| 46 | exists | Transfer already exists |
| 54 | exceeds_credits | Transfer exceeds credit limit |
| 55 | exceeds_debits | Transfer exceeds debit limit |

**For complete error code reference, see [TigerBeetle Documentation](https://docs.tigerbeetle.com/)**

## Advanced Usage

### Linked Operations

The `linked` flag creates **chains** of operations that must all succeed or all fail together. This matches TigerBeetle's behavior exactly.

#### How Linked Chains Work

- A chain **starts** when an operation has the `linked` flag set
- The chain **continues** as long as subsequent operations have the `linked` flag
- The chain **ends** when an operation does NOT have the `linked` flag (or the batch ends)
- If ANY operation in a chain fails, the ENTIRE chain is rolled back
- Multiple independent chains can exist in a single batch

#### Examples

**Simple Chain (all-or-nothing):**
```python
accounts = [
    {"id": "1", "ledger": 700, "code": 10, "flags": 0x0001},  # LINKED - chain starts
    {"id": "2", "ledger": 700, "code": 10, "flags": 0}        # NOT linked - chain ends
]
# Both accounts are created, or neither (if any fails)
```

**Multiple Independent Chains:**
```python
accounts = [
    # Chain 1
    {"id": "1", "ledger": 700, "code": 10, "flags": 0x0001},  # LINKED
    {"id": "2", "ledger": 700, "code": 10, "flags": 0},       # NOT linked - Chain 1 ends

    # Chain 2
    {"id": "3", "ledger": 700, "code": 10, "flags": 0x0001},  # LINKED
    {"id": "4", "ledger": 700, "code": 10, "flags": 0},       # NOT linked - Chain 2 ends
]
# Chain 1 and Chain 2 are independent
# If Chain 1 fails, Chain 2 can still succeed (and vice versa)
```

**Long Chain:**
```python
accounts = [
    {"id": "1", "ledger": 700, "code": 10, "flags": 0x0001},  # LINKED
    {"id": "2", "ledger": 700, "code": 10, "flags": 0x0001},  # LINKED (chain continues)
    {"id": "3", "ledger": 700, "code": 10, "flags": 0x0001},  # LINKED (chain continues)
    {"id": "4", "ledger": 700, "code": 10, "flags": 0}        # NOT linked - chain ends
]
# All 4 accounts created atomically, or none
```

**Error: Unclosed Chain:**
```python
accounts = [
    {"id": "1", "ledger": 700, "code": 10, "flags": 0x0001},  # LINKED
    {"id": "2", "ledger": 700, "code": 10, "flags": 0x0001},  # LINKED (still linked!)
]
# ERROR: linked_event_chain_open (2)
# Chain never closed - all operations fail and are rolled back
```

### Balance Constraints

Prevent overdrafts by setting balance constraints:

```python
# Account that cannot have debits exceed credits
account = {
    "id": "1",
    "ledger": 700,
    "code": 10,
    "flags": 2  # DEBITS_MUST_NOT_EXCEED_CREDITS
}
```

## Implementation Notes

## Key Differences from TigerBeetle

1. **Storage Backend**: Redis/DragonflyDB/EloqKV vs. TigerBeetle's custom LSM
2. **Binary Format**: Fixed 128-byte encoding (compatible) but stored differently
3. **Index Strategy**: APPEND-based with query-time processing vs. sorted indexes
4. **Query Performance**: Full scan with filtering vs. range queries
5. **Timestamps**: Redis TIME (microsecond precision) vs. TigerBeetle's timestamps

## Performance Characteristics

### Write Path (Optimized)
- **create_transfer**: O(1) - simple APPEND operations
- **Balance updates**: O(1) - direct field updates
- **Index maintenance**: O(1) - APPEND (no sorting)
- **Rollback**: O(1) - track length, truncate on error

### Read Path (Query-time Processing)
- **get_account_transfers**: O(N) where N = total transfers for account
  - Fetches entire index, parses all entries, filters, sorts
  - Trade-off: Slower queries for faster writes
- **get_account_balances**: O(M) where M = total balance snapshots
  - Only for accounts with HISTORY flag

### Optimization Strategy
- **Critical path** (writes): Minimal work, maximum throughput
- **Non-critical path** (queries): Do all processing here
- **Use case**: Write-heavy workloads (transfers >> queries)

### Atomicity Guarantees
- All Lua script operations are atomic (Redis EVAL semantics)
- No other commands execute during script execution
- Linked transfers use explicit rollback on any error

### Best Practices
1. Use `SCRIPT LOAD` once, reuse SHA across calls
2. Use pipelining for independent operations
3. Use `/mnt/ramdisk/tests/` for test data (avoid disk I/O)
4. Clean data between tests (`FLUSHDB` or restart)

## Testing

### Comprehensive Functional Tests

**Python: `tests/functional_tests.py`** - Complete test coverage (17 tests):

**Go: `tests/functional_tests.go`** - Comprehensive Go tests (11 tests):

```bash
# Python tests
cd tests
python3 functional_tests.py

# Go tests
cd tests
go run functional_tests.go common.go
```

### Performance Tests

**tests/** - Stress test and benchmarks:
- Multiple workload types: transfer, lookup, twophase, mixed
- Hot/cold account modeling
- Configurable workers, batch sizes, duration
- Supports Redis, DragonflyDB, EloqKV, TigerBeetle

```bash
cd tests
go build -o stress_test main.go common.go luabeetle_stress.go tigerbeetle_stress.go
./stress_test -mode=redis -workload=transfer -accounts=10000 -workers=4 -duration=30
```

### Benchmark Suite

Comprehensive benchmark suite (448 test configurations):

```bash
cd tests
./run_benchmarks.sh                    # Run full suite (~2 hours)
./monitor_benchmarks.sh                # Monitor progress
python3 analyze_results.py results/    # Analyze results
```

## Data Storage Schema

### Primary Storage (Binary Format)

- **Accounts**: `account:{16-byte-binary-id}` - Binary blob (128 bytes)
- **Transfers**: `transfer:{hex-id}` - Binary blob (128 bytes)
  - Transfer IDs are stored as hex strings in keys for Redis compatibility

### Secondary Indexes (APPEND-based)

**Design Philosophy**: All sorting and filtering happens at query time (non-critical path). Write path uses simple APPEND for maximum performance.

- **Transfer Index**: `account:{16-byte-binary-id}:transfers`
  - Binary string of concatenated 16-byte transfer IDs
  - Uses `APPEND` for O(1) writes (no sorting at write time)
  - Fixed-size entries enable easy rollback (track length, truncate on error)
  - Query-time: `GET` entire blob, parse chunks, filter, sort

- **Balance History**: `account:{16-byte-binary-id}:balance_history`
  - Binary string of concatenated 128-byte AccountBalance snapshots
  - Only maintained when account has HISTORY flag (0x08)
  - Uses `APPEND` for O(1) writes
  - Query-time: `GET` entire blob, parse 128-byte chunks, filter, sort
  - Each snapshot: timestamp (8) + debits_pending/posted (32) + credits_pending/posted (32) + reserved (56)

### Rollback Strategy

For linked transfers (linked operations):
1. Track original index length with `STRLEN` before first append
2. On error: Use `GETRANGE 0 (original_len-1)` + `SET` to truncate
3. If original length was 0: Use `DEL` to remove key entirely

## License

MIT

## References

- [TigerBeetle Documentation](https://docs.tigerbeetle.com/)
- [Redis Lua Scripting](https://redis.io/docs/manual/programmability/eval-intro/)
