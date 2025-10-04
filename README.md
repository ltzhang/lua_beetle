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
│   ├── create_transfer.lua          # Create 1 transfer (binary 128-byte)
│   ├── create_chained_transfers.lua # Create linked transfers with rollback
│   ├── lookup_account.lua           # Lookup 1 account by ID
│   ├── lookup_transfer.lua          # Lookup 1 transfer by ID
│   ├── get_account_transfers.lua    # Query transfers with AccountFilter (128-byte)
│   └── get_account_balances.lua     # Query balance history (64-byte snapshots)
├── stress_test/
│   ├── stress_test.go               # Go-based stress test suite
│   ├── functional_test.go           # Functional tests
│   ├── run_benchmarks.sh            # Comprehensive benchmark suite
│   └── analyze_results.py           # Results analysis
├── tests/
│   ├── test_functional.py           # Python functional tests
│   └── test_query_functions.py      # Tests for query operations
└── README.md
```

## Installation

### Prerequisites
- Redis 5.0+ or compatible backend (DragonflyDB, EloqKV, TigerBeetle)
- Python 3.8+ with `redis` client: `pip install redis`
- Go 1.19+ (for stress tests)

### Quick Start

```bash
# 1. Start Redis with data in ramdisk
mkdir -p /mnt/ramdisk/tests
cd /mnt/ramdisk/tests
redis-server --dir /mnt/ramdisk/tests --daemonize yes

# 2. Run functional tests
cd /home/lintaoz/work/lua_beetle/tests
python3 test_functional.py
python3 test_query_functions.py

# 3. Run stress tests
cd /home/lintaoz/work/lua_beetle/stress_test
go build
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

### AccountBalance (64 bytes)
```
[0:8]     timestamp (uint64)
[8:24]    debits_pending (uint128)
[24:40]   debits_posted (uint128)
[40:56]   credits_pending (uint128)
[56:64]   credits_posted (uint128)
```

## API Usage

### Basic Operations

```python
import redis
from encoder import BinaryEncoder

client = redis.Redis(decode_responses=False)
encoder = BinaryEncoder()

# Load scripts
with open('scripts/create_account.lua', 'r') as f:
    create_account_sha = client.script_load(f.read())

with open('scripts/create_transfer.lua', 'r') as f:
    create_transfer_sha = client.script_load(f.read())

# Create account (binary 128 bytes)
account_data = encoder.encode_account(
    account_id=1,
    ledger=700,
    code=10,
    flags=0x0008  # HISTORY flag
)
result = client.evalsha(create_account_sha, 0, account_data)

# Create transfer (binary 128 bytes)
transfer_data = encoder.encode_transfer(
    transfer_id="tx_1",
    debit_account_id=1,
    credit_account_id=2,
    amount=500,
    ledger=700,
    code=10,
    flags=0
)
result = client.evalsha(create_transfer_sha, 0, transfer_data)
```

### Two-Phase Transfers

```python
# 1. Create pending transfer
pending_data = encoder.encode_transfer(
    transfer_id="pending_1",
    debit_account_id=1,
    credit_account_id=2,
    amount=500,
    ledger=700,
    code=10,
    flags=0x0002  # PENDING
)
client.evalsha(create_transfer_sha, 0, pending_data)

# 2. Post pending transfer
post_data = encoder.encode_transfer_with_pending(
    transfer_id="post_1",
    debit_account_id=1,
    credit_account_id=2,
    amount=500,
    pending_id="pending_1",
    ledger=700,
    code=10,
    flags=0x0004  # POST_PENDING_TRANSFER
)
client.evalsha(create_transfer_sha, 0, post_data)

# OR: Void pending transfer
void_data = encoder.encode_transfer_with_pending(
    transfer_id="void_1",
    debit_account_id=1,
    credit_account_id=2,
    amount=500,
    pending_id="pending_1",
    ledger=700,
    code=10,
    flags=0x0008  # VOID_PENDING_TRANSFER
)
client.evalsha(create_transfer_sha, 0, void_data)
```

### Query Operations

```python
# Load query scripts
with open('scripts/get_account_transfers.lua', 'r') as f:
    get_transfers_sha = client.script_load(f.read())

with open('scripts/get_account_balances.lua', 'r') as f:
    get_balances_sha = client.script_load(f.read())

# Get account transfers with filter
account_filter = encoder.encode_account_filter(
    account_id=1,
    timestamp_min=0,
    timestamp_max=2**64 - 1,
    limit=10,
    flags=0x03  # DEBITS | CREDITS
)
transfers_blob = client.evalsha(get_transfers_sha, 0, account_filter)

# Parse results (each transfer is 128 bytes)
num_transfers = len(transfers_blob) // 128
for i in range(num_transfers):
    transfer = encoder.decode_transfer(transfers_blob[i*128:(i+1)*128])
    print(transfer)

# Get account balance history
balances_blob = client.evalsha(get_balances_sha, 0, account_filter)

# Parse results (each balance is 64 bytes)
num_balances = len(balances_blob) // 64
for i in range(num_balances):
    balance = encoder.decode_account_balance(balances_blob[i*64:(i+1)*64])
    print(balance)
```

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

### Functional Tests (Python)

**test_functional.py** - Core functionality:
- ✅ Account creation and validation
- ✅ Single-phase transfers
- ✅ Two-phase transfers (pending/post/void)
- ✅ Linked operations and rollback
- ✅ Balance constraints
- ✅ Error handling

**test_query_functions.py** - Query operations:
- ✅ get_account_transfers (basic, debits-only, credits-only, with limit)
- ✅ get_account_balances (basic, HISTORY flag handling)
- ✅ AccountFilter support (binary 128-byte format)
- ✅ Binary encoding/decoding

```bash
cd tests
python3 test_functional.py
python3 test_query_functions.py
```

### Stress Tests (Go)

**functional_test.go** - Go functional tests:
- Account creation, transfer creation, two-phase transfers
- Concurrent operations

**stress_test.go** - Performance testing:
- Multiple workload types: transfer, lookup, twophase, mixed
- Hot/cold account modeling
- Configurable workers, batch sizes, duration
- Supports Redis, DragonflyDB, EloqKV, TigerBeetle

```bash
cd stress_test
go test -v functional_test.go common.go
go build
./stress_test -mode=redis -workload=transfer -accounts=10000 -workers=4 -duration=30
```

### Benchmark Suite

Comprehensive benchmark suite (192 test configurations):

```bash
cd stress_test
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
  - Binary string of concatenated 64-byte AccountBalance snapshots
  - Only maintained when account has HISTORY flag (0x08)
  - Uses `APPEND` for O(1) writes
  - Query-time: `GET` entire blob, parse 64-byte chunks, filter, sort
  - Each snapshot: timestamp + debits_pending/posted + credits_pending/posted

### Rollback Strategy

For linked transfers (chained operations):
1. Track original index length with `STRLEN` before first append
2. On error: Use `GETRANGE 0 (original_len-1)` + `SET` to truncate
3. If original length was 0: Use `DEL` to remove key entirely

## License

MIT

## References

- [TigerBeetle Documentation](https://docs.tigerbeetle.com/)
- [Redis Lua Scripting](https://redis.io/docs/manual/programmability/eval-intro/)
