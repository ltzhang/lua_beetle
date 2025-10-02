# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Lua Beetle is a Redis Lua implementation of TigerBeetle's core financial transaction APIs. It provides functionally equivalent operations for account management and atomic transfers using Redis as the storage backend, matching TigerBeetle's exact error codes, flags, and semantics.

## Build and Test Commands

### Basic Tests

```bash
# Start Redis (required)
cd third_party
./redis-server --daemonize yes

# Run Python tests
cd tests
source ~/venv/bin/activate  # Activate Python virtual environment
python3 test_basic.py

# Tests should show: ✅ All tests passed!
```

### Stress Tests

```bash
# Build stress test
cd stress_test
go build -o stress_test

# Run Redis stress test
./stress_test -mode=redis -accounts=1000 -workers=4 -duration=10

# Run comparison (Redis + TigerBeetle)
./stress_test -mode=all -accounts=10000 -workers=10 -duration=30

# Run DragonflyDB test (requires dragonfly-x86_64 running on port 6380)
./stress_test -mode=dragonfly -accounts=1000 -workers=4 -duration=10
```

### Start Required Services

#### Redis (Required - Port 6379)
```bash
cd third_party
./redis-server --daemonize yes

# Verify it's running
redis-cli ping  # Should return: PONG

# Stop Redis
redis-cli shutdown

# Check if running
pgrep redis-server
```

#### DragonflyDB (Optional - Port 6380)
DragonflyDB is a Redis-compatible alternative with better performance for some workloads.

**Important**: Must use `--default_lua_flags=allow-undeclared-keys` because Lua Beetle scripts access keys dynamically.

```bash
cd third_party

# Start DragonflyDB
./dragonfly-x86_64 --logtostderr --port=6380 --default_lua_flags=allow-undeclared-keys > dragonfly.log 2>&1 &

# Verify it's running
redis-cli -p 6380 ping  # Should return: PONG

# Stop DragonflyDB
killall dragonfly-x86_64

# Check if running
pgrep -f dragonfly
```

#### TigerBeetle (Optional - Port 3000)
TigerBeetle is used for comparison testing in stress tests.

```bash
cd third_party

# First time: Format the database file
./tigerbeetle format --cluster=0 --replica=0 --replica-count=1 --development ./0_0.tigerbeetle

# Start TigerBeetle server
./tigerbeetle start --addresses=3000 --development ./0_0.tigerbeetle > tigerbeetle.log 2>&1 &

# Verify it's running
pgrep tigerbeetle

# Stop TigerBeetle
killall tigerbeetle

# Clean up (removes all data)
rm -f 0_0.tigerbeetle tigerbeetle.log

# To restart with fresh data:
killall tigerbeetle
rm -f 0_0.tigerbeetle tigerbeetle.log
./tigerbeetle format --cluster=0 --replica=0 --replica-count=1 --development ./0_0.tigerbeetle
./tigerbeetle start --addresses=3000 --development ./0_0.tigerbeetle > tigerbeetle.log 2>&1 &
```

**Note**: TigerBeetle has an immutable ledger. To reset data, you must stop the server, delete the data file, reformat, and restart.

## Critical Architecture Principles

### 1. Single Operations vs Chained Operations

**This is the most important architectural distinction:**

- **Single operation scripts** (`create_account.lua`, `create_transfer.lua`, `lookup_account.lua`, etc.):
  - Handle ONE operation at a time
  - Take a JSON object (not an array) as input
  - Explicitly REJECT the `linked` flag (return error if set)
  - Used with Redis **pipelining** for performance
  - Independent failures don't affect other operations in the pipeline

- **Chained operation scripts** (`create_chained_accounts.lua`, `create_chained_transfers.lua`):
  - Handle ARRAYS of operations with `linked` flags
  - Implement transactional semantics (all-or-nothing for chains)
  - Support multiple independent chains in one batch
  - Roll back entire chain on any failure within that chain

**Key insight**: Batching ≠ Transactions
- Batching is for **performance** (client-side pipelining)
- Transactions are only for **explicitly linked** operations via the `linked` flag
- Never batch independent operations in a single script call - use Redis pipelining instead

### 2. Linked Chain Semantics (Matching TigerBeetle Exactly)

A linked chain works as follows:
- Chain **starts** when an operation has `flags & 0x0001` (LINKED) set
- Chain **continues** while subsequent operations have LINKED flag
- Chain **ends** when an operation does NOT have LINKED flag
- If ANY operation in a chain fails, the ENTIRE chain is rolled back
- Multiple **independent** chains can exist in one batch

Example:
```
[op1 LINKED, op2 NOT_LINKED, op3 LINKED, op4 LINKED, op5 NOT_LINKED]
Chain 1: [op1, op2] - atomic
Chain 2: [op3, op4, op5] - atomic
Chain 1 and Chain 2 are independent of each other
```

### 3. Error Codes and Response Format

- ALL error codes match TigerBeetle's `CreateAccountsResult` and `CreateTransfersResult` enums exactly
- Response field is called `result` (NOT `error`)
- Error code `0` means success
- Use hex notation for flags: `0x0001`, `0x0002`, `0x0004`, etc.

### 4. Data Storage Schema

**Primary Storage:**
- Accounts: `account:{id}` - Redis hash
- Transfers: `transfer:{id}` - Redis hash

**Secondary Indexes (maintained automatically):**
- Transfer index: `account:{id}:transfers` - Sorted set (score=timestamp)
- Balance history: `account:{id}:balance_history` - Sorted set (only if HISTORY flag set)

## Script Architecture

```
scripts/
├── Single Operation Scripts (for pipelining):
│   ├── create_account.lua         # Create 1 account, reject LINKED
│   ├── create_transfer.lua        # Create 1 transfer, reject LINKED
│   ├── lookup_account.lua         # Lookup 1 account by ID
│   ├── lookup_transfer.lua        # Lookup 1 transfer by ID
│   ├── get_account_transfers.lua  # Get transfers for 1 account
│   └── get_account_balances.lua   # Get balance history for 1 account
│
├── Chained Operation Scripts (for transactions):
│   ├── create_chained_accounts.lua  # Array input, support LINKED chains
│   └── create_chained_transfers.lua # Array input, support LINKED chains
│
└── Legacy (kept for convenience):
    ├── lookup_accounts.lua        # Batch lookup (array input, no LINKED)
    └── lookup_transfers.lua       # Batch lookup (array input, no LINKED)
```

## Client Usage Pattern

### Python Example (with pipelining)

```python
import redis, json

r = redis.Redis(decode_responses=True)

# Load single operation scripts
with open('scripts/create_account.lua') as f:
    create_account_sha = r.script_load(f.read())

# Create multiple independent accounts using pipeline
pipe = r.pipeline()
for i in range(1000):
    account = {"id": str(i), "ledger": 700, "code": 10, "flags": 0}
    pipe.evalsha(create_account_sha, 0, json.dumps(account))
results = pipe.execute()

# For linked operations, use chained script
with open('scripts/create_chained_accounts.lua') as f:
    create_chained_sha = r.script_load(f.read())

linked_accounts = [
    {"id": "1", "ledger": 700, "code": 10, "flags": 0x0001},  # LINKED
    {"id": "2", "ledger": 700, "code": 10, "flags": 0}        # Chain ends
]
result = r.evalsha(create_chained_sha, 0, json.dumps(linked_accounts))
```

### Go Example (stress test pattern)

```go
// Setup: Create accounts using pipeline
pipe := client.Pipeline()
for i := 0; i < numAccounts; i++ {
    account := map[string]interface{}{
        "id": fmt.Sprintf("%d", i+1),
        "ledger": 700,
        "code": 10,
        "flags": 0,
    }
    accountJSON, _ := json.Marshal(account)
    pipe.EvalSha(ctx, createAccountSHA, []string{}, accountJSON)
}
pipe.Exec(ctx)

// Runtime: Create transfers using pipeline
pipe := client.Pipeline()
for i := 0; i < batchSize; i++ {
    transfer := map[string]interface{}{
        "id": generateID(),
        "debit_account_id": fmt.Sprintf("%d", debitID),
        "credit_account_id": fmt.Sprintf("%d", creditID),
        "amount": 100,
        "ledger": 700,
        "code": 10,
        "flags": 0,  // Independent operation
    }
    transferJSON, _ := json.Marshal(transfer)
    pipe.EvalSha(ctx, createTransferSHA, []string{}, transferJSON)
}
results, _ := pipe.Exec(ctx)
```

## Common Debugging Practices (from user preferences)

When debugging:
1. Add intrusive sanity checks directly in Lua scripts to verify constraints
2. Insert debug print statements liberally to track variable changes
3. Check consistency immediately after operations (e.g., verify balances match expected)
4. Always crash/exit tests immediately on unexpected behavior - never continue on errors
5. Remove all debug code and verbose prints after debugging is complete

## Testing Strategy

Tests MUST:
- Exit immediately on any failure (no continuing after errors)
- Verify consistency at each step
- Test both single operations (with pipelining) and chained operations separately
- Verify that single operation scripts reject LINKED flags
- Verify that chained operations properly roll back on failure
- Test multiple independent chains in one batch

## Performance Considerations

1. **Use script loading**: Always use `SCRIPT LOAD` + `EVALSHA`, never `EVAL`
2. **Pipeline independent operations**: Don't batch in Lua, use Redis pipelining
3. **Batch sizes**:
   - Redis pipelining: 1000 operations is typical
   - TigerBeetle: up to 8189 operations
4. **Connection reuse**: Reuse Redis connections across operations

## Key Differences from TigerBeetle

- Uses Redis strings for IDs (not 128-bit integers)
- Uses Redis TIME for timestamps (nanosecond precision)
- No built-in timeout enforcement (must be handled externally)
- Secondary indexes are manually maintained (not automatic)
- DragonflyDB requires `--default_lua_flags=allow-undeclared-keys` flag

## Common Pitfalls

1. ❌ **Don't batch independent operations in a single script** - use pipelining
2. ❌ **Don't use batch scripts for single operations** - use single operation scripts
3. ❌ **Don't allow LINKED flag in single operation scripts** - must reject with error
4. ❌ **Don't assume batch = transaction** - only LINKED operations are transactional
5. ❌ **Don't forget to maintain secondary indexes** in create_transfer operations

## Reference Documentation

- TigerBeetle docs: https://docs.tigerbeetle.com/
- Redis Lua scripting: https://redis.io/docs/manual/programmability/eval-intro/
- Error codes reference: Match TigerBeetle's CreateAccountsResult and CreateTransfersResult exactly
