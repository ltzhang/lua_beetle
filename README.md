# Lua Beetle

A Redis Lua implementation of TigerBeetle's core financial transaction APIs. Lua Beetle provides functionally equivalent operations for account management and atomic transfers using Redis as the storage backend.

## Features

- ✅ **Account Management**: Create and lookup accounts with balance tracking
- ✅ **Atomic Transfers**: Single-phase and two-phase (pending/post/void) transfers
- ✅ **Double-Entry Bookkeeping**: Maintains debit/credit balance invariants
- ✅ **Balance Constraints**: Support for overdraft protection and balance limits
- ✅ **Linked Operations**: All-or-nothing atomic batches
- ✅ **Transaction Safety**: Rollback on errors in linked operations
- ✅ **Account Transfers Query**: Get all transfers for an account with timestamp filtering
- ✅ **Balance History**: Track balance changes over time (when HISTORY flag is set)

## Project Structure

```
lua_beetle/
├── scripts/
│   ├── utils.lua                  # Shared utilities and constants
│   ├── create_accounts.lua        # Account creation script
│   ├── create_transfers.lua       # Transfer creation script
│   ├── lookup_accounts.lua        # Account lookup script
│   ├── lookup_transfers.lua       # Transfer lookup script
│   ├── get_account_transfers.lua  # Get all transfers for an account
│   └── get_account_balances.lua   # Get balance history for an account
├── tests/
│   └── test_basic.py              # Test suite
└── README.md
```

## Installation

1. Install Redis (version 5.0+)
2. Install Python redis client: `pip install redis`

## Quick Start

### 1. Start Redis

```bash
redis-server
```

### 2. Run Tests

```bash
cd tests
python3 test_basic.py
```

## API Documentation

### Data Structures

#### Account

```json
{
  "id": "1",
  "ledger": 700,
  "code": 10,
  "flags": 0,
  "debits_pending": "0",
  "debits_posted": "0",
  "credits_pending": "0",
  "credits_posted": "0",
  "user_data_128": "",
  "user_data_64": "",
  "user_data_32": "",
  "timestamp": "1234567890000000000"
}
```

**Required Fields:**
- `id`: Unique account identifier (non-zero string)
- `ledger`: Ledger identifier (non-zero integer)
- `code`: Account type code (non-zero integer)

**Account Flags (matching TigerBeetle):**
- `0x0001` (1): `linked` - Chain account creation (all-or-nothing)
- `0x0002` (2): `debits_must_not_exceed_credits` - Prevent overdrafts
- `0x0004` (4): `credits_must_not_exceed_debits` - Prevent negative balances
- `0x0008` (8): `history` - Retain balance history
- `0x0010` (16): `imported` - Allow importing historical accounts
- `0x0020` (32): `closed` - Prevent further transfers

#### Transfer

```json
{
  "id": "1",
  "debit_account_id": "1",
  "credit_account_id": "2",
  "amount": 100,
  "ledger": 700,
  "code": 10,
  "flags": 0,
  "pending_id": "",
  "timeout": 0,
  "user_data_128": "",
  "user_data_64": "",
  "user_data_32": "",
  "timestamp": "1234567890000000000"
}
```

**Required Fields:**
- `id`: Unique transfer identifier (non-zero string)
- `debit_account_id`: Account to debit
- `credit_account_id`: Account to credit
- `amount`: Transfer amount (positive integer)
- `ledger`: Ledger identifier (must match accounts)

**Transfer Flags (matching TigerBeetle):**
- `0x0001` (1): `linked` - Chain transfer outcomes
- `0x0002` (2): `pending` - Create pending transfer
- `0x0004` (4): `post_pending_transfer` - Post a pending transfer
- `0x0008` (8): `void_pending_transfer` - Void a pending transfer
- `0x0010` (16): `balancing_debit` - Adjust based on debit constraints
- `0x0020` (32): `balancing_credit` - Adjust based on credit constraints
- `0x0040` (64): `closing_debit` - Close debit account
- `0x0080` (128): `closing_credit` - Close credit account
- `0x0100` (256): `imported` - Allow importing historical transfers

### Operations

#### create_accounts

Create one or more accounts atomically.

**Python Example:**

```python
import redis
import json

r = redis.Redis(decode_responses=True)

# Load script
with open('scripts/create_accounts.lua', 'r') as f:
    script = r.script_load(f.read())

# Create accounts
accounts = [
    {"id": "1", "ledger": 700, "code": 10, "flags": 0},
    {"id": "2", "ledger": 700, "code": 10, "flags": 0}
]

result = r.evalsha(script, 0, json.dumps(accounts))
print(json.loads(result))
```

**Output:**

```json
[
  {"index": 0, "error": 0},
  {"index": 1, "error": 0}
]
```

#### create_transfers

Create one or more transfers atomically.

**Single-Phase Transfer:**

```python
transfers = [{
    "id": "1",
    "debit_account_id": "1",
    "credit_account_id": "2",
    "amount": 100,
    "ledger": 700,
    "code": 10,
    "flags": 0
}]

result = r.evalsha(transfer_script, 0, json.dumps(transfers))
```

**Two-Phase Transfer (Pending → Post):**

```python
# Step 1: Create pending transfer
pending = [{
    "id": "1",
    "debit_account_id": "1",
    "credit_account_id": "2",
    "amount": 100,
    "ledger": 700,
    "code": 10,
    "flags": 2  # PENDING
}]

r.evalsha(transfer_script, 0, json.dumps(pending))

# Step 2: Post the pending transfer
post = [{
    "id": "2",
    "pending_id": "1",
    "debit_account_id": "1",
    "credit_account_id": "2",
    "amount": 100,
    "ledger": 700,
    "code": 10,
    "flags": 4  # POST_PENDING_TRANSFER
}]

r.evalsha(transfer_script, 0, json.dumps(post))
```

**Two-Phase Transfer (Pending → Void):**

```python
# Void instead of post
void = [{
    "id": "2",
    "pending_id": "1",
    "debit_account_id": "1",
    "credit_account_id": "2",
    "amount": 100,
    "ledger": 700,
    "code": 10,
    "flags": 8  # VOID_PENDING_TRANSFER
}]

r.evalsha(transfer_script, 0, json.dumps(void))
```

#### lookup_accounts

Lookup accounts by IDs.

```python
with open('scripts/lookup_accounts.lua', 'r') as f:
    lookup_script = r.script_load(f.read())

result = r.evalsha(lookup_script, 0, json.dumps(["1", "2"]))
accounts = json.loads(result)
print(accounts)
```

#### lookup_transfers

Lookup transfers by IDs.

```python
with open('scripts/lookup_transfers.lua', 'r') as f:
    lookup_script = r.script_load(f.read())

result = r.evalsha(lookup_script, 0, json.dumps(["1", "2"]))
transfers = json.loads(result)
print(transfers)
```

#### get_account_transfers

Get all transfers for an account, ordered by timestamp.

**Arguments:**
- `ARGV[1]`: account_id (required)
- `ARGV[2]`: timestamp_min (optional, default: -inf)
- `ARGV[3]`: timestamp_max (optional, default: +inf)
- `ARGV[4]`: limit (optional, default: -1 for all)

```python
with open('scripts/get_account_transfers.lua', 'r') as f:
    get_transfers_script = r.script_load(f.read())

# Get all transfers for account "1"
result = r.evalsha(get_transfers_script, 0, "1")
transfers = json.loads(result)

# Get transfers with timestamp range and limit
result = r.evalsha(get_transfers_script, 0, "1", "1000000000", "2000000000", "10")
transfers = json.loads(result)
```

**Output:**
```json
[
  {
    "id": "100",
    "debit_account_id": "1",
    "credit_account_id": "2",
    "amount": "100",
    "timestamp": "1234567890000000000",
    ...
  }
]
```

#### get_account_balances

Get current balance and history (if HISTORY flag is set) for an account.

**Arguments:**
- `ARGV[1]`: account_id (required)
- `ARGV[2]`: timestamp_min (optional, default: -inf)
- `ARGV[3]`: timestamp_max (optional, default: +inf)
- `ARGV[4]`: limit (optional, default: -1 for all)

```python
with open('scripts/get_account_balances.lua', 'r') as f:
    get_balances_script = r.script_load(f.read())

# Get balance for account "1"
result = r.evalsha(get_balances_script, 0, "1")
balance_data = json.loads(result)
```

**Output (without HISTORY flag):**
```json
{
  "account_id": "1",
  "current_balance": {
    "debits_pending": "0",
    "debits_posted": "150",
    "credits_pending": "0",
    "credits_posted": "50",
    "timestamp": "1234567890000000000"
  },
  "history": []
}
```

**Output (with HISTORY flag):**
```json
{
  "account_id": "1",
  "current_balance": {
    "debits_pending": "0",
    "debits_posted": "150",
    "credits_pending": "0",
    "credits_posted": "50",
    "timestamp": "1234567890000000000"
  },
  "history": [
    {
      "timestamp": 1234567890000000000,
      "debits_pending": 0,
      "debits_posted": 100,
      "credits_pending": 0,
      "credits_posted": 50,
      "transfer_id": "1"
    },
    {
      "timestamp": 1234567891000000000,
      "debits_pending": 0,
      "debits_posted": 150,
      "credits_pending": 0,
      "credits_posted": 50,
      "transfer_id": "2"
    }
  ]
}
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

Use the LINKED flag to ensure all operations in a batch succeed or fail together:

```python
# All accounts created atomically or none
accounts = [
    {"id": "1", "ledger": 700, "code": 10, "flags": 1},  # LINKED
    {"id": "2", "ledger": 700, "code": 10, "flags": 1},  # LINKED
    {"id": "3", "ledger": 700, "code": 10, "flags": 0}
]
```

If any account fails, all previous accounts in the linked chain are rolled back.

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

### Differences from TigerBeetle

1. **Storage**: Uses Redis instead of custom database
2. **IDs**: Uses strings instead of 128-bit integers
3. **Timestamps**: Uses Redis TIME command (microsecond precision)
4. **Simplified Features**: Some advanced features not implemented:
   - Query operations
   - Account history tracking
   - Timeout enforcement (manual cleanup required)
   - Currency exchange helpers
   - Rate limiting

### Atomicity Guarantees

- All operations within a single Lua script are atomic
- Redis EVAL ensures no other commands execute during script execution
- Linked operations use explicit rollback on error

### Performance Considerations

- Script loading: Use `SCRIPT LOAD` once and reuse SHA
- Batch operations: Create multiple accounts/transfers in one call
- Connection pooling: Reuse Redis connections

## Testing

The test suite covers:
- ✅ Account creation and validation
- ✅ Single-phase transfers
- ✅ Two-phase transfers (pending/post/void)
- ✅ Linked operations and rollback
- ✅ Balance constraints
- ✅ Lookup operations
- ✅ Account transfers query
- ✅ Balance history tracking
- ✅ Error handling

Run tests:
```bash
cd tests
python3 test_basic.py
```

## Data Storage Schema

### Primary Storage

- **Accounts**: `account:{id}` - Redis hash containing account data
- **Transfers**: `transfer:{id}` - Redis hash containing transfer data

### Secondary Indexes

- **Transfer Index**: `account:{id}:transfers` - Sorted set (score=timestamp, member=transfer_id)
  - Enables efficient `get_account_transfers` queries
  - Automatically maintained by `create_transfers.lua`

- **Balance History**: `account:{id}:balance_history` - Sorted set (score=timestamp, member=JSON balance snapshot)
  - Only created when account has HISTORY flag (flag=8)
  - Stores complete balance state after each transfer
  - Enables time-series balance queries

## License

MIT

## References

- [TigerBeetle Documentation](https://docs.tigerbeetle.com/)
- [Redis Lua Scripting](https://redis.io/docs/manual/programmability/eval-intro/)
