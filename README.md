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

**Account Flags:**
- `0`: None
- `1`: LINKED - Chain account creation (all-or-nothing)
- `2`: DEBITS_MUST_NOT_EXCEED_CREDITS - Prevent overdrafts
- `4`: CREDITS_MUST_NOT_EXCEED_DEBITS - Prevent negative balances
- `8`: HISTORY - Retain balance history
- `16`: CLOSED - Prevent further transfers

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

**Transfer Flags:**
- `0`: None (single-phase immediate transfer)
- `1`: LINKED - Chain transfer outcomes
- `2`: PENDING - Create pending transfer
- `4`: POST_PENDING_TRANSFER - Post a pending transfer
- `8`: VOID_PENDING_TRANSFER - Void a pending transfer
- `16`: BALANCING_DEBIT - Adjust based on debit constraints
- `32`: BALANCING_CREDIT - Adjust based on credit constraints
- `64`: CLOSING_DEBIT - Close debit account
- `128`: CLOSING_CREDIT - Close credit account

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

| Code | Error | Description |
|------|-------|-------------|
| 0 | OK | Success |
| 1 | ACCOUNT_EXISTS | Account ID already exists |
| 2 | ACCOUNT_NOT_FOUND | Account not found |
| 3 | ACCOUNT_INVALID_ID | Invalid account ID |
| 4 | ACCOUNT_INVALID_LEDGER | Invalid ledger value |
| 5 | ACCOUNT_INVALID_CODE | Invalid code value |
| 7 | ACCOUNT_BALANCES_NOT_ZERO | Initial balances must be zero |
| 8 | ACCOUNT_CLOSED | Account is closed |
| 9 | TRANSFER_EXISTS | Transfer ID already exists |
| 10 | TRANSFER_INVALID_ID | Invalid transfer ID |
| 11 | TRANSFER_INVALID_DEBIT_ACCOUNT | Debit account not found |
| 12 | TRANSFER_INVALID_CREDIT_ACCOUNT | Credit account not found |
| 13 | TRANSFER_ACCOUNTS_SAME | Debit and credit accounts are same |
| 14 | TRANSFER_INVALID_AMOUNT | Amount must be positive |
| 15 | TRANSFER_INVALID_LEDGER | Invalid ledger value |
| 17 | TRANSFER_LEDGER_MISMATCH | Account ledgers don't match |
| 18 | TRANSFER_EXCEEDS_CREDITS | Transfer would exceed credit limit |
| 19 | TRANSFER_EXCEEDS_DEBITS | Transfer would exceed debit limit |
| 20 | TRANSFER_PENDING_NOT_FOUND | Pending transfer not found |
| 22 | LINKED_EVENT_FAILED | Linked operation failed |

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
