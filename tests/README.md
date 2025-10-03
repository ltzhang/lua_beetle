# Lua Beetle Functional Tests

Comprehensive functional tests for all Lua Beetle operations.

## Test Coverage

Both Python and Go test suites cover:

1. **Account Operations**
   - Create account
   - Duplicate account detection
   - Account lookup

2. **Transfer Operations**
   - Simple transfers between accounts
   - Multiple transfers
   - Pipelined transfers
   - Duplicate transfer detection
   - Nonexistent account error handling

3. **Two-Phase Transfers**
   - Creating pending transfers
   - Posting pending transfers (commit)
   - Voiding pending transfers (cancel)
   - Balance verification (pending vs posted)

4. **Error Handling**
   - Duplicate IDs
   - Missing accounts
   - Invalid operations

## Running Tests

### Python Tests

```bash
# Activate virtual environment
source ~/venv/bin/activate

# Run tests
cd tests
python3 test_functional.py
```

**Expected Output:**
```
============================================================
Lua Beetle Functional Tests
============================================================

=== Test: Create Account ===
✓ Account created and verified

=== Test: Duplicate Account ===
✓ Duplicate account correctly rejected

... (all tests)

============================================================
✅ All tests passed!
============================================================
```

### Go Tests

```bash
cd stress_test
go test -v functional_test.go common.go
```

**Expected Output:**
```
=== RUN   TestCreateAccount
    functional_test.go:197: ✓ Account created successfully
--- PASS: TestCreateAccount (0.01s)
... (all tests)
PASS
ok  	command-line-arguments	0.059s
```

## Prerequisites

### Redis Must Be Running

```bash
# Start Redis
/home/lintaoz/database/redis/src/redis-server --daemonize yes

# Verify
/home/lintaoz/database/redis/src/redis-cli ping  # Should return PONG
```

### Python Requirements

```bash
# Install dependencies (in virtual environment)
source ~/venv/bin/activate
pip install redis
```

### Go Requirements

Tests run in the `stress_test` directory where all dependencies are already set up.

## Test Details

### Python Test Suite (`test_functional.py`)

- **Lines**: ~450
- **Tests**: 9 test functions
- **Features**:
  - Binary encoding/decoding
  - Direct Lua script invocation
  - Detailed assertions
  - Comprehensive error checking

### Go Test Suite (`functional_test.go`)

- **Lines**: ~500
- **Tests**: 10 test functions
- **Features**:
  - Test fixtures with setup/teardown
  - Type-safe binary encoding
  - Pipeline testing
  - Idiomatic Go testing patterns

## Known Limitations

1. **Transfer Lookup Test**: Currently commented out in Python tests (lookup_transfer script needs verification)

2. **Duplicate Transfer Detection**: Returns error code 29 (timestamp invalid) instead of 21 (ID already exists), but correctly detects duplicates

## Test Architecture

Both test suites use the **binary encoding format** (128-byte fixed-size):

```
Account (128 bytes):
  [0:16]   ID (uint128)
  [16:32]  debits_pending (uint128)
  [32:48]  debits_posted (uint128)
  [48:64]  credits_pending (uint128)
  [64:80]  credits_posted (uint128)
  ...
  [112:116] ledger (uint32)
  [116:118] code (uint16)
  [118:120] flags (uint16)

Transfer (128 bytes):
  [0:16]   ID (uint128)
  [16:32]  debit_account_id (uint128)
  [32:48]  credit_account_id (uint128)
  [48:64]  amount (uint128)
  [64:80]  pending_id (uint128)
  ...
  [112:116] ledger (uint32)
  [116:118] code (uint16)
  [118:120] flags (uint16)
```

## Error Codes

Tests verify TigerBeetle-compatible error codes:

- `0` - Success
- `21` - ID already exists
- `34` - Pending transfer not found
- `38` - Debit account not found
- `39` - Credit account not found
- `40` - Accounts must be different
- `42` - Exceeds credits
- `43` - Exceeds debits

## Adding New Tests

### Python

```python
def test_new_feature():
    """Test description."""
    print("\n=== Test: New Feature ===")

    # Setup
    account_data = encode_account(100, 700, 10, 0)
    r.evalsha(create_account_sha, 0, account_data)

    # Test
    result = ...

    # Assertions
    assert_equal(actual, expected, "Message")

    print("✓ Test passed")

# Add to main():
test_new_feature()
```

### Go

```go
func TestNewFeature(t *testing.T) {
    f := setupTest(t)
    defer f.cleanup()

    // Setup
    f.createAccount(t, 100, 700, 10, 0)

    // Test
    errCode := ...

    // Assertions
    if errCode != ErrOK {
        t.Fatalf("Expected success, got error %d", errCode)
    }

    t.Log("✓ Test passed")
}
```

## Debugging

### Enable Verbose Output

Python:
```python
# Uncomment debug line in decode_result()
print(f"DEBUG: result type={type(result)}, value={result!r}")
```

Go:
```go
// Add verbose flag
go test -v -run TestName functional_test.go common.go
```

### Check Redis State

```bash
# Monitor Redis commands
redis-cli MONITOR

# Check keys
redis-cli KEYS '*'

# Inspect account
redis-cli GET account:1 | xxd
```

## Continuous Integration

These tests should be run:
- Before committing changes
- After modifying Lua scripts
- After changing encoding formats
- As part of CI/CD pipeline

## Test Execution Time

- **Python**: ~1-2 seconds
- **Go**: ~0.06 seconds (faster due to compiled nature)

Both suites flush the database before running, ensuring clean state.
