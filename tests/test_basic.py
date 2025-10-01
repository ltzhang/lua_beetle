#!/usr/bin/env python3
"""
Basic test suite for Lua Beetle
Tests account creation, transfers, and lookups
"""

import json
import redis
import sys

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Load Lua scripts
with open('../scripts/create_accounts.lua', 'r') as f:
    create_accounts_script = f.read()

with open('../scripts/create_transfers.lua', 'r') as f:
    create_transfers_script = f.read()

with open('../scripts/lookup_accounts.lua', 'r') as f:
    lookup_accounts_script = f.read()

with open('../scripts/lookup_transfers.lua', 'r') as f:
    lookup_transfers_script = f.read()

with open('../scripts/get_account_transfers.lua', 'r') as f:
    get_account_transfers_script = f.read()

with open('../scripts/get_account_balances.lua', 'r') as f:
    get_account_balances_script = f.read()

# Register scripts
create_accounts_sha = r.script_load(create_accounts_script)
create_transfers_sha = r.script_load(create_transfers_script)
lookup_accounts_sha = r.script_load(lookup_accounts_script)
lookup_transfers_sha = r.script_load(lookup_transfers_script)
get_account_transfers_sha = r.script_load(get_account_transfers_script)
get_account_balances_sha = r.script_load(get_account_balances_script)

def cleanup():
    """Clean up test data"""
    r.flushdb()

def test_create_single_account():
    """Test creating a single account"""
    cleanup()
    print("Test: Create single account...")

    accounts = [{
        "id": "1",
        "ledger": 700,
        "code": 10,
        "flags": 0
    }]

    result = r.evalsha(create_accounts_sha, 0, json.dumps(accounts))
    results = json.loads(result)

    assert len(results) == 1, f"Expected 1 result, got {len(results)}"
    assert results[0]['error'] == 0, f"Expected error 0, got {results[0]['error']}"

    # Verify account exists
    account_data = r.hgetall("account:1")
    assert account_data['id'] == "1"
    assert account_data['ledger'] == "700"
    assert account_data['code'] == "10"
    assert account_data['debits_posted'] == "0"
    assert account_data['credits_posted'] == "0"

    print("✓ Passed")

def test_create_duplicate_account():
    """Test that creating a duplicate account fails"""
    cleanup()
    print("Test: Create duplicate account...")

    accounts = [{
        "id": "1",
        "ledger": 700,
        "code": 10,
        "flags": 0
    }]

    # Create first account
    r.evalsha(create_accounts_sha, 0, json.dumps(accounts))

    # Try to create duplicate
    result = r.evalsha(create_accounts_sha, 0, json.dumps(accounts))
    results = json.loads(result)

    assert results[0]['error'] == 1, f"Expected error 1 (ACCOUNT_EXISTS), got {results[0]['error']}"
    print("✓ Passed")

def test_simple_transfer():
    """Test a simple single-phase transfer"""
    cleanup()
    print("Test: Simple transfer...")

    # Create two accounts
    accounts = [
        {"id": "1", "ledger": 700, "code": 10, "flags": 0},
        {"id": "2", "ledger": 700, "code": 10, "flags": 0}
    ]
    r.evalsha(create_accounts_sha, 0, json.dumps(accounts))

    # Create transfer
    transfers = [{
        "id": "1",
        "debit_account_id": "1",
        "credit_account_id": "2",
        "amount": 100,
        "ledger": 700,
        "code": 10,
        "flags": 0
    }]

    result = r.evalsha(create_transfers_sha, 0, json.dumps(transfers))
    results = json.loads(result)

    assert results[0]['error'] == 0, f"Expected error 0, got {results[0]['error']}"

    # Check account balances
    account1 = r.hgetall("account:1")
    account2 = r.hgetall("account:2")

    assert account1['debits_posted'] == "100", f"Expected debits_posted=100, got {account1['debits_posted']}"
    assert account2['credits_posted'] == "100", f"Expected credits_posted=100, got {account2['credits_posted']}"

    print("✓ Passed")

def test_pending_transfer():
    """Test two-phase transfer (pending -> post)"""
    cleanup()
    print("Test: Pending transfer...")

    # Create two accounts
    accounts = [
        {"id": "1", "ledger": 700, "code": 10, "flags": 0},
        {"id": "2", "ledger": 700, "code": 10, "flags": 0}
    ]
    r.evalsha(create_accounts_sha, 0, json.dumps(accounts))

    # Create pending transfer (flags=2 for PENDING)
    transfers = [{
        "id": "1",
        "debit_account_id": "1",
        "credit_account_id": "2",
        "amount": 100,
        "ledger": 700,
        "code": 10,
        "flags": 2  # PENDING
    }]

    result = r.evalsha(create_transfers_sha, 0, json.dumps(transfers))
    results = json.loads(result)
    assert results[0]['error'] == 0, f"Expected error 0, got {results[0]['error']}"

    # Check pending balances
    account1 = r.hgetall("account:1")
    account2 = r.hgetall("account:2")
    assert account1['debits_pending'] == "100"
    assert account1['debits_posted'] == "0"
    assert account2['credits_pending'] == "100"
    assert account2['credits_posted'] == "0"

    # Post the pending transfer (flags=4 for POST_PENDING_TRANSFER)
    post_transfers = [{
        "id": "2",
        "pending_id": "1",
        "debit_account_id": "1",
        "credit_account_id": "2",
        "amount": 100,
        "ledger": 700,
        "code": 10,
        "flags": 4  # POST_PENDING_TRANSFER
    }]

    result = r.evalsha(create_transfers_sha, 0, json.dumps(post_transfers))
    results = json.loads(result)
    assert results[0]['error'] == 0, f"Expected error 0, got {results[0]['error']}"

    # Check posted balances
    account1 = r.hgetall("account:1")
    account2 = r.hgetall("account:2")
    assert account1['debits_pending'] == "0"
    assert account1['debits_posted'] == "100"
    assert account2['credits_pending'] == "0"
    assert account2['credits_posted'] == "100"

    print("✓ Passed")

def test_void_pending_transfer():
    """Test voiding a pending transfer"""
    cleanup()
    print("Test: Void pending transfer...")

    # Create two accounts
    accounts = [
        {"id": "1", "ledger": 700, "code": 10, "flags": 0},
        {"id": "2", "ledger": 700, "code": 10, "flags": 0}
    ]
    r.evalsha(create_accounts_sha, 0, json.dumps(accounts))

    # Create pending transfer
    transfers = [{
        "id": "1",
        "debit_account_id": "1",
        "credit_account_id": "2",
        "amount": 100,
        "ledger": 700,
        "code": 10,
        "flags": 2  # PENDING
    }]
    r.evalsha(create_transfers_sha, 0, json.dumps(transfers))

    # Void the pending transfer (flags=8 for VOID_PENDING_TRANSFER)
    void_transfers = [{
        "id": "2",
        "pending_id": "1",
        "debit_account_id": "1",
        "credit_account_id": "2",
        "amount": 100,
        "ledger": 700,
        "code": 10,
        "flags": 8  # VOID_PENDING_TRANSFER
    }]

    result = r.evalsha(create_transfers_sha, 0, json.dumps(void_transfers))
    results = json.loads(result)
    assert results[0]['error'] == 0, f"Expected error 0, got {results[0]['error']}"

    # Check balances are back to zero
    account1 = r.hgetall("account:1")
    account2 = r.hgetall("account:2")
    assert account1['debits_pending'] == "0"
    assert account1['debits_posted'] == "0"
    assert account2['credits_pending'] == "0"
    assert account2['credits_posted'] == "0"

    print("✓ Passed")

def test_linked_accounts():
    """Test linked account creation (all-or-nothing)"""
    cleanup()
    print("Test: Linked accounts...")

    # Create accounts with second one invalid and linked flag set
    accounts = [
        {"id": "1", "ledger": 700, "code": 10, "flags": 1},  # LINKED
        {"id": "1", "ledger": 700, "code": 10, "flags": 0}   # Duplicate ID
    ]

    result = r.evalsha(create_accounts_sha, 0, json.dumps(accounts))
    results = json.loads(result)

    # Both should fail
    assert results[0]['error'] == 22, f"Expected error 22 (LINKED_EVENT_FAILED), got {results[0]['error']}"
    assert results[1]['error'] == 1, f"Expected error 1 (ACCOUNT_EXISTS), got {results[1]['error']}"

    # First account should not exist (rolled back)
    assert r.exists("account:1") == 0, "Account 1 should not exist after rollback"

    print("✓ Passed")

def test_lookup_accounts():
    """Test account lookup"""
    cleanup()
    print("Test: Lookup accounts...")

    # Create accounts
    accounts = [
        {"id": "1", "ledger": 700, "code": 10, "flags": 0},
        {"id": "2", "ledger": 700, "code": 20, "flags": 0}
    ]
    r.evalsha(create_accounts_sha, 0, json.dumps(accounts))

    # Lookup accounts
    result = r.evalsha(lookup_accounts_sha, 0, json.dumps(["1", "2", "999"]))
    results = json.loads(result)

    assert len(results) == 2, f"Expected 2 accounts found, got {len(results)}"
    assert results[0]['id'] == "1"
    assert results[0]['ledger'] == "700"
    assert results[1]['id'] == "2"
    assert results[1]['code'] == "20"

    print("✓ Passed")

def test_balance_constraints():
    """Test account balance constraints"""
    cleanup()
    print("Test: Balance constraints...")

    # Create account with debits_must_not_exceed_credits flag (flag=2)
    accounts = [
        {"id": "1", "ledger": 700, "code": 10, "flags": 2},  # DEBITS_MUST_NOT_EXCEED_CREDITS
        {"id": "2", "ledger": 700, "code": 10, "flags": 0}
    ]
    r.evalsha(create_accounts_sha, 0, json.dumps(accounts))

    # Try to debit from account 1 (should fail because credits=0)
    transfers = [{
        "id": "1",
        "debit_account_id": "1",
        "credit_account_id": "2",
        "amount": 100,
        "ledger": 700,
        "code": 10,
        "flags": 0
    }]

    result = r.evalsha(create_transfers_sha, 0, json.dumps(transfers))
    results = json.loads(result)

    assert results[0]['error'] == 18, f"Expected error 18 (EXCEEDS_CREDITS), got {results[0]['error']}"

    print("✓ Passed")

def test_get_account_transfers():
    """Test getting account transfers"""
    cleanup()
    print("Test: Get account transfers...")

    # Create accounts
    accounts = [
        {"id": "1", "ledger": 700, "code": 10, "flags": 0},
        {"id": "2", "ledger": 700, "code": 10, "flags": 0},
        {"id": "3", "ledger": 700, "code": 10, "flags": 0}
    ]
    r.evalsha(create_accounts_sha, 0, json.dumps(accounts))

    # Create multiple transfers
    transfers = [
        {"id": "100", "debit_account_id": "1", "credit_account_id": "2", "amount": 50, "ledger": 700, "code": 10, "flags": 0},
        {"id": "101", "debit_account_id": "1", "credit_account_id": "3", "amount": 30, "ledger": 700, "code": 10, "flags": 0},
        {"id": "102", "debit_account_id": "2", "credit_account_id": "1", "amount": 20, "ledger": 700, "code": 10, "flags": 0}
    ]
    r.evalsha(create_transfers_sha, 0, json.dumps(transfers))

    # Get transfers for account 1
    result = r.evalsha(get_account_transfers_sha, 0, "1")
    account_transfers = json.loads(result)

    # Account 1 should have 3 transfers (2 debits, 1 credit)
    assert len(account_transfers) == 3, f"Expected 3 transfers, got {len(account_transfers)}"

    transfer_ids = [t['id'] for t in account_transfers]
    assert "100" in transfer_ids
    assert "101" in transfer_ids
    assert "102" in transfer_ids

    print("✓ Passed")

def test_get_account_balances_no_history():
    """Test getting account balances without history flag"""
    cleanup()
    print("Test: Get account balances (no history)...")

    # Create account without HISTORY flag
    accounts = [
        {"id": "1", "ledger": 700, "code": 10, "flags": 0},
        {"id": "2", "ledger": 700, "code": 10, "flags": 0}
    ]
    r.evalsha(create_accounts_sha, 0, json.dumps(accounts))

    # Create transfers
    transfers = [
        {"id": "1", "debit_account_id": "1", "credit_account_id": "2", "amount": 100, "ledger": 700, "code": 10, "flags": 0},
        {"id": "2", "debit_account_id": "1", "credit_account_id": "2", "amount": 50, "ledger": 700, "code": 10, "flags": 0}
    ]
    r.evalsha(create_transfers_sha, 0, json.dumps(transfers))

    # Get balances
    result = r.evalsha(get_account_balances_sha, 0, "1")
    balance_data = json.loads(result)

    assert balance_data['account_id'] == "1"
    assert balance_data['current_balance']['debits_posted'] == "150"
    assert len(balance_data['history']) == 0, "Should have no history without HISTORY flag"

    print("✓ Passed")

def test_get_account_balances_with_history():
    """Test getting account balances with history flag"""
    cleanup()
    print("Test: Get account balances (with history)...")

    # Create account with HISTORY flag (flag=8)
    accounts = [
        {"id": "1", "ledger": 700, "code": 10, "flags": 8},  # HISTORY
        {"id": "2", "ledger": 700, "code": 10, "flags": 0}
    ]
    r.evalsha(create_accounts_sha, 0, json.dumps(accounts))

    # Create transfers
    transfers = [
        {"id": "1", "debit_account_id": "1", "credit_account_id": "2", "amount": 100, "ledger": 700, "code": 10, "flags": 0},
        {"id": "2", "debit_account_id": "1", "credit_account_id": "2", "amount": 50, "ledger": 700, "code": 10, "flags": 0}
    ]
    r.evalsha(create_transfers_sha, 0, json.dumps(transfers))

    # Get balances
    result = r.evalsha(get_account_balances_sha, 0, "1")
    balance_data = json.loads(result)

    assert balance_data['account_id'] == "1"
    assert balance_data['current_balance']['debits_posted'] == "150"
    assert len(balance_data['history']) == 2, f"Should have 2 history entries, got {len(balance_data['history'])}"

    # Check history entries
    history = balance_data['history']
    assert history[0]['debits_posted'] == 100
    assert history[0]['transfer_id'] == "1"
    assert history[1]['debits_posted'] == 150
    assert history[1]['transfer_id'] == "2"

    print("✓ Passed")

# Run all tests
def main():
    try:
        print("Running Lua Beetle Tests\n")
        test_create_single_account()
        test_create_duplicate_account()
        test_simple_transfer()
        test_pending_transfer()
        test_void_pending_transfer()
        test_linked_accounts()
        test_lookup_accounts()
        test_balance_constraints()
        test_get_account_transfers()
        test_get_account_balances_no_history()
        test_get_account_balances_with_history()
        print("\n✅ All tests passed!")
        cleanup()
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
