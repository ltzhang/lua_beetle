#!/usr/bin/env python3
"""
Basic test suite for Lua Beetle
Tests account creation, transfers, and lookups using single operations with pipelining
"""

import json
import redis
import sys

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Load Lua scripts
with open('../scripts/create_account.lua', 'r') as f:
    create_account_script = f.read()

with open('../scripts/create_chained_accounts.lua', 'r') as f:
    create_chained_accounts_script = f.read()

with open('../scripts/create_transfer.lua', 'r') as f:
    create_transfer_script = f.read()

with open('../scripts/create_chained_transfers.lua', 'r') as f:
    create_chained_transfers_script = f.read()

with open('../scripts/lookup_account.lua', 'r') as f:
    lookup_account_script = f.read()

with open('../scripts/get_account_transfers.lua', 'r') as f:
    get_account_transfers_script = f.read()

with open('../scripts/get_account_balances.lua', 'r') as f:
    get_account_balances_script = f.read()

# Register scripts
create_account_sha = r.script_load(create_account_script)
create_chained_accounts_sha = r.script_load(create_chained_accounts_script)
create_transfer_sha = r.script_load(create_transfer_script)
create_chained_transfers_sha = r.script_load(create_chained_transfers_script)
lookup_account_sha = r.script_load(lookup_account_script)
get_account_transfers_sha = r.script_load(get_account_transfers_script)
get_account_balances_sha = r.script_load(get_account_balances_script)

def cleanup():
    """Clean up test data"""
    r.flushdb()

def test_create_single_account():
    """Test creating a single account"""
    cleanup()
    print("Test: Create single account...")

    account = {
        "id": "1",
        "ledger": 700,
        "code": 10,
        "flags": 0
    }

    result = r.evalsha(create_account_sha, 0, json.dumps(account))
    result_obj = json.loads(result)

    assert result_obj['result'] == 0, f"Expected result 0, got {result_obj['result']}"

    # Verify account exists
    account_data = r.hgetall("account:1")
    assert account_data['id'] == "1"
    assert account_data['ledger'] == "700"
    assert account_data['code'] == "10"
    assert account_data['debits_posted'] == "0"
    assert account_data['credits_posted'] == "0"

    print("✓ Passed")

def test_create_multiple_accounts_pipelined():
    """Test creating multiple accounts using pipelining"""
    cleanup()
    print("Test: Create multiple accounts (pipelined)...")

    # Create 5 accounts using pipeline
    pipe = r.pipeline()
    for i in range(1, 6):
        account = {
            "id": str(i),
            "ledger": 700,
            "code": 10,
            "flags": 0
        }
        pipe.evalsha(create_account_sha, 0, json.dumps(account))

    results = pipe.execute()

    # Check all succeeded
    for i, result in enumerate(results):
        result_obj = json.loads(result)
        assert result_obj['result'] == 0, f"Account {i+1} creation failed with result {result_obj['result']}"

    # Verify all accounts exist
    for i in range(1, 6):
        assert r.exists(f"account:{i}") == 1, f"Account {i} not found"

    print("✓ Passed")

def test_create_duplicate_account():
    """Test that creating a duplicate account fails"""
    cleanup()
    print("Test: Create duplicate account...")

    account = {
        "id": "1",
        "ledger": 700,
        "code": 10,
        "flags": 0
    }

    # Create first account
    r.evalsha(create_account_sha, 0, json.dumps(account))

    # Try to create duplicate
    result = r.evalsha(create_account_sha, 0, json.dumps(account))
    result_obj = json.loads(result)

    assert result_obj['result'] == 21, f"Expected result 21 (exists), got {result_obj['result']}"
    print("✓ Passed")

def test_simple_transfer():
    """Test a simple single-phase transfer using pipelined account creation"""
    cleanup()
    print("Test: Simple transfer...")

    # Create two accounts using pipeline
    pipe = r.pipeline()
    for i in [1, 2]:
        account = {
            "id": str(i),
            "ledger": 700,
            "code": 10,
            "flags": 0
        }
        pipe.evalsha(create_account_sha, 0, json.dumps(account))
    pipe.execute()

    # Create a transfer
    transfer = {
        "id": "1",
        "debit_account_id": "1",
        "credit_account_id": "2",
        "amount": 100,
        "ledger": 700,
        "code": 10,
        "flags": 0
    }

    result = r.evalsha(create_transfer_sha, 0, json.dumps(transfer))
    result_obj = json.loads(result)

    assert result_obj['result'] == 0, f"Expected result 0, got {result_obj['result']}"

    # Verify balances
    account1 = r.hgetall("account:1")
    account2 = r.hgetall("account:2")

    assert account1['debits_posted'] == "100"
    assert account2['credits_posted'] == "100"

    print("✓ Passed")

def test_pending_transfer():
    """Test pending and posting a transfer"""
    cleanup()
    print("Test: Pending transfer...")

    # Create two accounts
    pipe = r.pipeline()
    for i in [1, 2]:
        account = {
            "id": str(i),
            "ledger": 700,
            "code": 10,
            "flags": 0
        }
        pipe.evalsha(create_account_sha, 0, json.dumps(account))
    pipe.execute()

    # Create pending transfer (flags = 0x0002)
    transfer = {
        "id": "1",
        "debit_account_id": "1",
        "credit_account_id": "2",
        "amount": 100,
        "ledger": 700,
        "code": 10,
        "flags": 0x0002  # PENDING
    }

    result = r.evalsha(create_transfer_sha, 0, json.dumps(transfer))
    result_obj = json.loads(result)

    assert result_obj['result'] == 0, f"Expected result 0, got {result_obj['result']}"

    # Verify pending balances
    account1 = r.hgetall("account:1")
    account2 = r.hgetall("account:2")

    assert account1['debits_pending'] == "100"
    assert account1['debits_posted'] == "0"
    assert account2['credits_pending'] == "100"
    assert account2['credits_posted'] == "0"

    print("✓ Passed")

def test_linked_accounts():
    """Test linked account creation (uses chained script)"""
    cleanup()
    print("Test: Linked accounts...")

    # Test that a chain with failure rolls back properly
    accounts = [
        {"id": "1", "ledger": 700, "code": 10, "flags": 0x0001},  # LINKED
        {"id": "1", "ledger": 700, "code": 10, "flags": 0}  # Duplicate ID, NOT linked
    ]

    result = r.evalsha(create_chained_accounts_sha, 0, json.dumps(accounts))
    results = json.loads(result)

    # Both should fail: first with linked_event_failed, second with exists
    assert len(results) == 2, f"Expected 2 results, got {len(results)}"
    assert results[0]['result'] == 1, f"Expected result 1 (linked_event_failed), got {results[0]['result']}"
    assert results[1]['result'] == 21, f"Expected result 21 (exists), got {results[1]['result']}"

    # Account 1 should not exist (rolled back)
    assert r.exists("account:1") == 0, "Account 1 should not exist after rollback"

    print("✓ Passed")

def test_lookup_account():
    """Test looking up an account"""
    cleanup()
    print("Test: Lookup account...")

    # Create account
    account = {
        "id": "1",
        "ledger": 700,
        "code": 10,
        "flags": 0
    }
    r.evalsha(create_account_sha, 0, json.dumps(account))

    # Lookup account
    result = r.evalsha(lookup_account_sha, 0, "1")
    account_data = json.loads(result)

    assert account_data['id'] == "1"
    assert account_data['ledger'] == "700"
    assert account_data['code'] == "10"

    # Lookup non-existent account
    result = r.evalsha(lookup_account_sha, 0, "999")
    account_data = json.loads(result)
    assert account_data == {}, "Non-existent account should return empty object"

    print("✓ Passed")

def test_balance_constraints():
    """Test account balance constraints"""
    cleanup()
    print("Test: Balance constraints...")

    # Create account with debits_must_not_exceed_credits flag (0x0002)
    account = {
        "id": "1",
        "ledger": 700,
        "code": 10,
        "flags": 0x0002
    }
    r.evalsha(create_account_sha, 0, json.dumps(account))

    # Create another account without constraints
    account2 = {
        "id": "2",
        "ledger": 700,
        "code": 10,
        "flags": 0
    }
    r.evalsha(create_account_sha, 0, json.dumps(account2))

    # Try to debit from account 1 (should fail - exceeds credits)
    transfer = {
        "id": "1",
        "debit_account_id": "1",
        "credit_account_id": "2",
        "amount": 100,
        "ledger": 700,
        "code": 10,
        "flags": 0
    }

    result = r.evalsha(create_transfer_sha, 0, json.dumps(transfer))
    result_obj = json.loads(result)

    assert result_obj['result'] == 58, f"Expected result 58 (exceeds_credits), got {result_obj['result']}"

    print("✓ Passed")

def test_get_account_transfers():
    """Test getting transfers for an account"""
    cleanup()
    print("Test: Get account transfers...")

    # Create two accounts
    pipe = r.pipeline()
    for i in [1, 2]:
        account = {
            "id": str(i),
            "ledger": 700,
            "code": 10,
            "flags": 0
        }
        pipe.evalsha(create_account_sha, 0, json.dumps(account))
    pipe.execute()

    # Create multiple transfers using pipeline
    pipe = r.pipeline()
    for i in range(1, 4):
        transfer = {
            "id": str(i),
            "debit_account_id": "1",
            "credit_account_id": "2",
            "amount": 100 * i,
            "ledger": 700,
            "code": 10,
            "flags": 0
        }
        pipe.evalsha(create_transfer_sha, 0, json.dumps(transfer))
    pipe.execute()

    # Get transfers for account 1
    result = r.evalsha(get_account_transfers_sha, 0, "1")
    transfers = json.loads(result)

    assert len(transfers) == 3, f"Expected 3 transfers, got {len(transfers)}"
    assert transfers[0]['id'] == "1"
    assert transfers[1]['id'] == "2"
    assert transfers[2]['id'] == "3"

    print("✓ Passed")

def test_get_account_balances_no_history():
    """Test getting account balances without history"""
    cleanup()
    print("Test: Get account balances (no history)...")

    # Create account without HISTORY flag
    account = {
        "id": "1",
        "ledger": 700,
        "code": 10,
        "flags": 0
    }
    r.evalsha(create_account_sha, 0, json.dumps(account))

    # Get balances
    result = r.evalsha(get_account_balances_sha, 0, "1")
    balances = json.loads(result)

    assert balances['account_id'] == "1"
    assert 'current_balance' in balances
    assert balances['history'] == [] or balances['history'] == {}

    print("✓ Passed")

def test_get_account_balances_with_history():
    """Test getting account balances with history"""
    cleanup()
    print("Test: Get account balances (with history)...")

    # Create account with HISTORY flag (0x0008)
    account = {
        "id": "1",
        "ledger": 700,
        "code": 10,
        "flags": 0x0008
    }
    r.evalsha(create_account_sha, 0, json.dumps(account))

    # Create another account
    account2 = {
        "id": "2",
        "ledger": 700,
        "code": 10,
        "flags": 0
    }
    r.evalsha(create_account_sha, 0, json.dumps(account2))

    # Create some transfers using pipeline
    pipe = r.pipeline()
    for i in range(1, 4):
        transfer = {
            "id": str(i),
            "debit_account_id": "1",
            "credit_account_id": "2",
            "amount": 100,
            "ledger": 700,
            "code": 10,
            "flags": 0
        }
        pipe.evalsha(create_transfer_sha, 0, json.dumps(transfer))
    pipe.execute()

    # Get balances with history
    result = r.evalsha(get_account_balances_sha, 0, "1")
    balances = json.loads(result)

    assert balances['account_id'] == "1"
    assert 'current_balance' in balances
    assert len(balances['history']) == 3, f"Expected 3 history entries, got {len(balances['history'])}"

    print("✓ Passed")

if __name__ == "__main__":
    print("Running Lua Beetle Tests\n")

    try:
        test_create_single_account()
        test_create_multiple_accounts_pipelined()
        test_create_duplicate_account()
        test_simple_transfer()
        test_pending_transfer()
        test_linked_accounts()
        test_lookup_account()
        test_balance_constraints()
        test_get_account_transfers()
        test_get_account_balances_no_history()
        test_get_account_balances_with_history()

        print("\n✅ All tests passed!")
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
