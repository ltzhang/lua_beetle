#!/usr/bin/env python3
"""
Comprehensive functional tests for Lua Beetle.
Tests all core functionality: accounts, transfers, two-phase transfers,
lookups, query operations (get_account_transfers, get_account_balances).
"""

import redis
import struct
import sys
import os

# Redis connection
r = redis.Redis(host='localhost', port=6379, decode_responses=False)

# Error codes (matching TigerBeetle)
ERR_OK = 0
ERR_LINKED_EVENT_CHAIN_OPEN = 1
ERR_ID_ALREADY_EXISTS = 21
ERR_EXISTS_WITH_DIFFERENT_FLAGS = 29
ERR_PENDING_TRANSFER_NOT_FOUND = 34
ERR_PENDING_TRANSFER_ALREADY_POSTED = 35
ERR_PENDING_TRANSFER_ALREADY_VOIDED = 36
ERR_DEBIT_ACCOUNT_NOT_FOUND = 38
ERR_CREDIT_ACCOUNT_NOT_FOUND = 39
ERR_ACCOUNTS_MUST_BE_DIFFERENT = 40
ERR_EXCEEDS_CREDITS = 42
ERR_EXCEEDS_DEBITS = 43
ERR_LEDGER_MUST_MATCH = 52

# Flags
FLAG_LINKED = 0x0001
FLAG_PENDING = 0x0002
FLAG_POST_PENDING = 0x0004
FLAG_VOID_PENDING = 0x0008
FLAG_HISTORY = 0x0008  # Account flag

# Filter flags
FILTER_DEBITS = 0x01
FILTER_CREDITS = 0x02
FILTER_REVERSED = 0x04

# Load scripts
def load_script(filename):
    script_path = os.path.join(os.path.dirname(__file__), '..', 'scripts', filename)
    with open(script_path, 'r') as f:
        return r.script_load(f.read())

# Load all scripts
create_account_sha = load_script('create_account.lua')
create_linked_accounts_sha = load_script('create_linked_accounts.lua')
create_transfer_sha = load_script('create_transfer.lua')
create_linked_transfers_sha = load_script('create_linked_transfers.lua')
lookup_account_sha = load_script('lookup_account.lua')
lookup_transfer_sha = load_script('lookup_transfer.lua')
get_transfers_sha = load_script('get_account_transfers.lua')
get_balances_sha = load_script('get_account_balances.lua')

print("Scripts loaded successfully")

# Helper functions
def u64_to_bytes(n):
    """Convert uint64 to little-endian bytes."""
    return struct.pack('<Q', n)

def u128_to_bytes(n):
    """Convert uint128 to 16-byte little-endian."""
    return struct.pack('<QQ', n & 0xFFFFFFFFFFFFFFFF, (n >> 64) & 0xFFFFFFFFFFFFFFFF)

def u32_to_bytes(n):
    """Convert uint32 to little-endian bytes."""
    return struct.pack('<I', n)

def u16_to_bytes(n):
    """Convert uint16 to little-endian bytes."""
    return struct.pack('<H', n)

def bytes_to_u64(b, offset=0):
    """Convert bytes to uint64."""
    return struct.unpack_from('<Q', b, offset)[0]

def bytes_to_u128(b, offset=0):
    """Convert bytes to uint128."""
    low, high = struct.unpack_from('<QQ', b, offset)
    return low | (high << 64)

def hash_string_to_u64(s):
    """Simple hash function to convert string ID to uint64."""
    h = 0
    for c in s:
        h = (h * 31 + ord(c)) & 0xFFFFFFFFFFFFFFFF
    return h

def encode_account(account_id, ledger, code, flags):
    """Encode account to 128-byte binary format."""
    if isinstance(account_id, str):
        account_id = hash_string_to_u64(account_id)

    buf = bytearray(128)
    struct.pack_into('<QQ', buf, 0, account_id & 0xFFFFFFFFFFFFFFFF, (account_id >> 64) & 0xFFFFFFFFFFFFFFFF)
    struct.pack_into('<I', buf, 112, ledger)
    struct.pack_into('<H', buf, 116, code)
    struct.pack_into('<H', buf, 118, flags)
    return bytes(buf)

def encode_transfer(transfer_id, debit_account_id, credit_account_id, amount, ledger, code, flags, pending_id=None):
    """Encode transfer to 128-byte binary format."""
    if isinstance(transfer_id, str):
        transfer_id = hash_string_to_u64(transfer_id)
    if isinstance(debit_account_id, str):
        debit_account_id = hash_string_to_u64(debit_account_id)
    if isinstance(credit_account_id, str):
        credit_account_id = hash_string_to_u64(credit_account_id)

    buf = bytearray(128)
    struct.pack_into('<QQ', buf, 0, transfer_id & 0xFFFFFFFFFFFFFFFF, (transfer_id >> 64) & 0xFFFFFFFFFFFFFFFF)
    struct.pack_into('<QQ', buf, 16, debit_account_id & 0xFFFFFFFFFFFFFFFF, (debit_account_id >> 64) & 0xFFFFFFFFFFFFFFFF)
    struct.pack_into('<QQ', buf, 32, credit_account_id & 0xFFFFFFFFFFFFFFFF, (credit_account_id >> 64) & 0xFFFFFFFFFFFFFFFF)
    struct.pack_into('<QQ', buf, 48, amount & 0xFFFFFFFFFFFFFFFF, (amount >> 64) & 0xFFFFFFFFFFFFFFFF)

    if pending_id is not None:
        if isinstance(pending_id, str):
            pending_id = hash_string_to_u64(pending_id)
        struct.pack_into('<QQ', buf, 64, pending_id & 0xFFFFFFFFFFFFFFFF, (pending_id >> 64) & 0xFFFFFFFFFFFFFFFF)

    struct.pack_into('<I', buf, 112, ledger)
    struct.pack_into('<H', buf, 116, code)
    struct.pack_into('<H', buf, 118, flags)
    return bytes(buf)

def encode_account_filter(account_id, timestamp_min=0, timestamp_max=2**64-1, limit=100, flags=FILTER_DEBITS | FILTER_CREDITS):
    """Encode AccountFilter to 128-byte binary format."""
    if isinstance(account_id, str):
        account_id = hash_string_to_u64(account_id)

    buf = bytearray(128)
    struct.pack_into('<QQ', buf, 0, account_id & 0xFFFFFFFFFFFFFFFF, (account_id >> 64) & 0xFFFFFFFFFFFFFFFF)
    struct.pack_into('<Q', buf, 48, timestamp_min)
    struct.pack_into('<Q', buf, 56, timestamp_max)
    struct.pack_into('<I', buf, 64, limit)
    struct.pack_into('<I', buf, 68, flags)
    return bytes(buf)

def decode_result(result):
    """Decode error code from result."""
    if isinstance(result, int):
        return result
    elif isinstance(result, bytes):
        return result[0] if len(result) >= 1 else 0
    elif isinstance(result, str):
        result_bytes = result.encode('latin-1')
        return result_bytes[0] if len(result_bytes) >= 1 else 0
    return int(result)

def decode_account(data):
    """Decode account from binary format."""
    if not data or len(data) < 128:
        return None

    if isinstance(data, str):
        data = data.encode('latin-1')

    account = {
        'id': bytes_to_u128(data, 0),
        'debits_pending': bytes_to_u128(data, 16),
        'debits_posted': bytes_to_u128(data, 32),
        'credits_pending': bytes_to_u128(data, 48),
        'credits_posted': bytes_to_u128(data, 64),
        'ledger': struct.unpack_from('<I', data, 112)[0],
        'code': struct.unpack_from('<H', data, 116)[0],
        'flags': struct.unpack_from('<H', data, 118)[0],
    }
    return account

def decode_transfer(data):
    """Decode transfer from binary format."""
    if not data or len(data) < 128:
        return None

    if isinstance(data, str):
        data = data.encode('latin-1')

    transfer = {
        'id': bytes_to_u128(data, 0),
        'debit_account_id': bytes_to_u128(data, 16),
        'credit_account_id': bytes_to_u128(data, 32),
        'amount': bytes_to_u128(data, 48),
        'pending_id': bytes_to_u128(data, 64),
        'ledger': struct.unpack_from('<I', data, 112)[0],
        'code': struct.unpack_from('<H', data, 116)[0],
        'flags': struct.unpack_from('<H', data, 118)[0],
        'timestamp': bytes_to_u64(data, 120),
    }
    return transfer

# Test utilities
def assert_equal(actual, expected, msg):
    if actual != expected:
        print(f"❌ FAIL: {msg}")
        print(f"   Expected: {expected}, Got: {actual}")
        sys.exit(1)

def assert_not_equal(actual, expected, msg):
    if actual == expected:
        print(f"❌ FAIL: {msg}")
        print(f"   Expected NOT to be: {expected}, Got: {actual}")
        sys.exit(1)

# Test functions
def test_create_account():
    """Test basic account creation."""
    print("\n=== Test: Create Account ===")

    account_data = encode_account(1, 700, 10, 0)
    result = r.evalsha(create_account_sha, 0, account_data)
    assert_equal(decode_result(result), ERR_OK, "Create account should succeed")

    lookup_data = u128_to_bytes(1)
    account_result = r.evalsha(lookup_account_sha, 0, lookup_data)
    account = decode_account(account_result)

    assert_equal(account['id'], 1, "Account ID should match")
    assert_equal(account['ledger'], 700, "Ledger should match")
    assert_equal(account['code'], 10, "Code should match")
    assert_equal(account['debits_posted'], 0, "Initial debits should be 0")
    assert_equal(account['credits_posted'], 0, "Initial credits should be 0")

    print("✓ Account created and verified")

def test_duplicate_account():
    """Test duplicate account creation fails."""
    print("\n=== Test: Duplicate Account ===")

    account_data = encode_account(2, 700, 10, 0)
    result = r.evalsha(create_account_sha, 0, account_data)
    assert_equal(decode_result(result), ERR_OK, "First account creation should succeed")

    result = r.evalsha(create_account_sha, 0, account_data)
    assert_equal(decode_result(result), ERR_ID_ALREADY_EXISTS, "Duplicate account should fail")

    print("✓ Duplicate account correctly rejected")

def test_linked_accounts():
    """Test linked account creation with LINKED flag."""
    print("\n=== Test: Linked Accounts ===")

    # Create 3 linked accounts (all succeed)
    accounts = b''
    accounts += encode_account(500, 700, 10, FLAG_LINKED)  # LINKED
    accounts += encode_account(501, 700, 10, FLAG_LINKED)  # LINKED
    accounts += encode_account(502, 700, 10, 0)  # End of chain

    result = r.evalsha(create_linked_accounts_sha, 0, accounts)
    # Result should be empty or success indicators

    # Verify all accounts created
    account1 = decode_account(r.evalsha(lookup_account_sha, 0, u128_to_bytes(500)))
    account2 = decode_account(r.evalsha(lookup_account_sha, 0, u128_to_bytes(501)))
    account3 = decode_account(r.evalsha(lookup_account_sha, 0, u128_to_bytes(502)))

    assert_equal(account1['ledger'], 700, "Account 500 should be created")
    assert_equal(account2['ledger'], 700, "Account 501 should be created")
    assert_equal(account3['ledger'], 700, "Account 502 should be created")

    print("✓ Linked accounts created successfully")

def test_linked_accounts_rollback():
    """Test linked account rollback on error."""
    print("\n=== Test: Linked Accounts Rollback ===")

    # Create first account separately
    r.evalsha(create_account_sha, 0, encode_account(600, 700, 10, 0))

    # Try to create linked chain with duplicate (should rollback entire chain)
    accounts = b''
    accounts += encode_account(601, 700, 10, FLAG_LINKED)  # LINKED
    accounts += encode_account(600, 700, 10, 0)  # Duplicate - should fail

    result = r.evalsha(create_linked_accounts_sha, 0, accounts)
    # Should fail due to duplicate

    # Verify account 601 was NOT created (rolled back)
    lookup_data = u128_to_bytes(601)
    account_result = r.evalsha(lookup_account_sha, 0, lookup_data)

    # Empty result means account doesn't exist
    assert_equal(len(account_result), 0, "Account 601 should be rolled back")

    print("✓ Linked accounts rollback successful")

def test_simple_transfer():
    """Test basic transfer between accounts."""
    print("\n=== Test: Simple Transfer ===")

    account_data = encode_account(10, 700, 10, 0)
    r.evalsha(create_account_sha, 0, account_data)

    account_data = encode_account(11, 700, 10, 0)
    r.evalsha(create_account_sha, 0, account_data)

    transfer_data = encode_transfer("transfer1", 10, 11, 1000, 700, 10, 0)
    result = r.evalsha(create_transfer_sha, 0, transfer_data)
    assert_equal(decode_result(result), ERR_OK, "Transfer should succeed")

    account1 = decode_account(r.evalsha(lookup_account_sha, 0, u128_to_bytes(10)))
    account2 = decode_account(r.evalsha(lookup_account_sha, 0, u128_to_bytes(11)))

    assert_equal(account1['debits_posted'], 1000, "Debit account should have debits")
    assert_equal(account2['credits_posted'], 1000, "Credit account should have credits")

    print("✓ Transfer completed and balances verified")

def test_transfer_nonexistent_account():
    """Test transfer with nonexistent accounts fails."""
    print("\n=== Test: Transfer with Nonexistent Account ===")

    account_data = encode_account(20, 700, 10, 0)
    r.evalsha(create_account_sha, 0, account_data)

    transfer_data = encode_transfer("transfer2", 20, 999, 100, 700, 10, 0)
    result = r.evalsha(create_transfer_sha, 0, transfer_data)
    assert_equal(decode_result(result), ERR_CREDIT_ACCOUNT_NOT_FOUND, "Transfer should fail with account not found")

    transfer_data = encode_transfer("transfer3", 999, 20, 100, 700, 10, 0)
    result = r.evalsha(create_transfer_sha, 0, transfer_data)
    assert_equal(decode_result(result), ERR_DEBIT_ACCOUNT_NOT_FOUND, "Transfer should fail with account not found")

    print("✓ Nonexistent account transfers correctly rejected")

def test_two_phase_pending():
    """Test creating pending transfer."""
    print("\n=== Test: Two-Phase Pending Transfer ===")

    account_data = encode_account(30, 700, 10, 0)
    r.evalsha(create_account_sha, 0, account_data)

    account_data = encode_account(31, 700, 10, 0)
    r.evalsha(create_account_sha, 0, account_data)

    transfer_data = encode_transfer("pending1", 30, 31, 500, 700, 10, FLAG_PENDING)
    result = r.evalsha(create_transfer_sha, 0, transfer_data)
    assert_equal(decode_result(result), ERR_OK, "Pending transfer should succeed")

    account1 = decode_account(r.evalsha(lookup_account_sha, 0, u128_to_bytes(30)))
    account2 = decode_account(r.evalsha(lookup_account_sha, 0, u128_to_bytes(31)))

    assert_equal(account1['debits_pending'], 500, "Debit account should have pending debits")
    assert_equal(account1['debits_posted'], 0, "Debit account should have no posted debits")
    assert_equal(account2['credits_pending'], 500, "Credit account should have pending credits")
    assert_equal(account2['credits_posted'], 0, "Credit account should have no posted credits")

    print("✓ Pending transfer created successfully")

def test_two_phase_post():
    """Test posting a pending transfer."""
    print("\n=== Test: Two-Phase Post Transfer ===")

    account_data = encode_account(40, 700, 10, 0)
    r.evalsha(create_account_sha, 0, account_data)

    account_data = encode_account(41, 700, 10, 0)
    r.evalsha(create_account_sha, 0, account_data)

    transfer_data = encode_transfer("pending2", 40, 41, 600, 700, 10, FLAG_PENDING)
    result = r.evalsha(create_transfer_sha, 0, transfer_data)
    assert_equal(decode_result(result), ERR_OK, "Pending transfer should succeed")

    transfer_data = encode_transfer("post1", 40, 41, 600, 700, 10, FLAG_POST_PENDING, pending_id="pending2")
    result = r.evalsha(create_transfer_sha, 0, transfer_data)
    assert_equal(decode_result(result), ERR_OK, "Post transfer should succeed")

    account1 = decode_account(r.evalsha(lookup_account_sha, 0, u128_to_bytes(40)))
    account2 = decode_account(r.evalsha(lookup_account_sha, 0, u128_to_bytes(41)))

    assert_equal(account1['debits_pending'], 0, "Pending debits should be cleared")
    assert_equal(account1['debits_posted'], 600, "Debits should be posted")
    assert_equal(account2['credits_pending'], 0, "Pending credits should be cleared")
    assert_equal(account2['credits_posted'], 600, "Credits should be posted")

    print("✓ Pending transfer posted successfully")

def test_two_phase_void():
    """Test voiding a pending transfer."""
    print("\n=== Test: Two-Phase Void Transfer ===")

    account_data = encode_account(50, 700, 10, 0)
    r.evalsha(create_account_sha, 0, account_data)

    account_data = encode_account(51, 700, 10, 0)
    r.evalsha(create_account_sha, 0, account_data)

    transfer_data = encode_transfer("pending3", 50, 51, 700, 700, 10, FLAG_PENDING)
    result = r.evalsha(create_transfer_sha, 0, transfer_data)
    assert_equal(decode_result(result), ERR_OK, "Pending transfer should succeed")

    transfer_data = encode_transfer("void1", 50, 51, 700, 700, 10, FLAG_VOID_PENDING, pending_id="pending3")
    result = r.evalsha(create_transfer_sha, 0, transfer_data)
    assert_equal(decode_result(result), ERR_OK, "Void transfer should succeed")

    account1 = decode_account(r.evalsha(lookup_account_sha, 0, u128_to_bytes(50)))
    account2 = decode_account(r.evalsha(lookup_account_sha, 0, u128_to_bytes(51)))

    assert_equal(account1['debits_pending'], 0, "Pending debits should be cleared")
    assert_equal(account1['debits_posted'], 0, "Posted debits should remain 0")
    assert_equal(account2['credits_pending'], 0, "Pending credits should be cleared")
    assert_equal(account2['credits_posted'], 0, "Posted credits should remain 0")

    print("✓ Pending transfer voided successfully")

def test_get_account_transfers():
    """Test get_account_transfers query functionality."""
    print("\n=== Test: Get Account Transfers ===")

    # Create accounts
    r.evalsha(create_account_sha, 0, encode_account(100, 700, 10, 0))
    r.evalsha(create_account_sha, 0, encode_account(101, 700, 10, 0))

    # Create multiple transfers
    for i in range(3):
        transfer_data = encode_transfer(f"query_tx_{i}", 100, 101, 100 * (i + 1), 700, 10, 0)
        result = r.evalsha(create_transfer_sha, 0, transfer_data)
        assert_equal(decode_result(result), ERR_OK, f"Transfer {i} should succeed")

    # Query all transfers for account 100
    account_filter = encode_account_filter(100, limit=10, flags=FILTER_DEBITS | FILTER_CREDITS)
    transfers_blob = r.evalsha(get_transfers_sha, 0, account_filter)

    # Should get 3 transfers, 128 bytes each
    assert_equal(len(transfers_blob), 3 * 128, "Should get 3 transfers")

    # Parse and verify
    for i in range(3):
        transfer = decode_transfer(transfers_blob[i*128:(i+1)*128])
        assert_equal(transfer['debit_account_id'], 100, f"Transfer {i} debit account should match")

    # Query debits only
    account_filter = encode_account_filter(100, limit=10, flags=FILTER_DEBITS)
    transfers_blob = r.evalsha(get_transfers_sha, 0, account_filter)
    assert_equal(len(transfers_blob), 3 * 128, "Should get 3 debit transfers")

    # Query with limit
    account_filter = encode_account_filter(100, limit=2, flags=FILTER_DEBITS)
    transfers_blob = r.evalsha(get_transfers_sha, 0, account_filter)
    assert_equal(len(transfers_blob), 2 * 128, "Should respect limit=2")

    print("✓ Get account transfers query successful")

def test_get_account_balances():
    """Test get_account_balances query functionality."""
    print("\n=== Test: Get Account Balances ===")

    # Create account WITH HISTORY flag
    r.evalsha(create_account_sha, 0, encode_account(200, 700, 10, FLAG_HISTORY))
    r.evalsha(create_account_sha, 0, encode_account(201, 700, 10, 0))

    # Create transfers to generate balance history
    for i in range(2):
        transfer_data = encode_transfer(f"balance_tx_{i}", 200, 201, 150 * (i + 1), 700, 10, 0)
        result = r.evalsha(create_transfer_sha, 0, transfer_data)
        assert_equal(decode_result(result), ERR_OK, f"Transfer {i} should succeed")

    # Query balance history
    account_filter = encode_account_filter(200, limit=10, flags=0)
    balances_blob = r.evalsha(get_balances_sha, 0, account_filter)

    # Should get 2 balance snapshots, 128 bytes each
    assert_equal(len(balances_blob), 2 * 128, "Should get 2 balance snapshots")

    # Verify balances increase
    debits1 = bytes_to_u128(balances_blob, 24)  # First snapshot debits_posted
    debits2 = bytes_to_u128(balances_blob[128:], 24)  # Second snapshot debits_posted
    assert_equal(debits1, 150, "First balance should be 150")
    assert_equal(debits2, 450, "Second balance should be 450 (150 + 300)")

    # Test account without HISTORY flag returns empty
    account_filter = encode_account_filter(201, limit=10, flags=0)
    balances_blob = r.evalsha(get_balances_sha, 0, account_filter)
    assert_equal(len(balances_blob), 0, "Account without HISTORY should return empty")

    print("✓ Get account balances query successful")

def test_lookup_transfer():
    """Test transfer lookup."""
    print("\n=== Test: Lookup Transfer ===")

    # Create accounts
    r.evalsha(create_account_sha, 0, encode_account(700, 700, 10, 0))
    r.evalsha(create_account_sha, 0, encode_account(701, 700, 10, 0))

    # Create transfer
    transfer_id = "lookup_test_tx"
    transfer_data = encode_transfer(transfer_id, 700, 701, 250, 700, 10, 0)
    r.evalsha(create_transfer_sha, 0, transfer_data)

    # Lookup transfer
    transfer_id_bytes = u128_to_bytes(hash_string_to_u64(transfer_id))
    transfer_result = r.evalsha(lookup_transfer_sha, 0, transfer_id_bytes)
    transfer = decode_transfer(transfer_result)

    assert_equal(transfer['debit_account_id'], 700, "Debit account should match")
    assert_equal(transfer['credit_account_id'], 701, "Credit account should match")
    assert_equal(transfer['amount'], 250, "Amount should match")

    print("✓ Transfer lookup successful")

def test_linked_transfers():
    """Test linked transfers with LINKED flag and rollback."""
    print("\n=== Test: Linked Transfers ===")

    # Create accounts
    r.evalsha(create_account_sha, 0, encode_account(300, 700, 10, 0))
    r.evalsha(create_account_sha, 0, encode_account(301, 700, 10, 0))
    r.evalsha(create_account_sha, 0, encode_account(302, 700, 10, 0))

    # Create linked transfers (all succeed)
    transfers = b''
    transfers += encode_transfer("link1", 300, 301, 100, 700, 10, FLAG_LINKED)
    transfers += encode_transfer("link2", 301, 302, 50, 700, 10, 0)  # End of chain

    result = r.evalsha(create_linked_transfers_sha, 0, transfers)
    # Result should be empty or success indicator

    # Verify all transfers succeeded
    account1 = decode_account(r.evalsha(lookup_account_sha, 0, u128_to_bytes(300)))
    account2 = decode_account(r.evalsha(lookup_account_sha, 0, u128_to_bytes(301)))
    account3 = decode_account(r.evalsha(lookup_account_sha, 0, u128_to_bytes(302)))

    assert_equal(account1['debits_posted'], 100, "Account 300 should have debits")
    assert_equal(account2['credits_posted'], 100, "Account 301 should have credits from first")
    assert_equal(account2['debits_posted'], 50, "Account 301 should have debits from second")
    assert_equal(account3['credits_posted'], 50, "Account 302 should have credits")

    print("✓ Linked transfers successful")

def test_linked_transfers_rollback():
    """Test linked transfers rollback on error."""
    print("\n=== Test: Linked Transfers Rollback ===")

    # Create accounts
    r.evalsha(create_account_sha, 0, encode_account(800, 700, 10, 0))
    r.evalsha(create_account_sha, 0, encode_account(801, 700, 10, 0))

    # Create linked transfers where second fails (nonexistent account)
    transfers = b''
    transfers += encode_transfer("rollback1", 800, 801, 100, 700, 10, FLAG_LINKED)
    transfers += encode_transfer("rollback2", 800, 999, 50, 700, 10, 0)  # Account 999 doesn't exist

    result = r.evalsha(create_linked_transfers_sha, 0, transfers)
    # Should fail and rollback

    # Verify first transfer was rolled back (balances should be 0)
    account1 = decode_account(r.evalsha(lookup_account_sha, 0, u128_to_bytes(800)))
    assert_equal(account1['debits_posted'], 0, "Account 800 should have no debits (rolled back)")

    account2 = decode_account(r.evalsha(lookup_account_sha, 0, u128_to_bytes(801)))
    assert_equal(account2['credits_posted'], 0, "Account 801 should have no credits (rolled back)")

    print("✓ Linked transfers rollback successful")

def test_multiple_transfers():
    """Test multiple transfers between same accounts."""
    print("\n=== Test: Multiple Transfers ===")

    account_data = encode_account(80, 700, 10, 0)
    r.evalsha(create_account_sha, 0, account_data)

    account_data = encode_account(81, 700, 10, 0)
    r.evalsha(create_account_sha, 0, account_data)

    for i in range(5):
        transfer_data = encode_transfer(f"multi_transfer_{i}", 80, 81, 100, 700, 10, 0)
        result = r.evalsha(create_transfer_sha, 0, transfer_data)
        assert_equal(decode_result(result), ERR_OK, f"Transfer {i} should succeed")

    account1 = decode_account(r.evalsha(lookup_account_sha, 0, u128_to_bytes(80)))
    account2 = decode_account(r.evalsha(lookup_account_sha, 0, u128_to_bytes(81)))

    assert_equal(account1['debits_posted'], 500, "Total debits should be 500")
    assert_equal(account2['credits_posted'], 500, "Total credits should be 500")

    print("✓ Multiple transfers successful")

# Main test runner
def main():
    print("=" * 60)
    print("Lua Beetle Comprehensive Functional Tests")
    print("=" * 60)

    print("\nFlushing Redis database...")
    r.flushdb()

    try:
        # Core functionality tests
        test_create_account()
        test_duplicate_account()
        test_linked_accounts()
        test_linked_accounts_rollback()

        test_simple_transfer()
        test_transfer_nonexistent_account()

        # Two-phase transfer tests
        test_two_phase_pending()
        test_two_phase_post()
        test_two_phase_void()

        # Lookup tests
        test_lookup_transfer()

        # Query functionality tests
        test_get_account_transfers()
        test_get_account_balances()

        # Advanced tests
        test_linked_transfers()
        test_linked_transfers_rollback()
        test_multiple_transfers()

        print("\n" + "=" * 60)
        print("✅ All tests passed!")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
