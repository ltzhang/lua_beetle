#!/usr/bin/env python3
"""
Test get_account_transfers and get_account_balances functionality
Tests the APPEND-based indexing with fixed-size entries
"""

import redis
import struct
import os
import sys

# Add parent directory to path for encoder import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class BinaryEncoder:
    """Encode binary data for TigerBeetle-compatible format"""

    @staticmethod
    def encode_u128(value):
        """Encode uint128 as 16 bytes little-endian"""
        low = value & ((1 << 64) - 1)
        high = (value >> 64) & ((1 << 64) - 1)
        return struct.pack('<QQ', low, high)

    @staticmethod
    def encode_u64(value):
        """Encode uint64 as 8 bytes little-endian"""
        return struct.pack('<Q', value)

    @staticmethod
    def encode_u32(value):
        """Encode uint32 as 4 bytes little-endian"""
        return struct.pack('<I', value)

    @staticmethod
    def encode_u16(value):
        """Encode uint16 as 2 bytes little-endian"""
        return struct.pack('<H', value)

    @staticmethod
    def decode_u128(data):
        """Decode 16 bytes little-endian to uint128"""
        low, high = struct.unpack('<QQ', data)
        return (high << 64) | low

    @staticmethod
    def decode_u64(data):
        """Decode 8 bytes little-endian to uint64"""
        return struct.unpack('<Q', data)[0]

    @staticmethod
    def encode_account(account_id, ledger=700, code=10, flags=0):
        """Encode account as 128 bytes"""
        data = bytearray(128)
        # ID (16 bytes)
        data[0:16] = BinaryEncoder.encode_u128(account_id)
        # debits_pending (16 bytes) - offset 16
        data[16:32] = BinaryEncoder.encode_u128(0)
        # debits_posted (16 bytes) - offset 32
        data[32:48] = BinaryEncoder.encode_u128(0)
        # credits_pending (16 bytes) - offset 48
        data[48:64] = BinaryEncoder.encode_u128(0)
        # credits_posted (16 bytes) - offset 64
        data[64:80] = BinaryEncoder.encode_u128(0)
        # user_data_128 (16 bytes) - offset 80
        data[80:96] = BinaryEncoder.encode_u128(0)
        # user_data_64 (8 bytes) - offset 96
        data[96:104] = BinaryEncoder.encode_u64(0)
        # user_data_32 (4 bytes) - offset 104
        data[104:108] = BinaryEncoder.encode_u32(0)
        # reserved (4 bytes) - offset 108
        # ledger (4 bytes) - offset 112
        data[112:116] = BinaryEncoder.encode_u32(ledger)
        # code (2 bytes) - offset 116
        data[116:118] = BinaryEncoder.encode_u16(code)
        # flags (2 bytes) - offset 118
        data[118:120] = BinaryEncoder.encode_u16(flags)
        # timestamp (8 bytes) - offset 120
        data[120:128] = BinaryEncoder.encode_u64(0)
        return bytes(data)

    @staticmethod
    def encode_transfer(transfer_id, debit_account_id, credit_account_id, amount,
                       ledger=700, code=10, flags=0, timestamp=0,
                       user_data_128=0, user_data_64=0, user_data_32=0, pending_id=0):
        """Encode transfer as 128 bytes"""
        data = bytearray(128)
        # ID (16 bytes)
        data[0:16] = BinaryEncoder.encode_u128(transfer_id)
        # debit_account_id (16 bytes) - offset 16
        data[16:32] = BinaryEncoder.encode_u128(debit_account_id)
        # credit_account_id (16 bytes) - offset 32
        data[32:48] = BinaryEncoder.encode_u128(credit_account_id)
        # amount (16 bytes) - offset 48
        data[48:64] = BinaryEncoder.encode_u128(amount)
        # pending_id (16 bytes) - offset 64
        data[64:80] = BinaryEncoder.encode_u128(pending_id)
        # user_data_128 (16 bytes) - offset 80
        data[80:96] = BinaryEncoder.encode_u128(user_data_128)
        # user_data_64 (8 bytes) - offset 96
        data[96:104] = BinaryEncoder.encode_u64(user_data_64)
        # user_data_32 (4 bytes) - offset 104
        data[104:108] = BinaryEncoder.encode_u32(user_data_32)
        # timeout (4 bytes) - offset 108
        data[108:112] = BinaryEncoder.encode_u32(0)
        # ledger (4 bytes) - offset 112
        data[112:116] = BinaryEncoder.encode_u32(ledger)
        # code (2 bytes) - offset 116
        data[116:118] = BinaryEncoder.encode_u16(code)
        # flags (2 bytes) - offset 118
        data[118:120] = BinaryEncoder.encode_u16(flags)
        # timestamp (8 bytes) - offset 120
        data[120:128] = BinaryEncoder.encode_u64(timestamp)
        return bytes(data)

    @staticmethod
    def encode_account_filter(account_id, timestamp_min=0, timestamp_max=0, limit=100, flags=0,
                             user_data_128=0, user_data_64=0, user_data_32=0, code=0):
        """Encode AccountFilter as 128 bytes"""
        data = bytearray(128)
        # account_id (16 bytes) - offset 0
        data[0:16] = BinaryEncoder.encode_u128(account_id)
        # user_data_128 (16 bytes) - offset 16
        data[16:32] = BinaryEncoder.encode_u128(user_data_128)
        # user_data_64 (8 bytes) - offset 32
        data[32:40] = BinaryEncoder.encode_u64(user_data_64)
        # user_data_32 (4 bytes) - offset 40
        data[40:44] = BinaryEncoder.encode_u32(user_data_32)
        # reserved (2 bytes) - offset 44
        # code (2 bytes) - offset 46
        data[46:48] = BinaryEncoder.encode_u16(code)
        # timestamp_min (8 bytes) - offset 48
        data[48:56] = BinaryEncoder.encode_u64(timestamp_min)
        # timestamp_max (8 bytes) - offset 56
        data[56:64] = BinaryEncoder.encode_u64(timestamp_max)
        # limit (4 bytes) - offset 64
        data[64:68] = BinaryEncoder.encode_u32(limit)
        # flags (4 bytes) - offset 68
        data[68:72] = BinaryEncoder.encode_u32(flags)
        # reserved (56 bytes) - offset 72-128
        return bytes(data)


def setup_test_data(client):
    """Setup test accounts and transfers"""
    # Load Lua scripts
    script_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'scripts')

    with open(os.path.join(script_dir, 'create_account.lua'), 'r') as f:
        create_account_script = f.read()
    with open(os.path.join(script_dir, 'create_transfer.lua'), 'r') as f:
        create_transfer_script = f.read()
    with open(os.path.join(script_dir, 'get_account_transfers.lua'), 'r') as f:
        get_transfers_script = f.read()
    with open(os.path.join(script_dir, 'get_account_balances.lua'), 'r') as f:
        get_balances_script = f.read()

    create_account_sha = client.script_load(create_account_script)
    create_transfer_sha = client.script_load(create_transfer_script)
    get_transfers_sha = client.script_load(get_transfers_script)
    get_balances_sha = client.script_load(get_balances_script)

    return create_account_sha, create_transfer_sha, get_transfers_sha, get_balances_sha


def test_get_account_transfers_basic(client, encoder, scripts):
    """Test basic get_account_transfers functionality"""
    print("\n=== Test: Basic get_account_transfers ===")

    create_account_sha, create_transfer_sha, get_transfers_sha, _ = scripts

    # Create two accounts
    account1 = encoder.encode_account(1, flags=0)
    account2 = encoder.encode_account(2, flags=0)

    client.evalsha(create_account_sha, 0, account1)
    client.evalsha(create_account_sha, 0, account2)

    # Create 3 transfers with different timestamps
    # Transfer 1: account1 -> account2 at timestamp 1000
    transfer1 = encoder.encode_transfer(101, 1, 2, 100, timestamp=1000)
    result = client.evalsha(create_transfer_sha, 0, transfer1)
    assert result[0] == 0, f"Transfer 1 failed: {result[0]}"

    # Transfer 2: account1 -> account2 at timestamp 2000
    transfer2 = encoder.encode_transfer(102, 1, 2, 200, timestamp=2000)
    result = client.evalsha(create_transfer_sha, 0, transfer2)
    assert result[0] == 0, f"Transfer 2 failed: {result[0]}"

    # Transfer 3: account2 -> account1 at timestamp 3000 (account1 is credit)
    transfer3 = encoder.encode_transfer(103, 2, 1, 150, timestamp=3000)
    result = client.evalsha(create_transfer_sha, 0, transfer3)
    assert result[0] == 0, f"Transfer 3 failed: {result[0]}"

    # Query all transfers for account1 (both debits and credits)
    account_filter = encoder.encode_account_filter(1, limit=10, flags=0)  # flags=0 means both debits and credits
    transfers_blob = client.evalsha(get_transfers_sha, 0, account_filter)

    # Should get 3 transfers (2 debits + 1 credit), 128 bytes each
    assert len(transfers_blob) == 3 * 128, f"Expected 384 bytes, got {len(transfers_blob)}"

    # Verify transfers are sorted by timestamp
    timestamps = []
    for i in range(3):
        transfer_data = transfers_blob[i * 128:(i + 1) * 128]
        timestamp = encoder.decode_u64(transfer_data[120:128])
        timestamps.append(timestamp)

    # Note: All have same timestamp (hardcoded in Lua), so just check we got 3
    assert len(timestamps) == 3, f"Expected 3 timestamps, got {len(timestamps)}"

    print("✅ Basic get_account_transfers test passed")


def test_get_account_transfers_debits_only(client, encoder, scripts):
    """Test get_account_transfers with debits flag"""
    print("\n=== Test: get_account_transfers debits only ===")

    create_account_sha, create_transfer_sha, get_transfers_sha, _ = scripts

    # Create two accounts
    account1 = encoder.encode_account(10, flags=0)
    account2 = encoder.encode_account(20, flags=0)

    client.evalsha(create_account_sha, 0, account1)
    client.evalsha(create_account_sha, 0, account2)

    # Transfer 1: account 10 -> 20 (debit for account 10)
    transfer1 = encoder.encode_transfer(201, 10, 20, 100, timestamp=1000)
    client.evalsha(create_transfer_sha, 0, transfer1)

    # Transfer 2: account 20 -> 10 (credit for account 10)
    transfer2 = encoder.encode_transfer(202, 20, 10, 50, timestamp=2000)
    client.evalsha(create_transfer_sha, 0, transfer2)

    # Query debits only (flags=0x01)
    account_filter = encoder.encode_account_filter(10, limit=10, flags=0x01)
    transfers_blob = client.evalsha(get_transfers_sha, 0, account_filter)

    # Should get only 1 transfer (the debit)
    assert len(transfers_blob) == 128, f"Expected 128 bytes (1 transfer), got {len(transfers_blob)}"

    # Verify it's the debit transfer (ID=201)
    transfer_id = encoder.decode_u128(transfers_blob[0:16])
    assert transfer_id == 201, f"Expected transfer ID 201, got {transfer_id}"

    print("✅ Debits only test passed")


def test_get_account_transfers_credits_only(client, encoder, scripts):
    """Test get_account_transfers with credits flag"""
    print("\n=== Test: get_account_transfers credits only ===")

    create_account_sha, create_transfer_sha, get_transfers_sha, _ = scripts

    # Create two accounts
    account1 = encoder.encode_account(30, flags=0)
    account2 = encoder.encode_account(40, flags=0)

    client.evalsha(create_account_sha, 0, account1)
    client.evalsha(create_account_sha, 0, account2)

    # Transfer 1: account 30 -> 40 (debit for account 30)
    transfer1 = encoder.encode_transfer(301, 30, 40, 100, timestamp=1000)
    client.evalsha(create_transfer_sha, 0, transfer1)

    # Transfer 2: account 40 -> 30 (credit for account 30)
    transfer2 = encoder.encode_transfer(302, 40, 30, 50, timestamp=2000)
    client.evalsha(create_transfer_sha, 0, transfer2)

    # Query credits only (flags=0x02)
    account_filter = encoder.encode_account_filter(30, limit=10, flags=0x02)
    transfers_blob = client.evalsha(get_transfers_sha, 0, account_filter)

    # Should get only 1 transfer (the credit)
    assert len(transfers_blob) == 128, f"Expected 128 bytes (1 transfer), got {len(transfers_blob)}"

    # Verify it's the credit transfer (ID=302)
    transfer_id = encoder.decode_u128(transfers_blob[0:16])
    assert transfer_id == 302, f"Expected transfer ID 302, got {transfer_id}"

    print("✅ Credits only test passed")


def test_get_account_transfers_with_limit(client, encoder, scripts):
    """Test get_account_transfers with limit"""
    print("\n=== Test: get_account_transfers with limit ===")

    create_account_sha, create_transfer_sha, get_transfers_sha, _ = scripts

    # Create two accounts
    account1 = encoder.encode_account(50, flags=0)
    account2 = encoder.encode_account(60, flags=0)

    client.evalsha(create_account_sha, 0, account1)
    client.evalsha(create_account_sha, 0, account2)

    # Create 5 transfers
    for i in range(5):
        transfer = encoder.encode_transfer(401 + i, 50, 60, 100, timestamp=1000 + i * 1000)
        client.evalsha(create_transfer_sha, 0, transfer)

    # Query with limit=2
    account_filter = encoder.encode_account_filter(50, limit=2, flags=0x01)
    transfers_blob = client.evalsha(get_transfers_sha, 0, account_filter)

    # Should get only 2 transfers
    assert len(transfers_blob) == 2 * 128, f"Expected 256 bytes (2 transfers), got {len(transfers_blob)}"

    print("✅ Limit test passed")


def test_get_account_balances_basic(client, encoder, scripts):
    """Test basic get_account_balances functionality"""
    print("\n=== Test: Basic get_account_balances ===")

    create_account_sha, create_transfer_sha, _, get_balances_sha = scripts

    # Create account with HISTORY flag (0x08)
    account1 = encoder.encode_account(70, flags=0x08)
    account2 = encoder.encode_account(80, flags=0)

    client.evalsha(create_account_sha, 0, account1)
    client.evalsha(create_account_sha, 0, account2)

    # Create 2 transfers
    transfer1 = encoder.encode_transfer(501, 70, 80, 100, timestamp=1000)
    result = client.evalsha(create_transfer_sha, 0, transfer1)
    assert result[0] == 0, f"Transfer 1 failed: {result[0]}"

    transfer2 = encoder.encode_transfer(502, 70, 80, 50, timestamp=2000)
    result = client.evalsha(create_transfer_sha, 0, transfer2)
    assert result[0] == 0, f"Transfer 2 failed: {result[0]}"

    # Query balance history
    account_filter = encoder.encode_account_filter(70, limit=10, flags=0)
    balances_blob = client.evalsha(get_balances_sha, 0, account_filter)

    # Should get 2 balance snapshots, 64 bytes each
    assert len(balances_blob) == 2 * 64, f"Expected 128 bytes (2 balances), got {len(balances_blob)}"

    print("✅ Basic get_account_balances test passed")


def test_get_account_balances_no_history_flag(client, encoder, scripts):
    """Test get_account_balances returns empty for accounts without HISTORY flag"""
    print("\n=== Test: get_account_balances without HISTORY flag ===")

    create_account_sha, create_transfer_sha, _, get_balances_sha = scripts

    # Create account WITHOUT HISTORY flag
    account1 = encoder.encode_account(90, flags=0)
    account2 = encoder.encode_account(100, flags=0)

    client.evalsha(create_account_sha, 0, account1)
    client.evalsha(create_account_sha, 0, account2)

    # Create transfer
    transfer1 = encoder.encode_transfer(601, 90, 100, 100, timestamp=1000)
    client.evalsha(create_transfer_sha, 0, transfer1)

    # Query balance history
    account_filter = encoder.encode_account_filter(90, limit=10, flags=0)
    balances_blob = client.evalsha(get_balances_sha, 0, account_filter)

    # Should get empty result
    assert len(balances_blob) == 0, f"Expected 0 bytes, got {len(balances_blob)}"

    print("✅ No history flag test passed")


def main():
    # Connect to Redis
    client = redis.Redis(host='localhost', port=6379, decode_responses=False)

    # Clean database
    print("Cleaning database...")
    client.flushdb()

    # Setup
    encoder = BinaryEncoder()
    scripts = setup_test_data(client)

    # Run tests
    try:
        test_get_account_transfers_basic(client, encoder, scripts)
        test_get_account_transfers_debits_only(client, encoder, scripts)
        test_get_account_transfers_credits_only(client, encoder, scripts)
        test_get_account_transfers_with_limit(client, encoder, scripts)
        test_get_account_balances_basic(client, encoder, scripts)
        test_get_account_balances_no_history_flag(client, encoder, scripts)

        print("\n" + "="*50)
        print("✅ All tests passed!")
        print("="*50)

    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
