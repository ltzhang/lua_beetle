package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"
)

// TestTwoPhaseTransfer tests the two-phase transfer workflow: pending -> post
func TestTwoPhaseTransfer(t *testing.T) {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer client.Close()

	encoder := NewBinaryEncoder()

	// Load scripts
	createAccountScript := mustLoadScriptTwoPhase(client, "../scripts/create_account.lua")
	createTransferScript := mustLoadScriptTwoPhase(client, "../scripts/create_transfer.lua")
	lookupAccountScript := mustLoadScriptTwoPhase(client, "../scripts/lookup_account.lua")

	var accountID1 uint64 = 201
	var accountID2 uint64 = 202

	// Flush database
	client.FlushDB(ctx)

	// Create accounts with sufficient balance
	for _, id := range []uint64{accountID1, accountID2} {
		accountData, err := encoder.EncodeAccount(id, 1, 1, 0)
		if err != nil {
			t.Fatalf("Failed to encode account: %v", err)
		}
		result, err := client.EvalSha(ctx, createAccountScript, []string{}, accountData).Result()
		if err != nil {
			t.Fatalf("Failed to create account: %v", err)
		}
		errCode, _ := encoder.DecodeTransferResult(result)
		if errCode != 0 {
			t.Fatalf("Failed to create account, error code: %d", errCode)
		}
	}

	// Phase 1: Create a pending transfer
	pendingTransferID := "pending_txn_001"
	transferAmount := uint64(500)

	transferData, err := encoder.EncodeTransfer(pendingTransferID, accountID1, accountID2, transferAmount, 1, 1, 0x0002) // PENDING flag
	if err != nil {
		t.Fatalf("Failed to encode pending transfer: %v", err)
	}

	result, err := client.EvalSha(ctx, createTransferScript, []string{}, transferData).Result()
	if err != nil {
		t.Fatalf("Failed to create pending transfer: %v", err)
	}
	errCode, _ := encoder.DecodeTransferResult(result)
	if errCode != 0 {
		t.Fatalf("Failed to create pending transfer, error code: %d", errCode)
	}

	fmt.Println("✓ Created pending transfer")

	// Verify pending balances
	account1Data, _ := client.EvalSha(ctx, lookupAccountScript, []string{}, U64ToID16(accountID1)).Result()
	account2Data, _ := client.EvalSha(ctx, lookupAccountScript, []string{}, U64ToID16(accountID2)).Result()

	debitsPending1 := decodeU128TwoPhase(account1Data.(string), 16)
	creditsPending2 := decodeU128TwoPhase(account2Data.(string), 48)
	debitsPosted1 := decodeU128TwoPhase(account1Data.(string), 32)
	creditsPosted2 := decodeU128TwoPhase(account2Data.(string), 64)

	if debitsPending1 != transferAmount {
		t.Fatalf("Expected account 1 debits_pending=%d, got %d", transferAmount, debitsPending1)
	}
	if creditsPending2 != transferAmount {
		t.Fatalf("Expected account 2 credits_pending=%d, got %d", transferAmount, creditsPending2)
	}
	if debitsPosted1 != 0 {
		t.Fatalf("Expected account 1 debits_posted=0, got %d", debitsPosted1)
	}
	if creditsPosted2 != 0 {
		t.Fatalf("Expected account 2 credits_posted=0, got %d", creditsPosted2)
	}

	fmt.Println("✓ Verified pending balances")

	// Phase 2: Post the pending transfer
	postTransferID := "post_txn_001"

	transferData, err = encoder.EncodeTransferWithPending(postTransferID, accountID1, accountID2, transferAmount, pendingTransferID, 1, 1, 0x0004) // POST_PENDING flag
	if err != nil {
		t.Fatalf("Failed to encode post transfer: %v", err)
	}

	result, err = client.EvalSha(ctx, createTransferScript, []string{}, transferData).Result()
	if err != nil {
		t.Fatalf("Failed to post transfer: %v", err)
	}
	errCode, _ = encoder.DecodeTransferResult(result)
	if errCode != 0 {
		t.Fatalf("Failed to post transfer, error code: %d", errCode)
	}

	fmt.Println("✓ Posted pending transfer")

	// Verify posted balances
	account1Data, _ = client.EvalSha(ctx, lookupAccountScript, []string{}, U64ToID16(accountID1)).Result()
	account2Data, _ = client.EvalSha(ctx, lookupAccountScript, []string{}, U64ToID16(accountID2)).Result()

	debitsPending1 = decodeU128TwoPhase(account1Data.(string), 16)
	creditsPending2 = decodeU128TwoPhase(account2Data.(string), 48)
	debitsPosted1 = decodeU128TwoPhase(account1Data.(string), 32)
	creditsPosted2 = decodeU128TwoPhase(account2Data.(string), 64)

	if debitsPending1 != 0 {
		t.Fatalf("Expected account 1 debits_pending=0, got %d", debitsPending1)
	}
	if creditsPending2 != 0 {
		t.Fatalf("Expected account 2 credits_pending=0, got %d", creditsPending2)
	}
	if debitsPosted1 != transferAmount {
		t.Fatalf("Expected account 1 debits_posted=%d, got %d", transferAmount, debitsPosted1)
	}
	if creditsPosted2 != transferAmount {
		t.Fatalf("Expected account 2 credits_posted=%d, got %d", transferAmount, creditsPosted2)
	}

	fmt.Println("✓ Verified posted balances")
	fmt.Println("\n✅ Two-phase transfer test passed!")
}

// TestVoidPendingTransfer tests voiding a pending transfer
func TestVoidPendingTransfer(t *testing.T) {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer client.Close()

	encoder := NewBinaryEncoder()

	createAccountScript := mustLoadScriptTwoPhase(client, "../scripts/create_account.lua")
	createTransferScript := mustLoadScriptTwoPhase(client, "../scripts/create_transfer.lua")
	lookupAccountScript := mustLoadScriptTwoPhase(client, "../scripts/lookup_account.lua")

	var accountID1 uint64 = 203
	var accountID2 uint64 = 204

	client.FlushDB(ctx)

	// Create accounts
	for _, id := range []uint64{accountID1, accountID2} {
		accountData, _ := encoder.EncodeAccount(id, 1, 1, 0)
		client.EvalSha(ctx, createAccountScript, []string{}, accountData)
	}

	// Create a pending transfer
	pendingTransferID := "pending_void_001"
	transferAmount := uint64(300)

	transferData, _ := encoder.EncodeTransfer(pendingTransferID, accountID1, accountID2, transferAmount, 1, 1, 0x0002)
	result, err := client.EvalSha(ctx, createTransferScript, []string{}, transferData).Result()
	if err != nil {
		t.Fatalf("Failed to create pending transfer: %v", err)
	}
	errCode, _ := encoder.DecodeTransferResult(result)
	if errCode != 0 {
		t.Fatalf("Failed to create pending transfer, error code: %d", errCode)
	}

	fmt.Println("✓ Created pending transfer")

	// Void the pending transfer
	voidTransferID := "void_txn_001"

	transferData, _ = encoder.EncodeTransferWithPending(voidTransferID, accountID1, accountID2, transferAmount, pendingTransferID, 1, 1, 0x0008) // VOID_PENDING flag
	result, err = client.EvalSha(ctx, createTransferScript, []string{}, transferData).Result()
	if err != nil {
		t.Fatalf("Failed to void transfer: %v", err)
	}
	errCode, _ = encoder.DecodeTransferResult(result)
	if errCode != 0 {
		t.Fatalf("Failed to void transfer, error code: %d", errCode)
	}

	fmt.Println("✓ Voided pending transfer")

	// Verify all balances are zero
	account1Data, _ := client.EvalSha(ctx, lookupAccountScript, []string{}, U64ToID16(accountID1)).Result()
	account2Data, _ := client.EvalSha(ctx, lookupAccountScript, []string{}, U64ToID16(accountID2)).Result()

	debitsPending1 := decodeU128TwoPhase(account1Data.(string), 16)
	creditsPending2 := decodeU128TwoPhase(account2Data.(string), 48)
	debitsPosted1 := decodeU128TwoPhase(account1Data.(string), 32)
	creditsPosted2 := decodeU128TwoPhase(account2Data.(string), 64)

	if debitsPending1 != 0 || creditsPending2 != 0 || debitsPosted1 != 0 || creditsPosted2 != 0 {
		t.Fatalf("Expected all balances to be 0, got: pending1=%d, pending2=%d, posted1=%d, posted2=%d",
			debitsPending1, creditsPending2, debitsPosted1, creditsPosted2)
	}

	fmt.Println("✓ Verified all balances are zero")
	fmt.Println("\n✅ Void pending transfer test passed!")
}

// Helper functions
func mustLoadScriptTwoPhase(client *redis.Client, path string) string {
	script, err := loadScript(path)
	if err != nil {
		panic(fmt.Sprintf("Failed to load script %s: %v", path, err))
	}
	sha, err := client.ScriptLoad(context.Background(), script).Result()
	if err != nil {
		panic(fmt.Sprintf("Failed to load script %s into Redis: %v", path, err))
	}
	return sha
}

func decodeU128TwoPhase(data string, offset int) uint64 {
	if len(data) < offset+8 {
		return 0
	}
	bytes := []byte(data)
	val := uint64(0)
	for i := 0; i < 8; i++ {
		val += uint64(bytes[offset+i]) << (i * 8)
	}
	return val
}
