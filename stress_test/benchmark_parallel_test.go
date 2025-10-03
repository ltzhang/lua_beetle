package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9"
)

func assertParallel(tb testing.TB, a, b interface{}, field string) {
	tb.Helper()
	if fmt.Sprintf("%v", a) != fmt.Sprintf("%v", b) {
		tb.Fatalf("Expected %s to be [%+v (%T)], got: [%+v (%T)]", field, b, b, a, a)
	}
}

// BenchmarkBasicLuaBeetleParallel - Parallel transfers with verification
func BenchmarkBasicLuaBeetleParallel(b *testing.B) {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer client.Close()

	encoder := NewBinaryEncoder()

	createAccountScript := mustLoadScriptParallel(client, "../scripts/create_account.lua")
	createTransferScript := mustLoadScriptParallel(client, "../scripts/create_transfer.lua")
	lookupAccountScript := mustLoadScriptParallel(client, "../scripts/lookup_account.lua")

	var accountID1 uint64 = 101
	var accountID2 uint64 = 102

	// Flush database to start fresh
	client.FlushDB(ctx)

	// Create accounts
	for _, id := range []uint64{accountID1, accountID2} {
		accountData, err := encoder.EncodeAccount(id, 1, 1, 0)
		if err != nil {
			b.Fatalf("Failed to encode account: %v", err)
		}
		result, err := client.EvalSha(ctx, createAccountScript, []string{}, accountData).Result()
		if err != nil {
			b.Fatalf("Could not create accounts: %v", err)
		}
		errCode, _ := encoder.DecodeTransferResult(result)
		if errCode != 0 {
			b.Fatalf("Could not create account, error code: %d", errCode)
		}
	}

	b.ResetTimer()

	var nextTxID uint64 = 1000000
	var totalTransferred uint64 = 0

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			currentTxID := atomic.AddUint64(&nextTxID, 1)
			transferID := fmt.Sprintf("benchpar1_%d", currentTxID)
			transferData, err := encoder.EncodeTransfer(transferID, accountID1, accountID2, 10, 1, 1, 0)
			if err != nil {
				b.Errorf("Error encoding transfer: %v", err)
				continue
			}

			result, err := client.EvalSha(ctx, createTransferScript, []string{}, transferData).Result()
			if err != nil {
				b.Errorf("Error creating transfer: %v", err)
				continue
			}

			errCode, _ := encoder.DecodeTransferResult(result)
			if errCode != 0 {
				b.Errorf("Error creating transfer, code: %d", errCode)
				continue
			}

			atomic.AddUint64(&totalTransferred, 10)
		}
	})

	b.StopTimer()

	finalTotalTransferred := atomic.LoadUint64(&totalTransferred)

	// Verify final balances
	account1Data, err := client.EvalSha(ctx, lookupAccountScript, []string{}, U64ToID16(accountID1)).Result()
	if err != nil {
		b.Fatalf("Could not fetch accounts post-benchmark: %v", err)
	}
	account2Data, err := client.EvalSha(ctx, lookupAccountScript, []string{}, U64ToID16(accountID2)).Result()
	if err != nil {
		b.Fatalf("Could not fetch accounts post-benchmark: %v", err)
	}

	debitsPosted1 := decodeU128Parallel(account1Data.(string), 32)
	creditsPosted2 := decodeU128Parallel(account2Data.(string), 64)

	assertParallel(b, debitsPosted1, finalTotalTransferred, "account 1 debits")
	assertParallel(b, creditsPosted2, finalTotalTransferred, "account 2 credits")
}

// BenchmarkBasicBatchLuaBeetleParallel - Parallel batched transfers
func BenchmarkBasicBatchLuaBeetleParallel(b *testing.B) {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer client.Close()

	encoder := NewBinaryEncoder()

	createAccountScript := mustLoadScriptParallel(client, "../scripts/create_account.lua")
	createTransferScript := mustLoadScriptParallel(client, "../scripts/create_transfer.lua")
	lookupAccountScript := mustLoadScriptParallel(client, "../scripts/lookup_account.lua")

	var accountID1 uint64 = 103
	var accountID2 uint64 = 104

	// Flush database to start fresh
	client.FlushDB(ctx)

	// Create accounts
	for _, id := range []uint64{accountID1, accountID2} {
		accountData, _ := encoder.EncodeAccount(id, 1, 1, 0)
		client.EvalSha(ctx, createAccountScript, []string{}, accountData)
	}

	b.ResetTimer()

	var nextTxID uint64 = 2000000
	var totalTransferred uint64 = 0

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pipe := client.Pipeline()

			// Create 100 transfers in pipeline per iteration
			for j := 0; j < 100; j++ {
				currentTxID := atomic.AddUint64(&nextTxID, 1)
				transferID := fmt.Sprintf("benchpar2_%d", currentTxID)
				transferData, _ := encoder.EncodeTransfer(transferID, accountID1, accountID2, 10, 1, 1, 0)
				pipe.EvalSha(ctx, createTransferScript, []string{}, transferData)
				atomic.AddUint64(&totalTransferred, 10)
			}

			results, err := pipe.Exec(ctx)
			if err != nil {
				b.Errorf("Pipeline failed: %v", err)
				continue
			}

			// Check for errors
			for _, result := range results {
				if result.Err() != nil {
					b.Errorf("Transfer failed: %v", result.Err())
					break
				}
			}
		}
	})

	b.StopTimer()

	// Report custom metrics: transfers per operation
	b.ReportMetric(float64(100), "transfers/op")

	finalTotalTransferred := atomic.LoadUint64(&totalTransferred)

	// Verify final balances
	account1Data, _ := client.EvalSha(ctx, lookupAccountScript, []string{}, U64ToID16(accountID1)).Result()
	account2Data, _ := client.EvalSha(ctx, lookupAccountScript, []string{}, U64ToID16(accountID2)).Result()

	debitsPosted1 := decodeU128Parallel(account1Data.(string), 32)
	creditsPosted2 := decodeU128Parallel(account2Data.(string), 64)

	assertParallel(b, debitsPosted1, finalTotalTransferred, "account 1 debits")
	assertParallel(b, creditsPosted2, finalTotalTransferred, "account 2 credits")
}

// Helper functions

func mustLoadScriptParallel(client *redis.Client, path string) string {
	script, err := loadScriptParallel(path)
	if err != nil {
		panic(fmt.Sprintf("Failed to load script %s: %v", path, err))
	}
	sha, err := client.ScriptLoad(context.Background(), script).Result()
	if err != nil {
		panic(fmt.Sprintf("Failed to load script %s into Redis: %v", path, err))
	}
	return sha
}

func loadScriptParallel(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func decodeU128Parallel(data string, offset int) uint64 {
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
