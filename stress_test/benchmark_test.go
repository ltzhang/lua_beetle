package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"reflect"
	"testing"

	"github.com/redis/go-redis/v9"
)

func assert(tb testing.TB, a, b interface{}, field string) {
	tb.Helper()
	if !reflect.DeepEqual(a, b) {
		log.Fatalf("Expected %s to be [%+v (%T)], got: [%+v (%T)]", field, b, b, a, a)
	}
}

// BenchmarkBasicLuaBeetle - Single transfer per iteration with verification
func BenchmarkBasicLuaBeetle(b *testing.B) {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer client.Close()

	// Use binary encoder
	encoder := NewBinaryEncoder()

	// Load scripts
	createAccountScript := mustLoadScript(client, "../scripts/create_account.lua")
	createTransferScript := mustLoadScript(client, "../scripts/create_transfer.lua")
	lookupAccountScript := mustLoadScript(client, "../scripts/lookup_account.lua")

	var accountID1 uint64 = 1
	var accountID2 uint64 = 2

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
			b.Fatalf("Failed to create account: %v", err)
		}
		errCode, _ := encoder.DecodeTransferResult(result)
		if errCode != 0 {
			b.Fatalf("Failed to create account, error code: %d", errCode)
		}
	}

	b.ResetTimer()

	var totalTransferred uint64 = 0
	for i := 0; i < b.N; i++ {
		transferID := fmt.Sprintf("bench1_%d", i)
		transferData, err := encoder.EncodeTransfer(transferID, accountID1, accountID2, 10, 1, 1, 0)
		if err != nil {
			b.Fatalf("Error encoding transfer: %v", err)
		}

		result, err := client.EvalSha(ctx, createTransferScript, []string{}, transferData).Result()
		if err != nil {
			b.Fatalf("Error creating transfer: %v", err)
		}

		errCode, _ := encoder.DecodeTransferResult(result)
		if errCode != 0 {
			b.Fatalf("Error creating transfer, code: %d", errCode)
		}

		totalTransferred += 10
	}

	b.StopTimer()

	// Verify balances at the end
	account1Data, err := client.EvalSha(ctx, lookupAccountScript, []string{}, U64ToID16(accountID1)).Result()
	if err != nil {
		b.Fatalf("Could not fetch account 1: %v", err)
	}
	account2Data, err := client.EvalSha(ctx, lookupAccountScript, []string{}, U64ToID16(accountID2)).Result()
	if err != nil {
		b.Fatalf("Could not fetch account 2: %v", err)
	}

	// Verify balances (debits_posted at offset 32, credits_posted at offset 64)
	debits1 := decodeU128(account1Data.(string), 32)
	credits2 := decodeU128(account2Data.(string), 64)

	assert(b, debits1, totalTransferred, "account 1 debits")
	assert(b, credits2, totalTransferred, "account 2 credits")
}

// TODO: BenchmarkTwoPhaseLuaBeetle - Requires pending_id support in binary encoder

// BenchmarkBasicBatchLuaBeetle - 1000 transfers per iteration using pipeline
func BenchmarkBasicBatchLuaBeetle(b *testing.B) {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer client.Close()

	encoder := NewBinaryEncoder()

	createAccountScript := mustLoadScript(client, "../scripts/create_account.lua")
	createTransferScript := mustLoadScript(client, "../scripts/create_transfer.lua")
	lookupAccountScript := mustLoadScript(client, "../scripts/lookup_account.lua")

	var accountID1 uint64 = 5
	var accountID2 uint64 = 6

	// Flush database to start fresh
	client.FlushDB(ctx)

	// Create accounts
	for _, id := range []uint64{accountID1, accountID2} {
		accountData, _ := encoder.EncodeAccount(id, 1, 1, 0)
		client.EvalSha(ctx, createAccountScript, []string{}, accountData)
	}

	b.ResetTimer()

	var totalTransferredBatch uint64 = 0

	for i := 0; i < b.N; i++ {
		pipe := client.Pipeline()

		// Create 1000 transfers in pipeline
		for j := 0; j < 1000; j++ {
			transferID := fmt.Sprintf("bench3_%d_%d", i, j)
			transferData, _ := encoder.EncodeTransfer(transferID, accountID1, accountID2, 10, 1, 1, 0)
			pipe.EvalSha(ctx, createTransferScript, []string{}, transferData)
			totalTransferredBatch += 10
		}

		results, err := pipe.Exec(ctx)
		if err != nil {
			b.Fatalf("Pipeline failed: %v", err)
		}

		// Check for errors
		for idx, result := range results {
			if result.Err() != nil {
				b.Fatalf("Transfer %d failed: %v", idx, result.Err())
			}
			// Check error code from script
			if cmd, ok := result.(*redis.Cmd); ok {
				scriptResult, _ := cmd.Result()
				errCode, _ := encoder.DecodeTransferResult(scriptResult)
				if errCode != 0 {
					b.Fatalf("Transfer %d returned error code: %d", idx, errCode)
				}
			}
		}
	}

	b.StopTimer()

	// Report custom metrics: transfers per operation
	b.ReportMetric(float64(1000), "transfers/op")

	// Verify balances at the end
	account1Data, _ := client.EvalSha(ctx, lookupAccountScript, []string{}, U64ToID16(accountID1)).Result()
	account2Data, _ := client.EvalSha(ctx, lookupAccountScript, []string{}, U64ToID16(accountID2)).Result()

	debitsPosted1 := decodeU128(account1Data.(string), 32)
	creditsPosted2 := decodeU128(account2Data.(string), 64)

	assert(b, debitsPosted1, totalTransferredBatch, "account 1 debits")
	assert(b, creditsPosted2, totalTransferredBatch, "account 2 credits")
}

// Helper functions

func mustLoadScript(client *redis.Client, path string) string {
	script, err := loadScript(path)
	if err != nil {
		log.Fatalf("Failed to load script %s: %v", path, err)
	}
	sha, err := client.ScriptLoad(context.Background(), script).Result()
	if err != nil {
		log.Fatalf("Failed to load script %s into Redis: %v", path, err)
	}
	return sha
}

func loadScript(path string) (string, error) {
	content, err := readFile(path)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

func readFile(path string) ([]byte, error) {
	// Use os.ReadFile
	data, err := os.ReadFile(path)
	return data, err
}

func decodeU128(data string, offset int) uint64 {
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
