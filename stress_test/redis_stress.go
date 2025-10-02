package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStressTest implements stress testing for Lua Beetle on Redis/DragonflyDB
type RedisStressTest struct {
	client                   *redis.Client
	config                   *StressTestConfig
	metrics                  *TestMetrics
	name                     string
	createAccountsSHA        string
	createTransfersSHA       string
	lookupAccountsSHA        string
	getAccountTransfersSHA   string
	getAccountBalancesSHA    string
}

// NewRedisStressTest creates a new Redis-compatible stress tester
func NewRedisStressTest(config *StressTestConfig, addr string, name string) (*RedisStressTest, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "",
		DB:       0,
	})

	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", name, err)
	}

	test := &RedisStressTest{
		client:  client,
		config:  config,
		metrics: &TestMetrics{},
		name:    name,
	}

	// Load Lua scripts
	if err := test.loadScripts(ctx); err != nil {
		return nil, err
	}

	return test, nil
}

// loadScripts loads all Lua scripts into Redis
func (r *RedisStressTest) loadScripts(ctx context.Context) error {
	scripts := map[string]*string{
		"../scripts/create_accounts.lua":        &r.createAccountsSHA,
		"../scripts/create_transfers.lua":       &r.createTransfersSHA,
		"../scripts/lookup_accounts.lua":        &r.lookupAccountsSHA,
		"../scripts/get_account_transfers.lua":  &r.getAccountTransfersSHA,
		"../scripts/get_account_balances.lua":   &r.getAccountBalancesSHA,
	}

	for path, shaPtr := range scripts {
		content, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("failed to read %s: %w", path, err)
		}

		sha, err := r.client.ScriptLoad(ctx, string(content)).Result()
		if err != nil {
			return fmt.Errorf("failed to load script %s: %w", path, err)
		}

		*shaPtr = sha
		if r.config.Verbose {
			fmt.Printf("Loaded script %s: %s\n", path, sha)
		}
	}

	return nil
}

// Setup creates initial accounts
func (r *RedisStressTest) Setup(ctx context.Context) error {
	fmt.Printf("Creating %d accounts...\n", r.config.NumAccounts)

	// Create accounts in batches
	batchSize := 1000
	for i := 0; i < r.config.NumAccounts; i += batchSize {
		end := i + batchSize
		if end > r.config.NumAccounts {
			end = r.config.NumAccounts
		}

		accounts := make([]map[string]interface{}, end-i)
		for j := i; j < end; j++ {
			accounts[j-i] = map[string]interface{}{
				"id":     fmt.Sprintf("%d", j+1),
				"ledger": r.config.LedgerID,
				"code":   10,
				"flags":  0,
			}
		}

		accountsJSON, err := json.Marshal(accounts)
		if err != nil {
			return fmt.Errorf("failed to marshal accounts: %w", err)
		}

		result, err := r.client.EvalSha(ctx, r.createAccountsSHA, []string{}, accountsJSON).Result()
		if err != nil {
			return fmt.Errorf("failed to create accounts: %w", err)
		}

		// Check for errors
		var results []map[string]interface{}
		if err := json.Unmarshal([]byte(result.(string)), &results); err != nil {
			return fmt.Errorf("failed to unmarshal results: %w", err)
		}

		for _, res := range results {
			if errCode := res["result"].(float64); errCode != 0 {
				return fmt.Errorf("account creation failed with result code: %v", errCode)
			}
		}

		if r.config.Verbose && (i+batchSize)%5000 == 0 {
			fmt.Printf("Created %d accounts...\n", i+batchSize)
		}
	}

	fmt.Printf("Successfully created %d accounts\n", r.config.NumAccounts)
	return nil
}

// Cleanup clears all test data
func (r *RedisStressTest) Cleanup(ctx context.Context) error {
	return r.client.FlushDB(ctx).Err()
}

// Close closes the Redis connection
func (r *RedisStressTest) Close() error {
	return r.client.Close()
}

// RunWorker runs a single worker thread
func (r *RedisStressTest) RunWorker(ctx context.Context, workerID int, wg *sync.WaitGroup) {
	defer wg.Done()

	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

	// Create account ID generator based on skew
	var idGen AccountIDGenerator
	if r.config.HotAccountSkew < 0.01 {
		idGen = NewUniformGenerator(r.config.NumAccounts, int64(workerID), 0)
	} else {
		idGen = NewZipfGenerator(r.config.NumAccounts, r.config.HotAccountSkew, int64(workerID))
	}

	operationCounter := uint64(0)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Decide operation type based on read ratio
			isRead := rng.Float64() < r.config.ReadRatio

			start := time.Now()
			var err error

			if isRead {
				err = r.performRead(ctx, idGen, rng)
			} else {
				err = r.performWrite(ctx, workerID, &operationCounter, idGen, rng)
			}

			latency := time.Since(start).Nanoseconds()
			r.metrics.TotalLatencyNs.Add(uint64(latency))

			if err != nil {
				r.metrics.OperationsFailed.Add(1)
				if r.config.Verbose {
					fmt.Printf("Worker %d error: %v\n", workerID, err)
				}
			} else {
				r.metrics.OperationsCompleted.Add(1)
			}
		}
	}
}

// performRead performs a read operation (lookup or get transfers)
func (r *RedisStressTest) performRead(ctx context.Context, idGen AccountIDGenerator, rng *rand.Rand) error {
	// Choose between lookup account or get account transfers
	if rng.Float64() < 0.5 {
		// Lookup accounts
		accountIDs := make([]string, r.config.BatchSize)
		for i := 0; i < r.config.BatchSize; i++ {
			accountIDs[i] = fmt.Sprintf("%d", idGen.Next())
		}

		accountIDsJSON, err := json.Marshal(accountIDs)
		if err != nil {
			return err
		}

		_, err = r.client.EvalSha(ctx, r.lookupAccountsSHA, []string{}, accountIDsJSON).Result()
		if err != nil {
			return err
		}

		r.metrics.AccountsLookedup.Add(uint64(r.config.BatchSize))
	} else {
		// Get account transfers
		accountID := fmt.Sprintf("%d", idGen.Next())
		_, err := r.client.EvalSha(ctx, r.getAccountTransfersSHA, []string{}, accountID).Result()
		if err != nil {
			return err
		}

		r.metrics.AccountsLookedup.Add(1)
	}

	return nil
}

// performWrite performs a write operation (create transfers)
func (r *RedisStressTest) performWrite(ctx context.Context, workerID int, counter *uint64, idGen AccountIDGenerator, rng *rand.Rand) error {
	// Create a batch of transfers
	transfers := make([]map[string]interface{}, r.config.BatchSize)

	for i := 0; i < r.config.BatchSize; i++ {
		*counter++
		debitAccountID := idGen.Next()
		creditAccountID := idGen.Next()

		// Ensure different accounts
		for creditAccountID == debitAccountID {
			creditAccountID = idGen.Next()
		}

		transfers[i] = map[string]interface{}{
			"id":                GenerateTransferID(workerID, *counter),
			"debit_account_id":  fmt.Sprintf("%d", debitAccountID),
			"credit_account_id": fmt.Sprintf("%d", creditAccountID),
			"amount":            RandomAmount(rng),
			"ledger":            r.config.LedgerID,
			"code":              10,
			"flags":             0,
		}
	}

	transfersJSON, err := json.Marshal(transfers)
	if err != nil {
		return err
	}

	result, err := r.client.EvalSha(ctx, r.createTransfersSHA, []string{}, transfersJSON).Result()
	if err != nil {
		return err
	}

	// Check for errors
	var results []map[string]interface{}
	if err := json.Unmarshal([]byte(result.(string)), &results); err != nil {
		return err
	}

	successCount := 0
	for _, res := range results {
		if errCode := res["result"].(float64); errCode == 0 {
			successCount++
		}
	}

	r.metrics.TransfersCreated.Add(uint64(successCount))

	return nil
}

// Run executes the stress test
func (r *RedisStressTest) Run(ctx context.Context) error {
	fmt.Printf("\n=== Starting %s Stress Test ===\n", r.name)
	fmt.Printf("Workers: %d\n", r.config.NumWorkers)
	fmt.Printf("Duration: %d seconds\n", r.config.Duration)
	fmt.Printf("Read Ratio: %.2f\n", r.config.ReadRatio)
	fmt.Printf("Hot Account Skew: %.2f\n", r.config.HotAccountSkew)
	fmt.Printf("Batch Size: %d\n", r.config.BatchSize)

	// Setup
	if err := r.Setup(ctx); err != nil {
		return fmt.Errorf("setup failed: %w", err)
	}

	// Start metrics
	r.metrics.StartTime = time.Now()

	// Create context with timeout
	testCtx, cancel := context.WithTimeout(ctx, time.Duration(r.config.Duration)*time.Second)
	defer cancel()

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < r.config.NumWorkers; i++ {
		wg.Add(1)
		go r.RunWorker(testCtx, i, &wg)
	}

	// Wait for completion
	wg.Wait()
	r.metrics.EndTime = time.Now()

	// Print results
	PrintMetrics(r.metrics, r.name+" (Lua Beetle)")

	return nil
}
