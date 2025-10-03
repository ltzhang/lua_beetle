package main

import (
	"context"
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
	encoder                  *BinaryEncoder
	createAccountSHA         string
	createTransferSHA        string
	lookupAccountSHA         string
	getAccountTransfersSHA   string
	getAccountBalancesSHA    string
	pendingTransfers         sync.Map // Track pending transfers for two-phase commits
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

	encoder := NewBinaryEncoder()

	test := &RedisStressTest{
		client:  client,
		config:  config,
		metrics: &TestMetrics{},
		name:    name,
		encoder: encoder,
	}

	// Load Lua scripts
	if err := test.loadScripts(ctx); err != nil {
		return nil, err
	}

	return test, nil
}

// loadScripts loads all Lua scripts into Redis/DragonflyDB
func (r *RedisStressTest) loadScripts(ctx context.Context) error {
	scripts := map[string]*string{
		"../scripts/create_account.lua":        &r.createAccountSHA,
		"../scripts/create_transfer.lua":       &r.createTransferSHA,
		"../scripts/lookup_account.lua":        &r.lookupAccountSHA,
		"../scripts/get_account_transfers.lua": &r.getAccountTransfersSHA,
		"../scripts/get_account_balances.lua":  &r.getAccountBalancesSHA,
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

// Setup creates initial accounts using pipelining
func (r *RedisStressTest) Setup(ctx context.Context) error {
	fmt.Printf("Creating %d accounts (%d hot, %d cold)...\n",
		r.config.NumAccounts, r.config.NumHotAccounts, r.config.NumAccounts-r.config.NumHotAccounts)

	// Create accounts using pipelines (batching for performance, not transactions)
	pipelineSize := 1000
	for i := 0; i < r.config.NumAccounts; i += pipelineSize {
		end := i + pipelineSize
		if end > r.config.NumAccounts {
			end = r.config.NumAccounts
		}

		pipe := r.client.Pipeline()
		for j := i; j < end; j++ {
			accountData, err := r.encoder.EncodeAccount(uint64(j+1), r.config.LedgerID, 10, 0)
			if err != nil {
				return fmt.Errorf("failed to encode account: %w", err)
			}

			pipe.EvalSha(ctx, r.createAccountSHA, []string{}, accountData)
		}

		// Execute pipeline
		results, err := pipe.Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to execute pipeline: %w", err)
		}

		// Check for errors
		for idx, result := range results {
			if result.Err() != nil {
				return fmt.Errorf("account %d creation failed: %w", i+idx+1, result.Err())
			}

			errCode, err := r.encoder.DecodeTransferResult(result.(*redis.Cmd).Val())
			if err != nil {
				return fmt.Errorf("failed to decode result: %w", err)
			}

			if errCode != 0 {
				return fmt.Errorf("account %d creation failed with result code: %v", i+idx+1, errCode)
			}
		}

		if r.config.Verbose && end%5000 == 0 {
			fmt.Printf("Created %d accounts...\n", end)
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

	// Create hot/cold account ID generator
	accountGen := NewHotColdGenerator(r.config.NumAccounts, r.config.NumHotAccounts, int64(workerID), 0)

	operationCounter := uint64(0)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			start := time.Now()
			var err error

			// Execute workload based on type
			switch r.config.Workload {
			case WorkloadTransfer:
				err = r.performTransferBatch(ctx, workerID, &operationCounter, accountGen)
			case WorkloadLookup:
				err = r.performLookupBatch(ctx, accountGen)
			case WorkloadTwoPhase:
				err = r.performTwoPhaseBatch(ctx, workerID, &operationCounter, accountGen, rng)
			case WorkloadMixed:
				// Mixed workload: decide operation type based on ratio
				if rng.Float64() < r.config.TransferRatio {
					// Transfer (possibly two-phase)
					if rng.Float64() < r.config.TwoPhaseRatio {
						err = r.performTwoPhaseBatch(ctx, workerID, &operationCounter, accountGen, rng)
					} else {
						err = r.performTransferBatch(ctx, workerID, &operationCounter, accountGen)
					}
				} else {
					// Lookup
					err = r.performLookupBatch(ctx, accountGen)
				}
			}

			latency := time.Since(start).Nanoseconds()
			r.metrics.TotalLatencyNs.Add(uint64(latency))

			if err != nil && r.config.Verbose {
				fmt.Printf("Worker %d error: %v\n", workerID, err)
			}
		}
	}
}

// performTransferBatch performs a batch of regular transfers
func (r *RedisStressTest) performTransferBatch(ctx context.Context, workerID int, counter *uint64, accountGen *HotColdGenerator) error {
	pipe := r.client.Pipeline()

	for i := 0; i < r.config.BatchSize; i++ {
		*counter++

		// Transfer between 1 hot account and 1 random account
		debitAccountID, creditAccountID := accountGen.NextHotAndAny()

		transferID := GenerateTransferID(workerID, *counter)
		amount := uint64(100) // Fixed amount for consistency

		transferData, err := r.encoder.EncodeTransfer(transferID, debitAccountID, creditAccountID, amount, r.config.LedgerID, 10, 0)
		if err != nil {
			return err
		}

		pipe.EvalSha(ctx, r.createTransferSHA, []string{}, transferData)
	}

	// Execute pipeline
	results, err := pipe.Exec(ctx)
	if err != nil {
		return err
	}

	// Check for errors and count successes
	successCount := 0
	for _, result := range results {
		if result.Err() != nil {
			continue
		}

		errCode, err := r.encoder.DecodeTransferResult(result.(*redis.Cmd).Val())
		if err != nil {
			continue
		}

		if errCode == 0 {
			successCount++
		}
	}

	r.metrics.TransfersCreated.Add(uint64(successCount))
	r.metrics.OperationsCompleted.Add(uint64(successCount))

	return nil
}

// performLookupBatch performs a batch of account lookups
func (r *RedisStressTest) performLookupBatch(ctx context.Context, accountGen *HotColdGenerator) error {
	pipe := r.client.Pipeline()

	// Half lookups on hot accounts, half on random accounts
	halfBatch := r.config.BatchSize / 2

	// Hot account lookups
	for i := 0; i < halfBatch; i++ {
		accountID := accountGen.NextHot()
		arg := U64ToID16(accountID)
		pipe.EvalSha(ctx, r.lookupAccountSHA, []string{}, arg)
	}

	// Random account lookups
	for i := 0; i < r.config.BatchSize-halfBatch; i++ {
		accountID := accountGen.NextAny()
		arg := U64ToID16(accountID)
		pipe.EvalSha(ctx, r.lookupAccountSHA, []string{}, arg)
	}

	results, err := pipe.Exec(ctx)
	if err != nil {
		return err
	}

	// Count successful lookups
	successCount := 0
	for _, result := range results {
		if result.Err() == nil {
			successCount++
		}
	}

	r.metrics.AccountsLookedup.Add(uint64(successCount))
	r.metrics.OperationsCompleted.Add(uint64(successCount))

	return nil
}

// performTwoPhaseBatch performs a batch of two-phase transfers
func (r *RedisStressTest) performTwoPhaseBatch(ctx context.Context, workerID int, counter *uint64, accountGen *HotColdGenerator, rng *rand.Rand) error {
	pipe := r.client.Pipeline()

	// Track pending transfers created in this batch
	type pendingTransfer struct {
		id              string
		debitAccountID  uint64
		creditAccountID uint64
		amount          uint64
	}
	pendingBatch := make([]pendingTransfer, 0, r.config.BatchSize)

	for i := 0; i < r.config.BatchSize; i++ {
		*counter++

		// 50% chance to create new pending, 25% to post existing, 25% to void existing
		action := rng.Float64()

		if action < 0.5 {
			// Create pending transfer
			debitAccountID, creditAccountID := accountGen.NextHotAndAny()
			transferID := GenerateTransferID(workerID, *counter)
			amount := uint64(100)

			transferData, err := r.encoder.EncodeTransfer(transferID, debitAccountID, creditAccountID, amount, r.config.LedgerID, 10, 0x0002) // PENDING flag
			if err != nil {
				return err
			}

			pipe.EvalSha(ctx, r.createTransferSHA, []string{}, transferData)

			pendingBatch = append(pendingBatch, pendingTransfer{
				id:              transferID,
				debitAccountID:  debitAccountID,
				creditAccountID: creditAccountID,
				amount:          amount,
			})

		} else {
			// Try to post or void an existing pending transfer
			// For simplicity, we'll create a pending and immediately post/void it
			debitAccountID, creditAccountID := accountGen.NextHotAndAny()
			pendingID := GenerateTransferID(workerID, *counter)
			amount := uint64(100)

			// Create pending
			pendingData, _ := r.encoder.EncodeTransfer(pendingID, debitAccountID, creditAccountID, amount, r.config.LedgerID, 10, 0x0002)
			pipe.EvalSha(ctx, r.createTransferSHA, []string{}, pendingData)

			// Post or void
			*counter++
			postVoidID := GenerateTransferID(workerID, *counter)
			var flags uint16
			if action < 0.75 {
				flags = 0x0004 // POST_PENDING
			} else {
				flags = 0x0008 // VOID_PENDING
			}

			postVoidData, _ := r.encoder.EncodeTransferWithPending(postVoidID, debitAccountID, creditAccountID, amount, pendingID, r.config.LedgerID, 10, flags)
			pipe.EvalSha(ctx, r.createTransferSHA, []string{}, postVoidData)
		}
	}

	// Execute pipeline
	results, err := pipe.Exec(ctx)
	if err != nil {
		return err
	}

	// Count successes
	successCount := 0
	pendingCount := 0
	postedCount := 0
	voidedCount := 0

	for _, result := range results {
		if result.Err() != nil {
			continue
		}

		errCode, err := r.encoder.DecodeTransferResult(result.(*redis.Cmd).Val())
		if err != nil {
			continue
		}

		if errCode == 0 {
			successCount++
			// Note: This is simplified - in reality we'd track which operation succeeded
			pendingCount++
		}
	}

	// Store pending transfers for future post/void (simplified - not used in current implementation)
	for _, pending := range pendingBatch {
		r.pendingTransfers.Store(pending.id, pending)
	}

	r.metrics.TwoPhaseCreated.Add(uint64(successCount))
	r.metrics.TwoPhasePending.Add(uint64(pendingCount))
	r.metrics.TwoPhasePosted.Add(uint64(postedCount))
	r.metrics.TwoPhaseVoided.Add(uint64(voidedCount))
	r.metrics.OperationsCompleted.Add(uint64(successCount))

	return nil
}

// PrintLuaStats prints Lua profiling statistics from Redis
func (r *RedisStressTest) PrintLuaStats(ctx context.Context, duration float64) {
	result, err := r.client.Do(ctx, "SCRIPT", "PROFILE").Result()
	if err != nil {
		fmt.Printf("\nWarning: failed to get Lua statistics: %v\n", err)
		return
	}

	// Redis returns map[interface{}]interface{}
	rawMap, ok := result.(map[interface{}]interface{})
	if !ok {
		fmt.Printf("\nWarning: unexpected Lua stats format: %T\n", result)
		return
	}

	// Convert to map[string]interface{}
	statsMap := make(map[string]interface{})
	for k, v := range rawMap {
		if keyStr, ok := k.(string); ok {
			statsMap[keyStr] = v
		}
	}

	// Extract values
	totalScripts := getInt64(statsMap["total_scripts"])
	totalTimeUs := getInt64(statsMap["total_time_us"])
	redisCallTimeUs := getInt64(statsMap["redis_call_time_us"])
	redisCallCount := getInt64(statsMap["redis_call_count"])
	luaInterpTimeUs := getInt64(statsMap["lua_interp_time_us"])
	luaInterpPercent := getFloat64(statsMap["lua_interp_percent"])

	fmt.Printf("\n=== Lua Profiling Statistics ===\n")
	fmt.Printf("Total Scripts Executed: %d (%.2f scripts/sec)\n", totalScripts, float64(totalScripts)/duration)
	fmt.Printf("Total Lua Time: %d us (%.2f us/sec, %.2f us/script)\n",
		totalTimeUs, float64(totalTimeUs)/duration, float64(totalTimeUs)/float64(totalScripts))
	fmt.Printf("Redis Call Time: %d us (%.2f us/sec, %.2f us/call)\n",
		redisCallTimeUs, float64(redisCallTimeUs)/duration, float64(redisCallTimeUs)/float64(redisCallCount))
	fmt.Printf("Redis Call Count: %d (%.2f calls/sec)\n", redisCallCount, float64(redisCallCount)/duration)
	fmt.Printf("Lua Interpretation Time: %d us (%.2f us/sec, %.2f us/script)\n",
		luaInterpTimeUs, float64(luaInterpTimeUs)/duration, float64(luaInterpTimeUs)/float64(totalScripts))
	fmt.Printf("Lua Interpretation Percent: %.2f%%\n", luaInterpPercent)
}

// Helper functions to extract numeric values from interface{}
func getInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int64:
		return val
	case int:
		return int64(val)
	case float64:
		return int64(val)
	default:
		return 0
	}
}

func getFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case int64:
		return float64(val)
	case int:
		return float64(val)
	default:
		return 0.0
	}
}

// Run executes the stress test
func (r *RedisStressTest) Run(ctx context.Context) error {
	fmt.Printf("\n=== Starting %s Stress Test ===\n", r.name)
	fmt.Printf("Workload: %s\n", r.config.Workload)
	fmt.Printf("Workers: %d\n", r.config.NumWorkers)
	fmt.Printf("Duration: %d seconds\n", r.config.Duration)
	fmt.Printf("Batch Size: %d\n", r.config.BatchSize)
	if r.config.Workload == WorkloadMixed {
		fmt.Printf("Transfer Ratio: %.2f\n", r.config.TransferRatio)
		fmt.Printf("Two-Phase Ratio: %.2f\n", r.config.TwoPhaseRatio)
	}

	// Setup
	if err := r.Setup(ctx); err != nil {
		return fmt.Errorf("setup failed: %w", err)
	}

	// Reset Lua profiling statistics
	if err := r.client.Do(ctx, "SCRIPT", "PROFILERESET").Err(); err != nil {
		fmt.Printf("Warning: failed to reset Lua statistics: %v\n", err)
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

	// Progress reporter
	progressTicker := time.NewTicker(5 * time.Second)
	defer progressTicker.Stop()

	go func() {
		for {
			select {
			case <-progressTicker.C:
				elapsed := time.Since(r.metrics.StartTime).Seconds()
				completed := r.metrics.OperationsCompleted.Load()
				fmt.Printf("[Progress] %.0fs elapsed, %d ops completed (%.0f ops/sec)\n",
					elapsed, completed, float64(completed)/elapsed)
			case <-testCtx.Done():
				return
			}
		}
	}()

	// Wait for completion
	wg.Wait()
	r.metrics.EndTime = time.Now()

	duration := r.metrics.EndTime.Sub(r.metrics.StartTime).Seconds()

	// Print results
	PrintMetrics(r.metrics, r.name+" (Lua Beetle)")

	// Print Lua profiling statistics
	r.PrintLuaStats(ctx, duration)

	return nil
}
