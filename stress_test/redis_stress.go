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
	encoder                  Encoder
	createAccountSHA         string
	createTransferSHA        string
	lookupAccountSHA         string
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

	// Choose encoder based on config
	var encoder Encoder
	if config.UseBinary {
		encoder = NewBinaryEncoder()
	} else {
		encoder = NewJSONEncoder()
	}

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

// loadScripts loads all Lua scripts into DragonflyDB
func (r *RedisStressTest) loadScripts(ctx context.Context) error {
	suffix := ".lua"
	if r.config.UseBinary {
		suffix = "_binary.lua"
	}

	scripts := map[string]*string{
		"../scripts/create_account" + suffix:         &r.createAccountSHA,
		"../scripts/create_transfer" + suffix:        &r.createTransferSHA,
		"../scripts/lookup_account" + suffix:         &r.lookupAccountSHA,
		"../scripts/get_account_transfers" + suffix:  &r.getAccountTransfersSHA,
		"../scripts/get_account_balances" + suffix:   &r.getAccountBalancesSHA,
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
	fmt.Printf("Creating %d accounts...\n", r.config.NumAccounts)

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

// performRead performs a read operation (lookup or get transfers) using pipelining
func (r *RedisStressTest) performRead(ctx context.Context, idGen AccountIDGenerator, rng *rand.Rand) error {
	// Choose between lookup account or get account transfers
	if rng.Float64() < 0.5 {
		// Lookup accounts using pipeline
		pipe := r.client.Pipeline()
		for i := 0; i < r.config.BatchSize; i++ {
			accountID := idGen.Next()
			var arg interface{}
			if r.config.UseBinary {
				arg = U64ToID16(accountID)
			} else {
				arg = fmt.Sprintf("%d", accountID)
			}
			pipe.EvalSha(ctx, r.lookupAccountSHA, []string{}, arg)
		}

		_, err := pipe.Exec(ctx)
		if err != nil {
			return err
		}

		r.metrics.AccountsLookedup.Add(uint64(r.config.BatchSize))
	} else {
		// Get account transfers
		accountID := idGen.Next()
		var arg interface{}
		if r.config.UseBinary {
			arg = U64ToID16(accountID)
		} else {
			arg = fmt.Sprintf("%d", accountID)
		}
		_, err := r.client.EvalSha(ctx, r.getAccountTransfersSHA, []string{}, arg).Result()
		if err != nil {
			return err
		}

		r.metrics.AccountsLookedup.Add(1)
	}

	return nil
}

// performWrite performs a write operation (create transfers) using pipelining
func (r *RedisStressTest) performWrite(ctx context.Context, workerID int, counter *uint64, idGen AccountIDGenerator, rng *rand.Rand) error {
	// Create transfers using pipeline (each transfer is independent)
	pipe := r.client.Pipeline()

	for i := 0; i < r.config.BatchSize; i++ {
		*counter++
		debitAccountID := idGen.Next()
		creditAccountID := idGen.Next()

		// Client-side check: Ensure different accounts
		// This prevents wasting Redis operations on invalid transfers
		// Server-side validation remains for correctness
		attempts := 0
		maxAttempts := 100 // Prevent infinite loop with extreme skew
		for creditAccountID == debitAccountID && attempts < maxAttempts {
			creditAccountID = idGen.Next()
			attempts++
		}

		// Skip this transfer if we couldn't find different accounts
		// (This only happens with extreme skew where same account dominates)
		if creditAccountID == debitAccountID {
			continue
		}

		transferID := GenerateTransferID(workerID, *counter)
		amount := RandomAmount(rng)

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
