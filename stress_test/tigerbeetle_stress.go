package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	tb "github.com/tigerbeetle/tigerbeetle-go"
	"github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

// TigerBeetleStressTest implements stress testing for TigerBeetle
type TigerBeetleStressTest struct {
	client  tb.Client
	config  *StressTestConfig
	metrics *TestMetrics
}

// NewTigerBeetleStressTest creates a new TigerBeetle stress tester
func NewTigerBeetleStressTest(config *StressTestConfig, addresses []string) (*TigerBeetleStressTest, error) {
	// Use cluster ID 0 for testing
	clusterID := types.ToUint128(0)
	client, err := tb.NewClient(clusterID, addresses)
	if err != nil {
		return nil, fmt.Errorf("failed to create TigerBeetle client: %w", err)
	}

	return &TigerBeetleStressTest{
		client:  client,
		config:  config,
		metrics: &TestMetrics{},
	}, nil
}

// Setup creates initial accounts
func (t *TigerBeetleStressTest) Setup(ctx context.Context) error {
	fmt.Printf("Creating %d accounts...\n", t.config.NumAccounts)

	// Create accounts in batches
	// TigerBeetle has a message size limit, accounts are 128 bytes each
	// Max message is ~1MB, so max accounts per batch is ~8000
	batchSize := 100 // Start with small batch for testing
	for i := 0; i < t.config.NumAccounts; i += batchSize {
		end := i + batchSize
		if end > t.config.NumAccounts {
			end = t.config.NumAccounts
		}

		accounts := make([]types.Account, end-i)
		for j := i; j < end; j++ {
			accounts[j-i] = types.Account{
				ID:     types.ToUint128(uint64(j + 1)),
				Ledger: t.config.LedgerID,
				Code:   10,
				Flags:  0,
			}
		}

		results, err := t.client.CreateAccounts(accounts)
		if err != nil {
			return fmt.Errorf("failed to create accounts: %w", err)
		}

		// Check for errors
		if len(results) > 0 {
			return fmt.Errorf("account creation had %d errors", len(results))
		}

		if t.config.Verbose && (i+batchSize)%5000 == 0 {
			fmt.Printf("Created %d accounts...\n", i+batchSize)
		}
	}

	fmt.Printf("Successfully created %d accounts\n", t.config.NumAccounts)
	return nil
}

// Cleanup is not needed for TigerBeetle (immutable ledger)
func (t *TigerBeetleStressTest) Cleanup(ctx context.Context) error {
	fmt.Println("Note: TigerBeetle has immutable ledger - cleanup not performed")
	return nil
}

// Close closes the TigerBeetle client
func (t *TigerBeetleStressTest) Close() error {
	t.client.Close()
	return nil
}

// RunWorker runs a single worker thread
func (t *TigerBeetleStressTest) RunWorker(ctx context.Context, workerID int, wg *sync.WaitGroup) {
	defer wg.Done()

	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

	// Create account ID generator based on skew
	var idGen AccountIDGenerator
	if t.config.HotAccountSkew < 0.01 {
		idGen = NewUniformGenerator(t.config.NumAccounts, int64(workerID), 0)
	} else {
		idGen = NewZipfGenerator(t.config.NumAccounts, t.config.HotAccountSkew, int64(workerID))
	}

	operationCounter := uint64(0)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Decide operation type based on read ratio
			isRead := rng.Float64() < t.config.ReadRatio

			start := time.Now()
			var err error

			if isRead {
				err = t.performRead(ctx, idGen, rng)
			} else {
				err = t.performWrite(ctx, workerID, &operationCounter, idGen, rng)
			}

			latency := time.Since(start).Nanoseconds()
			t.metrics.TotalLatencyNs.Add(uint64(latency))

			if err != nil {
				// For batch operations, individual operation tracking is handled in performRead/performWrite
				if t.config.Verbose {
					fmt.Printf("Worker %d error: %v\n", workerID, err)
				}
			}
			// Note: OperationsCompleted is incremented in performRead/performWrite
			// to accurately count individual operations, not batches
		}
	}
}

// performRead performs a read operation (lookup accounts)
func (t *TigerBeetleStressTest) performRead(ctx context.Context, idGen AccountIDGenerator, rng *rand.Rand) error {
	// Lookup accounts
	accountIDs := make([]types.Uint128, t.config.BatchSize)
	for i := 0; i < t.config.BatchSize; i++ {
		accountIDs[i] = types.ToUint128(idGen.Next())
	}

	_, err := t.client.LookupAccounts(accountIDs)
	if err != nil {
		return err
	}

	t.metrics.AccountsLookedup.Add(uint64(t.config.BatchSize))
	t.metrics.OperationsCompleted.Add(uint64(t.config.BatchSize))
	return nil
}

// performWrite performs a write operation (create transfers)
func (t *TigerBeetleStressTest) performWrite(ctx context.Context, workerID int, counter *uint64, idGen AccountIDGenerator, rng *rand.Rand) error {
	// Create a batch of transfers
	transfers := make([]types.Transfer, t.config.BatchSize)

	for i := 0; i < t.config.BatchSize; i++ {
		*counter++
		debitAccountID := idGen.Next()
		creditAccountID := idGen.Next()

		// Ensure different accounts
		for creditAccountID == debitAccountID {
			creditAccountID = idGen.Next()
		}

		// Generate unique transfer ID
		transferID := uint64(time.Now().UnixNano()) + uint64(workerID)*1e15 + *counter

		transfers[i] = types.Transfer{
			ID:              types.ToUint128(transferID),
			DebitAccountID:  types.ToUint128(debitAccountID),
			CreditAccountID: types.ToUint128(creditAccountID),
			Amount:          types.ToUint128(RandomAmount(rng)),
			Ledger:          t.config.LedgerID,
			Code:            10,
			Flags:           0,
		}
	}

	results, err := t.client.CreateTransfers(transfers)
	if err != nil {
		return err
	}

	// Count successful transfers
	successCount := t.config.BatchSize - len(results)
	t.metrics.TransfersCreated.Add(uint64(successCount))
	t.metrics.OperationsCompleted.Add(uint64(successCount))

	// If there are errors, it doesn't necessarily mean the operation failed
	// (could be validation errors for specific transfers)
	if len(results) > 0 && t.config.Verbose {
		fmt.Printf("Worker %d: %d transfers had errors\n", workerID, len(results))
	}

	return nil
}

// Run executes the stress test
func (t *TigerBeetleStressTest) Run(ctx context.Context) error {
	fmt.Printf("\n=== Starting TigerBeetle Stress Test ===\n")
	fmt.Printf("Workers: %d\n", t.config.NumWorkers)
	fmt.Printf("Duration: %d seconds\n", t.config.Duration)
	fmt.Printf("Read Ratio: %.2f\n", t.config.ReadRatio)
	fmt.Printf("Hot Account Skew: %.2f\n", t.config.HotAccountSkew)
	fmt.Printf("Batch Size: %d\n", t.config.BatchSize)

	// Setup
	if err := t.Setup(ctx); err != nil {
		return fmt.Errorf("setup failed: %w", err)
	}

	// Start metrics
	t.metrics.StartTime = time.Now()

	// Create context with timeout
	testCtx, cancel := context.WithTimeout(ctx, time.Duration(t.config.Duration)*time.Second)
	defer cancel()

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < t.config.NumWorkers; i++ {
		wg.Add(1)
		go t.RunWorker(testCtx, i, &wg)
	}

	// Wait for completion
	wg.Wait()
	t.metrics.EndTime = time.Now()

	// Print results
	PrintMetrics(t.metrics, "TigerBeetle")

	return nil
}
