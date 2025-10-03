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
	client           tb.Client
	config           *StressTestConfig
	metrics          *TestMetrics
	pendingTransfers sync.Map // Track pending transfers for two-phase commits
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
	fmt.Printf("Creating %d accounts (%d hot, %d cold)...\n",
		t.config.NumAccounts, t.config.NumHotAccounts, t.config.NumAccounts-t.config.NumHotAccounts)

	// Create accounts in batches
	// TigerBeetle has a message size limit, accounts are 128 bytes each
	// Max message is ~1MB, so max accounts per batch is ~8000
	batchSize := 100
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

		if t.config.Verbose && end%5000 == 0 {
			fmt.Printf("Created %d accounts...\n", end)
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

	// Create hot/cold account ID generator
	accountGen := NewHotColdGenerator(t.config.NumAccounts, t.config.NumHotAccounts, int64(workerID), 0)

	operationCounter := uint64(0)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			start := time.Now()
			var err error

			// Execute workload based on type
			switch t.config.Workload {
			case WorkloadTransfer:
				err = t.performTransferBatch(ctx, workerID, &operationCounter, accountGen)
			case WorkloadLookup:
				err = t.performLookupBatch(ctx, accountGen)
			case WorkloadTwoPhase:
				err = t.performTwoPhaseBatch(ctx, workerID, &operationCounter, accountGen, rng)
			case WorkloadMixed:
				// Mixed workload: decide operation type based on ratio
				if rng.Float64() < t.config.TransferRatio {
					// Transfer (possibly two-phase)
					if rng.Float64() < t.config.TwoPhaseRatio {
						err = t.performTwoPhaseBatch(ctx, workerID, &operationCounter, accountGen, rng)
					} else {
						err = t.performTransferBatch(ctx, workerID, &operationCounter, accountGen)
					}
				} else {
					// Lookup
					err = t.performLookupBatch(ctx, accountGen)
				}
			}

			latency := time.Since(start).Nanoseconds()
			t.metrics.TotalLatencyNs.Add(uint64(latency))

			if err != nil && t.config.Verbose {
				fmt.Printf("Worker %d error: %v\n", workerID, err)
			}
		}
	}
}

// performTransferBatch performs a batch of regular transfers
func (t *TigerBeetleStressTest) performTransferBatch(ctx context.Context, workerID int, counter *uint64, accountGen *HotColdGenerator) error {
	transfers := make([]types.Transfer, t.config.BatchSize)

	for i := 0; i < t.config.BatchSize; i++ {
		*counter++

		// Transfer between 1 hot account and 1 random account
		debitAccountID, creditAccountID := accountGen.NextHotAndAny()

		transfers[i] = types.Transfer{
			ID:              types.ToUint128(hashStringToU64(GenerateTransferID(workerID, *counter))),
			DebitAccountID:  types.ToUint128(debitAccountID),
			CreditAccountID: types.ToUint128(creditAccountID),
			Amount:          types.ToUint128(100),
			Ledger:          t.config.LedgerID,
			Code:            10,
			Flags:           0,
		}
	}

	results, err := t.client.CreateTransfers(transfers)
	if err != nil {
		return err
	}

	// Count successes (failures are in results array)
	successCount := t.config.BatchSize - len(results)

	t.metrics.TransfersCreated.Add(uint64(successCount))
	t.metrics.OperationsCompleted.Add(uint64(successCount))

	return nil
}

// performLookupBatch performs a batch of account lookups
func (t *TigerBeetleStressTest) performLookupBatch(ctx context.Context, accountGen *HotColdGenerator) error {
	// TigerBeetle supports batch lookups
	accountIDs := make([]types.Uint128, t.config.BatchSize)

	// Half lookups on hot accounts, half on random accounts
	halfBatch := t.config.BatchSize / 2

	// Hot account lookups
	for i := 0; i < halfBatch; i++ {
		accountID := accountGen.NextHot()
		accountIDs[i] = types.ToUint128(accountID)
	}

	// Random account lookups
	for i := halfBatch; i < t.config.BatchSize; i++ {
		accountID := accountGen.NextAny()
		accountIDs[i] = types.ToUint128(accountID)
	}

	accounts, err := t.client.LookupAccounts(accountIDs)
	if err != nil {
		return err
	}

	// Count successful lookups
	successCount := len(accounts)

	t.metrics.AccountsLookedup.Add(uint64(successCount))
	t.metrics.OperationsCompleted.Add(uint64(successCount))

	return nil
}

// performTwoPhaseBatch performs a batch of two-phase transfers
func (t *TigerBeetleStressTest) performTwoPhaseBatch(ctx context.Context, workerID int, counter *uint64, accountGen *HotColdGenerator, rng *rand.Rand) error {
	transfers := make([]types.Transfer, 0, t.config.BatchSize)

	// Track pending transfers created in this batch
	type pendingTransfer struct {
		id              types.Uint128
		debitAccountID  uint64
		creditAccountID uint64
		amount          uint64
	}
	pendingBatch := make([]pendingTransfer, 0)

	for i := 0; i < t.config.BatchSize; i++ {
		*counter++

		// 50% chance to create new pending, 25% to post existing, 25% to void existing
		action := rng.Float64()

		if action < 0.5 {
			// Create pending transfer
			debitAccountID, creditAccountID := accountGen.NextHotAndAny()
			transferID := types.ToUint128(hashStringToU64(GenerateTransferID(workerID, *counter)))
			amount := uint64(100)

			transfers = append(transfers, types.Transfer{
				ID:              transferID,
				DebitAccountID:  types.ToUint128(debitAccountID),
				CreditAccountID: types.ToUint128(creditAccountID),
				Amount:          types.ToUint128(amount),
				Ledger:          t.config.LedgerID,
				Code:            10,
				Flags:           types.TransferFlags{Pending: true}.ToUint16(),
			})

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
			pendingID := types.ToUint128(hashStringToU64(GenerateTransferID(workerID, *counter)))
			amount := uint64(100)

			// Create pending
			transfers = append(transfers, types.Transfer{
				ID:              pendingID,
				DebitAccountID:  types.ToUint128(debitAccountID),
				CreditAccountID: types.ToUint128(creditAccountID),
				Amount:          types.ToUint128(amount),
				Ledger:          t.config.LedgerID,
				Code:            10,
				Flags:           types.TransferFlags{Pending: true}.ToUint16(),
			})

			// Post or void
			*counter++
			postVoidID := types.ToUint128(hashStringToU64(GenerateTransferID(workerID, *counter)))
			var flags types.TransferFlags
			if action < 0.75 {
				flags = types.TransferFlags{PostPendingTransfer: true}
			} else {
				flags = types.TransferFlags{VoidPendingTransfer: true}
			}

			transfers = append(transfers, types.Transfer{
				ID:              postVoidID,
				DebitAccountID:  types.ToUint128(debitAccountID),
				CreditAccountID: types.ToUint128(creditAccountID),
				Amount:          types.ToUint128(amount),
				PendingID:       pendingID,
				Ledger:          t.config.LedgerID,
				Code:            10,
				Flags:           flags.ToUint16(),
			})
		}
	}

	results, err := t.client.CreateTransfers(transfers)
	if err != nil {
		return err
	}

	// Count successes
	successCount := len(transfers) - len(results)

	// Store pending transfers for future post/void (simplified)
	for _, pending := range pendingBatch {
		t.pendingTransfers.Store(pending.id, pending)
	}

	// Note: Simplified metrics - not distinguishing between pending/posted/voided
	t.metrics.TwoPhaseCreated.Add(uint64(successCount))
	t.metrics.TwoPhasePending.Add(uint64(len(pendingBatch)))
	t.metrics.OperationsCompleted.Add(uint64(successCount))

	return nil
}

// Run executes the stress test
func (t *TigerBeetleStressTest) Run(ctx context.Context) error {
	fmt.Printf("\n=== Starting TigerBeetle Stress Test ===\n")
	fmt.Printf("Workload: %s\n", t.config.Workload)
	fmt.Printf("Workers: %d\n", t.config.NumWorkers)
	fmt.Printf("Duration: %d seconds\n", t.config.Duration)
	fmt.Printf("Batch Size: %d\n", t.config.BatchSize)
	if t.config.Workload == WorkloadMixed {
		fmt.Printf("Transfer Ratio: %.2f\n", t.config.TransferRatio)
		fmt.Printf("Two-Phase Ratio: %.2f\n", t.config.TwoPhaseRatio)
	}

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

	// Progress reporter
	progressTicker := time.NewTicker(5 * time.Second)
	defer progressTicker.Stop()

	go func() {
		for {
			select {
			case <-progressTicker.C:
				elapsed := time.Since(t.metrics.StartTime).Seconds()
				completed := t.metrics.OperationsCompleted.Load()
				fmt.Printf("[Progress] %.0fs elapsed, %d ops completed (%.0f ops/sec)\n",
					elapsed, completed, float64(completed)/elapsed)
			case <-testCtx.Done():
				return
			}
		}
	}()

	// Wait for completion
	wg.Wait()
	t.metrics.EndTime = time.Now()

	// Print results
	PrintMetrics(t.metrics, "TigerBeetle")

	return nil
}
