package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
)

func main() {
	// Command line flags
	mode := flag.String("mode", "redis", "Test mode: redis, dragonfly, eloqkv, tigerbeetle, or all")
	numAccounts := flag.Int("accounts", 10000, "Number of accounts to create")
	numHotAccounts := flag.Int("hot-accounts", 100, "Number of hot accounts")
	numWorkers := flag.Int("workers", 10, "Number of concurrent workers")
	duration := flag.Int("duration", 60, "Test duration in seconds")
	workload := flag.String("workload", "transfer", "Workload type: transfer, lookup, twophase, or mixed")
	transferRatio := flag.Float64("transfer-ratio", 0.7, "For mixed workload: ratio of transfers (0.0-1.0)")
	twoPhaseRatio := flag.Float64("twophase-ratio", 0.1, "For mixed workload: ratio of two-phase transfers within transfers (0.0-1.0)")
	batchSize := flag.Int("batch", 100, "Operations per batch")
	ledgerID := flag.Int("ledger", 700, "Ledger ID")
	verbose := flag.Bool("verbose", false, "Enable verbose output")
	tbAddress := flag.String("tb-address", "3000", "TigerBeetle server address")
	noCleanup := flag.Bool("no-cleanup", false, "Skip cleanup after test")

	flag.Parse()

	// Validate parameters
	if *numHotAccounts <= 0 || *numHotAccounts > *numAccounts {
		fmt.Fprintf(os.Stderr, "Error: hot-accounts must be between 1 and %d\n", *numAccounts)
		os.Exit(1)
	}

	if *transferRatio < 0.0 || *transferRatio > 1.0 {
		fmt.Fprintf(os.Stderr, "Error: transfer-ratio must be between 0.0 and 1.0\n")
		os.Exit(1)
	}

	if *twoPhaseRatio < 0.0 || *twoPhaseRatio > 1.0 {
		fmt.Fprintf(os.Stderr, "Error: twophase-ratio must be between 0.0 and 1.0\n")
		os.Exit(1)
	}

	if *batchSize < 1 || *batchSize > 8000 {
		fmt.Fprintf(os.Stderr, "Error: batch size must be between 1 and 8000\n")
		os.Exit(1)
	}

	// Parse workload type
	var workloadType WorkloadType
	switch strings.ToLower(*workload) {
	case "transfer":
		workloadType = WorkloadTransfer
	case "lookup":
		workloadType = WorkloadLookup
	case "twophase":
		workloadType = WorkloadTwoPhase
	case "mixed":
		workloadType = WorkloadMixed
	default:
		fmt.Fprintf(os.Stderr, "Error: invalid workload '%s'. Must be 'transfer', 'lookup', 'twophase', or 'mixed'\n", *workload)
		os.Exit(1)
	}

	config := &StressTestConfig{
		NumAccounts:    *numAccounts,
		NumHotAccounts: *numHotAccounts,
		NumWorkers:     *numWorkers,
		Duration:       *duration,
		Workload:       workloadType,
		TransferRatio:  *transferRatio,
		TwoPhaseRatio:  *twoPhaseRatio,
		BatchSize:      *batchSize,
		LedgerID:       uint32(*ledgerID),
		Verbose:        *verbose,
	}

	ctx := context.Background()

	fmt.Printf("\n=== Stress Test Configuration ===\n")
	fmt.Printf("Mode: %s\n", *mode)
	fmt.Printf("Accounts: %d total (%d hot, %d cold)\n",
		config.NumAccounts, config.NumHotAccounts, config.NumAccounts-config.NumHotAccounts)
	fmt.Printf("Workers: %d\n", config.NumWorkers)
	fmt.Printf("Duration: %d seconds\n", config.Duration)
	fmt.Printf("Workload: %s\n", config.Workload)
	if config.Workload == WorkloadMixed {
		fmt.Printf("  Transfer Ratio: %.2f\n", config.TransferRatio)
		fmt.Printf("  Two-Phase Ratio: %.2f\n", config.TwoPhaseRatio)
	}
	fmt.Printf("Batch Size: %d\n", config.BatchSize)
	fmt.Printf("Ledger ID: %d\n", config.LedgerID)
	fmt.Printf("\n")

	// Run tests based on mode
	switch strings.ToLower(*mode) {
	case "redis":
		if err := runRedisTest(ctx, config, "localhost:6379", "Redis", *noCleanup); err != nil {
			fmt.Fprintf(os.Stderr, "Redis test failed: %v\n", err)
			os.Exit(1)
		}

	case "dragonfly":
		if err := runRedisTest(ctx, config, "localhost:6380", "DragonflyDB", *noCleanup); err != nil {
			fmt.Fprintf(os.Stderr, "DragonflyDB test failed: %v\n", err)
			os.Exit(1)
		}

	case "eloqkv":
		if err := runRedisTest(ctx, config, "localhost:6379", "EloqKV", *noCleanup); err != nil {
			fmt.Fprintf(os.Stderr, "EloqKV test failed: %v\n", err)
			os.Exit(1)
		}

	case "tigerbeetle":
		if err := runTigerBeetleTest(ctx, config, *tbAddress, *noCleanup); err != nil {
			fmt.Fprintf(os.Stderr, "TigerBeetle test failed: %v\n", err)
			os.Exit(1)
		}

	case "both":
		fmt.Println("Running both Redis and TigerBeetle tests for comparison...")

		// Run Redis first
		if err := runRedisTest(ctx, config, "localhost:6379", "Redis", *noCleanup); err != nil {
			fmt.Fprintf(os.Stderr, "Redis test failed: %v\n", err)
			os.Exit(1)
		}

		fmt.Println("\n" + strings.Repeat("=", 60) + "\n")

		// Run TigerBeetle
		if err := runTigerBeetleTest(ctx, config, *tbAddress, *noCleanup); err != nil {
			fmt.Fprintf(os.Stderr, "TigerBeetle test failed: %v\n", err)
			os.Exit(1)
		}

	case "all":
		fmt.Println("Running Redis, DragonflyDB, and TigerBeetle tests for comparison...")

		// Run Redis
		if err := runRedisTest(ctx, config, "localhost:6379", "Redis", *noCleanup); err != nil {
			fmt.Fprintf(os.Stderr, "Redis test failed: %v\n", err)
			os.Exit(1)
		}

		fmt.Println("\n" + strings.Repeat("=", 60) + "\n")

		// Run DragonflyDB
		if err := runRedisTest(ctx, config, "localhost:6380", "DragonflyDB", *noCleanup); err != nil {
			fmt.Fprintf(os.Stderr, "DragonflyDB test failed: %v\n", err)
			os.Exit(1)
		}

		fmt.Println("\n" + strings.Repeat("=", 60) + "\n")

		// Run TigerBeetle
		if err := runTigerBeetleTest(ctx, config, *tbAddress, *noCleanup); err != nil {
			fmt.Fprintf(os.Stderr, "TigerBeetle test failed: %v\n", err)
			os.Exit(1)
		}

	default:
		fmt.Fprintf(os.Stderr, "Error: invalid mode '%s'. Must be 'redis', 'dragonfly', 'eloqkv', 'tigerbeetle', 'both', or 'all'\n", *mode)
		os.Exit(1)
	}

	fmt.Println("\n✅ Stress test completed successfully!")
}

func runRedisTest(ctx context.Context, config *StressTestConfig, addr string, name string, noCleanup bool) error {
	test, err := NewRedisStressTest(config, addr, name)
	if err != nil {
		return err
	}
	defer test.Close()

	if err := test.Run(ctx); err != nil {
		return err
	}

	if !noCleanup {
		fmt.Printf("\nCleaning up %s data...\n", name)
		if err := test.Cleanup(ctx); err != nil {
			return fmt.Errorf("cleanup failed: %w", err)
		}
	}

	return nil
}

func runTigerBeetleTest(ctx context.Context, config *StressTestConfig, address string, noCleanup bool) error {
	test, err := NewTigerBeetleStressTest(config, []string{address})
	if err != nil {
		return err
	}
	defer test.Close()

	if err := test.Run(ctx); err != nil {
		return err
	}

	if !noCleanup {
		if err := test.Cleanup(ctx); err != nil {
			return fmt.Errorf("cleanup failed: %w", err)
		}
	}

	return nil
}
