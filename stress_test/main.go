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
	mode := flag.String("mode", "redis", "Test mode: redis, dragonfly, tigerbeetle, or all")
	numAccounts := flag.Int("accounts", 10000, "Number of accounts to create")
	numWorkers := flag.Int("workers", 10, "Number of concurrent workers")
	duration := flag.Int("duration", 60, "Test duration in seconds")
	readRatio := flag.Float64("read-ratio", 0.5, "Ratio of read operations (0.0-1.0)")
	hotAccountSkew := flag.Float64("skew", 0.0, "Hot account skew (0=uniform, 0.99=very skewed)")
	batchSize := flag.Int("batch", 100, "Operations per batch")
	ledgerID := flag.Int("ledger", 700, "Ledger ID")
	useBinary := flag.Bool("binary", false, "Use binary encoding instead of JSON")
	verbose := flag.Bool("verbose", false, "Enable verbose output")
	tbAddress := flag.String("tb-address", "3000", "TigerBeetle server address")
	noCleanup := flag.Bool("no-cleanup", false, "Skip cleanup after test")

	flag.Parse()

	// Validate parameters
	if *readRatio < 0.0 || *readRatio > 1.0 {
		fmt.Fprintf(os.Stderr, "Error: read-ratio must be between 0.0 and 1.0\n")
		os.Exit(1)
	}

	if *hotAccountSkew < 0.0 {
		fmt.Fprintf(os.Stderr, "Error: skew must be >= 0.0\n")
		os.Exit(1)
	}

	if *batchSize < 1 || *batchSize > 8000 {
		fmt.Fprintf(os.Stderr, "Error: batch size must be between 1 and 8000\n")
		os.Exit(1)
	}

	config := &StressTestConfig{
		NumAccounts:    *numAccounts,
		NumWorkers:     *numWorkers,
		Duration:       *duration,
		ReadRatio:      *readRatio,
		HotAccountSkew: *hotAccountSkew,
		BatchSize:      *batchSize,
		LedgerID:       uint32(*ledgerID),
		UseBinary:      *useBinary,
		Verbose:        *verbose,
	}

	ctx := context.Background()

	fmt.Printf("\n=== Stress Test Configuration ===\n")
	fmt.Printf("Mode: %s\n", *mode)
	fmt.Printf("Accounts: %d\n", config.NumAccounts)
	fmt.Printf("Workers: %d\n", config.NumWorkers)
	fmt.Printf("Duration: %d seconds\n", config.Duration)
	fmt.Printf("Read Ratio: %.2f\n", config.ReadRatio)
	fmt.Printf("Hot Account Skew: %.2f (", config.HotAccountSkew)
	if config.HotAccountSkew < 0.01 {
		fmt.Printf("uniform distribution)\n")
	} else if config.HotAccountSkew < 0.5 {
		fmt.Printf("mild skew)\n")
	} else if config.HotAccountSkew < 1.0 {
		fmt.Printf("moderate skew)\n")
	} else {
		fmt.Printf("heavy skew)\n")
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

	case "tigerbeetle":
		if err := runTigerBeetleTest(ctx, config, *tbAddress, *noCleanup); err != nil {
			fmt.Fprintf(os.Stderr, "TigerBeetle test failed: %v\n", err)
			os.Exit(1)
		}

	case "both":
		fmt.Println("Running both Redis and TigerBeetle tests for comparison...\n")

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
		fmt.Println("Running Redis, DragonflyDB, and TigerBeetle tests for comparison...\n")

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
		fmt.Fprintf(os.Stderr, "Error: invalid mode '%s'. Must be 'redis', 'dragonfly', 'tigerbeetle', 'both', or 'all'\n", *mode)
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
