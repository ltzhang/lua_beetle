package main

import (
	"fmt"
	"math"
	"math/rand"
	"sync/atomic"
	"time"
)

// StressTestConfig defines parameters for the stress test
type StressTestConfig struct {
	NumAccounts    int     // Total number of accounts to create
	NumWorkers     int     // Number of concurrent workers
	Duration       int     // Test duration in seconds
	ReadRatio      float64 // Ratio of read operations (0.0-1.0)
	HotAccountSkew float64 // Zipf skew parameter (0=uniform, 1+=skewed, typically 0.99)
	BatchSize      int     // Number of operations per batch
	LedgerID       uint32  // Ledger ID for all accounts/transfers
	UseBinary      bool    // Use binary encoding instead of JSON
	Verbose        bool    // Enable verbose output
}

// TestMetrics tracks performance metrics
type TestMetrics struct {
	OperationsCompleted atomic.Uint64
	OperationsFailed    atomic.Uint64
	TransfersCreated    atomic.Uint64
	AccountsLookedup    atomic.Uint64
	TotalLatencyNs      atomic.Uint64
	StartTime           time.Time
	EndTime             time.Time
}

// ZipfGenerator generates account IDs with Zipf distribution
type ZipfGenerator struct {
	rng    *rand.Rand
	zipf   *rand.Zipf
	offset uint64
}

// NewZipfGenerator creates a new Zipf distribution generator
func NewZipfGenerator(numAccounts int, skew float64, seed int64) *ZipfGenerator {
	source := rand.NewSource(seed)
	rng := rand.New(source)

	// Zipf requires s > 1, v >= 1
	// We map skew parameter to s: skew=0 -> uniform, skew=0.99 -> Zipf(s=2)
	s := 1.0 + skew*10.0
	if s <= 1.0 {
		s = 1.01
	}

	zipf := rand.NewZipf(rng, s, 1.0, uint64(numAccounts)-1)

	return &ZipfGenerator{
		rng:    rng,
		zipf:   zipf,
		offset: 0,
	}
}

// Next returns the next account ID according to distribution
func (z *ZipfGenerator) Next() uint64 {
	if z.zipf == nil {
		// Uniform distribution
		return uint64(z.rng.Intn(int(math.MaxInt64))) % (uint64(1<<63) - 1)
	}
	// Zipf distribution - returns 0 to numAccounts-1
	// We offset by 1 since account IDs start at 1
	return z.zipf.Uint64() + 1 + z.offset
}

// UniformGenerator generates uniformly distributed account IDs
type UniformGenerator struct {
	rng        *rand.Rand
	numAccounts int
	offset     uint64
}

// NewUniformGenerator creates a uniform distribution generator
func NewUniformGenerator(numAccounts int, seed int64, offset uint64) *UniformGenerator {
	return &UniformGenerator{
		rng:        rand.New(rand.NewSource(seed)),
		numAccounts: numAccounts,
		offset:     offset,
	}
}

// Next returns the next uniformly distributed account ID
func (u *UniformGenerator) Next() uint64 {
	return uint64(u.rng.Intn(u.numAccounts)) + 1 + u.offset
}

// AccountIDGenerator interface for different distribution strategies
type AccountIDGenerator interface {
	Next() uint64
}

// PrintMetrics prints the final metrics
func PrintMetrics(metrics *TestMetrics, testName string) {
	duration := metrics.EndTime.Sub(metrics.StartTime).Seconds()
	completed := metrics.OperationsCompleted.Load()
	failed := metrics.OperationsFailed.Load()
	transfers := metrics.TransfersCreated.Load()
	lookups := metrics.AccountsLookedup.Load()
	totalLatency := metrics.TotalLatencyNs.Load()

	throughput := float64(completed) / duration
	avgLatencyMs := 0.0
	if completed > 0 {
		avgLatencyMs = float64(totalLatency) / float64(completed) / 1e6
	}

	fmt.Printf("\n=== %s Results ===\n", testName)
	fmt.Printf("Duration: %.2f seconds\n", duration)
	fmt.Printf("Operations Completed: %d\n", completed)
	fmt.Printf("Operations Failed: %d\n", failed)
	fmt.Printf("Transfers Created: %d\n", transfers)
	fmt.Printf("Accounts Looked Up: %d\n", lookups)
	fmt.Printf("Throughput: %.2f ops/sec\n", throughput)
	fmt.Printf("Average Latency: %.2f ms\n", avgLatencyMs)
	if completed > 0 {
		successRate := float64(completed-failed) / float64(completed) * 100
		fmt.Printf("Success Rate: %.2f%%\n", successRate)
	}
}

// GenerateTransferID generates a unique transfer ID
func GenerateTransferID(workerID int, counter uint64) string {
	timestamp := time.Now().UnixNano()
	return fmt.Sprintf("%d_%d_%d", timestamp, workerID, counter)
}

// RandomAmount generates a random transfer amount
func RandomAmount(rng *rand.Rand) uint64 {
	// Random amount between 1 and 10000
	return uint64(rng.Intn(10000)) + 1
}
