package main

import (
	"encoding/binary"
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

// ============================================================================
// Encoding interfaces and implementations
// ============================================================================

// BinaryEncoder implements fixed-size binary encoding matching TigerBeetle format
type BinaryEncoder struct{}

func NewBinaryEncoder() *BinaryEncoder {
	return &BinaryEncoder{}
}

// Encode uint64 as 16-byte little-endian (128-bit)
func encodeU128(val uint64) []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], val)
	// Upper 8 bytes remain zero for values that fit in uint64
	return buf
}

// Encode uint32 as 4-byte little-endian
func encodeU32(val uint32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, val)
	return buf
}

// Encode uint16 as 2-byte little-endian
func encodeU16(val uint16) []byte {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, val)
	return buf
}

func (e *BinaryEncoder) EncodeAccount(id uint64, ledger uint32, code uint16, flags uint16) (interface{}, error) {
	// Account binary layout: 128 bytes
	buf := make([]byte, 128)

	// id: 16 bytes (offset 0)
	copy(buf[0:16], encodeU128(id))

	// debits_pending: 16 bytes (offset 16) - zero
	// debits_posted: 16 bytes (offset 32) - zero
	// credits_pending: 16 bytes (offset 48) - zero
	// credits_posted: 16 bytes (offset 64) - zero
	// user_data_128: 16 bytes (offset 80) - zero
	// user_data_64: 8 bytes (offset 96) - zero
	// user_data_32: 4 bytes (offset 104) - zero
	// reserved: 4 bytes (offset 108) - zero

	// ledger: 4 bytes (offset 112)
	copy(buf[112:116], encodeU32(ledger))

	// code: 2 bytes (offset 116)
	copy(buf[116:118], encodeU16(code))

	// flags: 2 bytes (offset 118)
	copy(buf[118:120], encodeU16(flags))

	// timestamp: 8 bytes (offset 120) - will be set by server

	return buf, nil
}

func (e *BinaryEncoder) EncodeTransfer(id string, debitAccountID, creditAccountID uint64, amount uint64, ledger uint32, code uint16, flags uint16) (interface{}, error) {
	// Transfer binary layout: 128 bytes
	buf := make([]byte, 128)

	// Parse ID string to uint64 (simplified - in production would handle the full ID format)
	// For now, use a hash or simplified conversion
	transferID := hashStringToU64(id)

	// id: 16 bytes (offset 0)
	copy(buf[0:16], encodeU128(transferID))

	// debit_account_id: 16 bytes (offset 16)
	copy(buf[16:32], encodeU128(debitAccountID))

	// credit_account_id: 16 bytes (offset 32)
	copy(buf[32:48], encodeU128(creditAccountID))

	// amount: 16 bytes (offset 48)
	copy(buf[48:64], encodeU128(amount))

	// pending_id: 16 bytes (offset 64) - zero
	// user_data_128: 16 bytes (offset 80) - zero
	// user_data_64: 8 bytes (offset 96) - zero
	// user_data_32: 4 bytes (offset 104) - zero

	// timeout: 4 bytes (offset 108) - zero

	// ledger: 4 bytes (offset 112)
	copy(buf[112:116], encodeU32(ledger))

	// code: 2 bytes (offset 116)
	copy(buf[116:118], encodeU16(code))

	// flags: 2 bytes (offset 118)
	copy(buf[118:120], encodeU16(flags))

	// timestamp: 8 bytes (offset 120) - will be set by server

	return buf, nil
}

func (e *BinaryEncoder) EncodeTransferWithPending(id string, debitAccountID, creditAccountID uint64, amount uint64, pendingID string, ledger uint32, code uint16, flags uint16) (interface{}, error) {
	// Transfer binary layout: 128 bytes
	buf := make([]byte, 128)

	// Parse ID strings to uint64
	transferID := hashStringToU64(id)
	pendingIDU64 := hashStringToU64(pendingID)

	// id: 16 bytes (offset 0)
	copy(buf[0:16], encodeU128(transferID))

	// debit_account_id: 16 bytes (offset 16)
	copy(buf[16:32], encodeU128(debitAccountID))

	// credit_account_id: 16 bytes (offset 32)
	copy(buf[32:48], encodeU128(creditAccountID))

	// amount: 16 bytes (offset 48)
	copy(buf[48:64], encodeU128(amount))

	// pending_id: 16 bytes (offset 64)
	copy(buf[64:80], encodeU128(pendingIDU64))

	// user_data_128: 16 bytes (offset 80) - zero
	// user_data_64: 8 bytes (offset 96) - zero
	// user_data_32: 4 bytes (offset 104) - zero

	// timeout: 4 bytes (offset 108) - zero

	// ledger: 4 bytes (offset 112)
	copy(buf[112:116], encodeU32(ledger))

	// code: 2 bytes (offset 116)
	copy(buf[116:118], encodeU16(code))

	// flags: 2 bytes (offset 118)
	copy(buf[118:120], encodeU16(flags))

	// timestamp: 8 bytes (offset 120) - will be set by server

	return buf, nil
}

func (e *BinaryEncoder) DecodeAccountID(data interface{}) string {
	buf, ok := data.([]byte)
	if !ok || len(buf) < 16 {
		return ""
	}

	// Extract first 16 bytes as ID
	id := binary.LittleEndian.Uint64(buf[0:8])
	return fmt.Sprintf("%d", id)
}

func (e *BinaryEncoder) DecodeTransferResult(data interface{}) (uint8, error) {
	// Redis returns Lua strings as Go strings, not []byte
	switch v := data.(type) {
	case []byte:
		if len(v) < 1 {
			return 0, fmt.Errorf("invalid result: empty byte array")
		}
		return v[0], nil
	case string:
		if len(v) < 1 {
			return 0, fmt.Errorf("invalid result: empty string")
		}
		return uint8(v[0]), nil
	default:
		return 0, fmt.Errorf("invalid result type: %T", data)
	}
}

// Simple hash function to convert string ID to uint64
func hashStringToU64(s string) uint64 {
	hash := uint64(0)
	for _, c := range s {
		hash = hash*31 + uint64(c)
	}
	return hash
}

// Helper to convert uint64 to 16-byte ID for binary mode
func U64ToID16(val uint64) []byte {
	return encodeU128(val)
}

// Helper to decode 16-byte ID to uint64
func ID16ToU64(id []byte) uint64 {
	if len(id) < 8 {
		return 0
	}
	return binary.LittleEndian.Uint64(id[0:8])
}
