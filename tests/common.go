package main

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
)

// ============================================================================
// Error codes matching TigerBeetle
// ============================================================================

const (
	ErrOK                           = 0
	ErrLinkedEventChainOpen         = 1
	ErrIDAlreadyExists              = 21
	ErrExistsWithDifferentFlags     = 29
	ErrPendingTransferNotFound      = 34
	ErrPendingTransferAlreadyPosted = 35
	ErrPendingTransferAlreadyVoided = 36
	ErrDebitAccountNotFound         = 38
	ErrCreditAccountNotFound        = 39
	ErrAccountsMustBeDifferent      = 40
	ErrExceedsCredits               = 42
	ErrExceedsDebits                = 43
	ErrLedgerMustMatch              = 52
)

// ============================================================================
// Flags
// ============================================================================

const (
	FlagLinked      = 0x0001
	FlagPending     = 0x0002
	FlagPostPending = 0x0004
	FlagVoidPending = 0x0008
	FlagHistory     = 0x0008 // Account flag
)

// ============================================================================
// Filter flags
// ============================================================================

const (
	FilterDebits   = 0x01
	FilterCredits  = 0x02
	FilterReversed = 0x04
)

// ============================================================================
// Workload Types
// ============================================================================

// WorkloadType defines the type of workload to run
type WorkloadType string

const (
	WorkloadTransfer   WorkloadType = "transfer"
	WorkloadLookup     WorkloadType = "lookup"
	WorkloadTwoPhase   WorkloadType = "twophase"
	WorkloadMixed      WorkloadType = "mixed"
)

// StressTestConfig defines parameters for the stress test
type StressTestConfig struct {
	NumAccounts    int          // Total number of accounts to create
	NumHotAccounts int          // Number of hot accounts (rest are cold)
	NumWorkers     int          // Number of concurrent workers
	Duration       int          // Test duration in seconds
	Workload       WorkloadType // Type of workload to run
	TransferRatio  float64      // For mixed workload: ratio of transfers (rest are lookups)
	TwoPhaseRatio  float64      // For mixed workload: ratio of two-phase transfers within transfers
	BatchSize      int          // Number of operations per batch
	LedgerID       uint32       // Ledger ID for all accounts/transfers
	Verbose        bool         // Enable verbose output
}

// TestMetrics tracks performance metrics
type TestMetrics struct {
	OperationsCompleted  atomic.Uint64
	OperationsFailed     atomic.Uint64
	TransfersCreated     atomic.Uint64
	TwoPhaseCreated      atomic.Uint64
	TwoPhasePending      atomic.Uint64
	TwoPhasePosted       atomic.Uint64
	TwoPhaseVoided       atomic.Uint64
	AccountsLookedup     atomic.Uint64
	TotalLatencyNs       atomic.Uint64
	StartTime            time.Time
	EndTime              time.Time
}

// HotColdGenerator generates account IDs with hot/cold distribution
type HotColdGenerator struct {
	rng            *rand.Rand
	numAccounts    int
	numHotAccounts int
	offset         uint64
}

// NewHotColdGenerator creates a hot/cold distribution generator
func NewHotColdGenerator(numAccounts, numHotAccounts int, seed int64, offset uint64) *HotColdGenerator {
	return &HotColdGenerator{
		rng:            rand.New(rand.NewSource(seed)),
		numAccounts:    numAccounts,
		numHotAccounts: numHotAccounts,
		offset:         offset,
	}
}

// NextHot returns a random hot account ID
func (h *HotColdGenerator) NextHot() uint64 {
	return uint64(h.rng.Intn(h.numHotAccounts)) + 1 + h.offset
}

// NextAny returns a random account ID (hot or cold)
func (h *HotColdGenerator) NextAny() uint64 {
	return uint64(h.rng.Intn(h.numAccounts)) + 1 + h.offset
}

// NextHotAndAny returns a pair: one hot account and one random account
// Ensures they are different
func (h *HotColdGenerator) NextHotAndAny() (hot uint64, any uint64) {
	hot = h.NextHot()
	any = h.NextAny()

	// Ensure different accounts (max 100 attempts)
	attempts := 0
	for any == hot && attempts < 100 {
		any = h.NextAny()
		attempts++
	}

	return hot, any
}


// PrintMetrics prints the final metrics
func PrintMetrics(metrics *TestMetrics, testName string) {
	duration := metrics.EndTime.Sub(metrics.StartTime).Seconds()
	completed := metrics.OperationsCompleted.Load()
	failed := metrics.OperationsFailed.Load()
	transfers := metrics.TransfersCreated.Load()
	twoPhaseTotal := metrics.TwoPhaseCreated.Load()
	twoPhasePending := metrics.TwoPhasePending.Load()
	twoPhasePosted := metrics.TwoPhasePosted.Load()
	twoPhaseVoided := metrics.TwoPhaseVoided.Load()
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
	if twoPhaseTotal > 0 {
		fmt.Printf("Two-Phase Transfers: %d (Pending: %d, Posted: %d, Voided: %d)\n",
			twoPhaseTotal, twoPhasePending, twoPhasePosted, twoPhaseVoided)
	}
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
	transferID := HashString(id)

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
	transferID := HashString(id)
	pendingIDU64 := HashString(pendingID)

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

// HashString is a simple hash function to convert string ID to uint64
func HashString(s string) uint64 {
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

// ============================================================================
// Additional encoding methods for test_functional.go
// ============================================================================

// EncodeAccountFilter encodes AccountFilter to 128-byte binary format
func (e *BinaryEncoder) EncodeAccountFilter(accountID uint64, timestampMin, timestampMax uint64, limit uint32, flags uint32) []byte {
	buf := make([]byte, 128)

	copy(buf[0:16], encodeU128(accountID))
	binary.LittleEndian.PutUint64(buf[48:56], timestampMin)
	binary.LittleEndian.PutUint64(buf[56:64], timestampMax)
	binary.LittleEndian.PutUint32(buf[64:68], limit)
	binary.LittleEndian.PutUint32(buf[68:72], flags)

	return buf
}

// DecodeAccount decodes account from 128-byte binary format
func DecodeAccount(data string) map[string]uint64 {
	if len(data) < 128 {
		return nil
	}

	buf := []byte(data)
	return map[string]uint64{
		"id":              binary.LittleEndian.Uint64(buf[0:8]),
		"debits_pending":  binary.LittleEndian.Uint64(buf[16:24]),
		"debits_posted":   binary.LittleEndian.Uint64(buf[32:40]),
		"credits_pending": binary.LittleEndian.Uint64(buf[48:56]),
		"credits_posted":  binary.LittleEndian.Uint64(buf[64:72]),
		"ledger":          uint64(binary.LittleEndian.Uint32(buf[112:116])),
		"code":            uint64(binary.LittleEndian.Uint16(buf[116:118])),
		"flags":           uint64(binary.LittleEndian.Uint16(buf[118:120])),
		"timestamp":       binary.LittleEndian.Uint64(buf[120:128]),
	}
}

// DecodeTransfer decodes transfer from 128-byte binary format
func DecodeTransfer(data string) map[string]uint64 {
	if len(data) < 128 {
		return nil
	}

	buf := []byte(data)
	return map[string]uint64{
		"id":                binary.LittleEndian.Uint64(buf[0:8]),
		"debit_account_id":  binary.LittleEndian.Uint64(buf[16:24]),
		"credit_account_id": binary.LittleEndian.Uint64(buf[32:40]),
		"amount":            binary.LittleEndian.Uint64(buf[48:56]),
		"pending_id":        binary.LittleEndian.Uint64(buf[64:72]),
		"ledger":            uint64(binary.LittleEndian.Uint32(buf[112:116])),
		"code":              uint64(binary.LittleEndian.Uint16(buf[116:118])),
		"flags":             uint64(binary.LittleEndian.Uint16(buf[118:120])),
		"timestamp":         binary.LittleEndian.Uint64(buf[120:128]),
	}
}
