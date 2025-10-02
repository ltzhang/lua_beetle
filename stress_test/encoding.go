package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

// Encoder interface for account/transfer encoding
type Encoder interface {
	EncodeAccount(id uint64, ledger uint32, code uint16, flags uint16) (interface{}, error)
	EncodeTransfer(id string, debitAccountID, creditAccountID uint64, amount uint64, ledger uint32, code uint16, flags uint16) (interface{}, error)
	DecodeAccountID(data interface{}) string
	DecodeTransferResult(data interface{}) (uint8, error)
}

// JSONEncoder implements text-based JSON encoding
type JSONEncoder struct{}

func NewJSONEncoder() *JSONEncoder {
	return &JSONEncoder{}
}

func (e *JSONEncoder) EncodeAccount(id uint64, ledger uint32, code uint16, flags uint16) (interface{}, error) {
	account := map[string]interface{}{
		"id":     fmt.Sprintf("%d", id),
		"ledger": ledger,
		"code":   code,
		"flags":  flags,
	}
	return json.Marshal(account)
}

func (e *JSONEncoder) EncodeTransfer(id string, debitAccountID, creditAccountID uint64, amount uint64, ledger uint32, code uint16, flags uint16) (interface{}, error) {
	transfer := map[string]interface{}{
		"id":                id,
		"debit_account_id":  fmt.Sprintf("%d", debitAccountID),
		"credit_account_id": fmt.Sprintf("%d", creditAccountID),
		"amount":            amount,
		"ledger":            ledger,
		"code":              code,
		"flags":             flags,
	}
	return json.Marshal(transfer)
}

func (e *JSONEncoder) DecodeAccountID(data interface{}) string {
	// For JSON, this would parse the JSON and extract ID
	// For now, simplified
	return ""
}

func (e *JSONEncoder) DecodeTransferResult(data interface{}) (uint8, error) {
	str, ok := data.(string)
	if !ok {
		return 0, fmt.Errorf("invalid result type")
	}

	var resultObj map[string]interface{}
	if err := json.Unmarshal([]byte(str), &resultObj); err != nil {
		return 0, err
	}

	errCode, ok := resultObj["result"].(float64)
	if !ok {
		return 0, fmt.Errorf("result field missing")
	}

	return uint8(errCode), nil
}

// BinaryEncoder implements fixed-size binary encoding
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
	for i, c := range s {
		hash = hash*31 + uint64(c)
		if i > 8 {
			break
		}
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
