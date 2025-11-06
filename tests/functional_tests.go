package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"
)

// Test fixture
type TestFixture struct {
	ctx                     context.Context
	client                  *redis.Client
	encoder                 *BinaryEncoder
	createAccountSHA        string
	createLinkedAccountsSHA string
	createTransferSHA       string
	createLinkedTransfersSHA string
	lookupAccountSHA        string
	lookupTransferSHA       string
	getTransfersSHA         string
	getBalancesSHA          string
}

func setupTest() (*TestFixture, error) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	// Flush database
	if err := client.FlushDB(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to flush database: %w", err)
	}

	encoder := NewBinaryEncoder()

	// Load scripts
	scripts := map[string]string{
		"createAccount":        "../scripts/create_account.lua",
		"createLinkedAccounts": "../scripts/create_linked_accounts.lua",
		"createTransfer":       "../scripts/create_transfer.lua",
		"createLinkedTransfers": "../scripts/create_linked_transfers.lua",
		"lookupAccount":        "../scripts/lookup_account.lua",
		"lookupTransfer":       "../scripts/lookup_transfer.lua",
		"getTransfers":         "../scripts/get_account_transfers.lua",
		"getBalances":          "../scripts/get_account_balances.lua",
	}

	commonScript, err := os.ReadFile("../scripts/common.lua")
	if err != nil {
		return nil, fmt.Errorf("failed to read common script: %w", err)
	}
	common := string(commonScript) + "\n"

	loadedSHAs := make(map[string]string)
	for name, path := range scripts {
		content, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("failed to read %s: %w", path, err)
		}

		sha, err := client.ScriptLoad(ctx, common+string(content)).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to load script %s: %w", path, err)
		}

		loadedSHAs[name] = sha
	}

	return &TestFixture{
		ctx:                     ctx,
		client:                  client,
		encoder:                 encoder,
		createAccountSHA:        loadedSHAs["createAccount"],
		createLinkedAccountsSHA: loadedSHAs["createLinkedAccounts"],
		createTransferSHA:       loadedSHAs["createTransfer"],
		createLinkedTransfersSHA: loadedSHAs["createLinkedTransfers"],
		lookupAccountSHA:        loadedSHAs["lookupAccount"],
		lookupTransferSHA:       loadedSHAs["lookupTransfer"],
		getTransfersSHA:         loadedSHAs["getTransfers"],
		getBalancesSHA:          loadedSHAs["getBalances"],
	}, nil
}

func (f *TestFixture) cleanup() {
	f.client.Close()
}

// Test helpers
func (f *TestFixture) createAccount(id uint64, ledger uint32, code uint16, flags uint16) (uint8, error) {
	accountData, err := f.encoder.EncodeAccount(id, ledger, code, flags)
	if err != nil {
		return 0, err
	}

	result, err := f.client.EvalSha(f.ctx, f.createAccountSHA, []string{}, accountData).Result()
	if err != nil {
		return 0, err
	}

	return f.encoder.DecodeTransferResult(result)
}

func (f *TestFixture) createTransfer(transferID string, debitID, creditID uint64, amount uint64, ledger uint32, code uint16, flags uint16) (uint8, error) {
	transferData, err := f.encoder.EncodeTransfer(transferID, debitID, creditID, amount, ledger, code, flags)
	if err != nil {
		return 0, err
	}

	result, err := f.client.EvalSha(f.ctx, f.createTransferSHA, []string{}, transferData).Result()
	if err != nil {
		return 0, err
	}

	return f.encoder.DecodeTransferResult(result)
}

func (f *TestFixture) createTransferWithPending(transferID string, debitID, creditID uint64, amount uint64, pendingID string, ledger uint32, code uint16, flags uint16) (uint8, error) {
	transferData, err := f.encoder.EncodeTransferWithPending(transferID, debitID, creditID, amount, pendingID, ledger, code, flags)
	if err != nil {
		return 0, err
	}

	result, err := f.client.EvalSha(f.ctx, f.createTransferSHA, []string{}, transferData).Result()
	if err != nil {
		return 0, err
	}

	return f.encoder.DecodeTransferResult(result)
}

func (f *TestFixture) lookupAccount(id uint64) (map[string]uint64, error) {
	arg := U64ToID16(id)
	result, err := f.client.EvalSha(f.ctx, f.lookupAccountSHA, []string{}, arg).Result()
	if err != nil {
		return nil, err
	}

	return DecodeAccount(result.(string)), nil
}

// Test functions
func testCreateAccount(f *TestFixture) error {
	fmt.Println("\n=== Test: Create Account ===")

	errCode, err := f.createAccount(1, 700, 10, 0)
	if err != nil {
		return fmt.Errorf("create account failed: %w", err)
	}
	if errCode != ErrOK {
		return fmt.Errorf("expected success, got error code %d", errCode)
	}

	account, err := f.lookupAccount(1)
	if err != nil {
		return fmt.Errorf("lookup account failed: %w", err)
	}

	if account["debits_posted"] != 0 || account["credits_posted"] != 0 {
		return fmt.Errorf("account balances should be zero")
	}

	fmt.Println("✓ Account created and verified")
	return nil
}

func testDuplicateAccount(f *TestFixture) error {
	fmt.Println("\n=== Test: Duplicate Account ===")

	errCode, _ := f.createAccount(2, 700, 10, 0)
	if errCode != ErrOK {
		return fmt.Errorf("first account creation failed")
	}

	errCode, _ = f.createAccount(2, 700, 10, 0)
	if errCode != ErrIDAlreadyExists {
		return fmt.Errorf("duplicate account should fail with code %d, got %d", ErrIDAlreadyExists, errCode)
	}

	fmt.Println("✓ Duplicate account correctly rejected")
	return nil
}

func testLinkedAccounts(f *TestFixture) error {
	fmt.Println("\n=== Test: Linked Accounts ===")

	// Create 3 linked accounts
	acc1, _ := f.encoder.EncodeAccount(500, 700, 10, FlagLinked)
	acc2, _ := f.encoder.EncodeAccount(501, 700, 10, FlagLinked)
	acc3, _ := f.encoder.EncodeAccount(502, 700, 10, 0) // End of chain
	accounts := string(acc1.([]byte)) + string(acc2.([]byte)) + string(acc3.([]byte))

	_, err := f.client.EvalSha(f.ctx, f.createLinkedAccountsSHA, []string{}, accounts).Result()
	if err != nil {
		return fmt.Errorf("linked accounts creation failed: %w", err)
	}

	// Verify all accounts created
	account1, _ := f.lookupAccount(500)
	account2, _ := f.lookupAccount(501)
	account3, _ := f.lookupAccount(502)

	if account1 == nil || account2 == nil || account3 == nil {
		return fmt.Errorf("linked accounts not all created")
	}

	fmt.Println("✓ Linked accounts created successfully")
	return nil
}

func testLinkedAccountsRollback(f *TestFixture) error {
	fmt.Println("\n=== Test: Linked Accounts Rollback ===")

	// Create first account separately
	f.createAccount(600, 700, 10, 0)

	// Try to create linked chain with duplicate
	acc1, _ := f.encoder.EncodeAccount(601, 700, 10, FlagLinked)
	acc2, _ := f.encoder.EncodeAccount(600, 700, 10, 0) // Duplicate
	accounts := string(acc1.([]byte)) + string(acc2.([]byte))

	f.client.EvalSha(f.ctx, f.createLinkedAccountsSHA, []string{}, accounts).Result()

	// Verify account 601 was NOT created (rolled back)
	result, _ := f.client.EvalSha(f.ctx, f.lookupAccountSHA, []string{}, U64ToID16(601)).Result()
	if result.(string) != "" {
		return fmt.Errorf("account 601 should be rolled back")
	}

	fmt.Println("✓ Linked accounts rollback successful")
	return nil
}

func testLookupTransfer(f *TestFixture) error {
	fmt.Println("\n=== Test: Lookup Transfer ===")

	f.createAccount(700, 700, 10, 0)
	f.createAccount(701, 700, 10, 0)

	transferID := "lookup_test_tx"
	errCode, _ := f.createTransfer(transferID, 700, 701, 250, 700, 10, 0)
	if errCode != ErrOK {
		return fmt.Errorf("transfer creation failed")
	}

	// Lookup transfer
	tid := HashString(transferID)
	result, err := f.client.EvalSha(f.ctx, f.lookupTransferSHA, []string{}, U64ToID16(tid)).Result()
	if err != nil {
		return fmt.Errorf("lookup transfer failed: %w", err)
	}

	transferData := result.(string)
	debitID := binary.LittleEndian.Uint64([]byte(transferData)[16:24])
	creditID := binary.LittleEndian.Uint64([]byte(transferData)[32:40])
	amount := binary.LittleEndian.Uint64([]byte(transferData)[48:56])

	if debitID != 700 || creditID != 701 || amount != 250 {
		return fmt.Errorf("transfer data mismatch")
	}

	fmt.Println("✓ Lookup transfer successful")
	return nil
}

func testLinkedTransfersRollback(f *TestFixture) error {
	fmt.Println("\n=== Test: Linked Transfers Rollback ===")

	f.createAccount(800, 700, 10, 0)
	f.createAccount(801, 700, 10, 0)

	// Create linked transfers where second fails
	tr1, _ := f.encoder.EncodeTransfer("rollback1", 800, 801, 100, 700, 10, FlagLinked)
	tr2, _ := f.encoder.EncodeTransfer("rollback2", 800, 999, 50, 700, 10, 0) // Account 999 doesn't exist
	transfers := string(tr1.([]byte)) + string(tr2.([]byte))

	f.client.EvalSha(f.ctx, f.createLinkedTransfersSHA, []string{}, transfers).Result()

	// Verify first transfer was rolled back
	account1, _ := f.lookupAccount(800)
	account2, _ := f.lookupAccount(801)

	if account1["debits_posted"] != 0 || account2["credits_posted"] != 0 {
		return fmt.Errorf("transfers should be rolled back")
	}

	fmt.Println("✓ Linked transfers rollback successful")
	return nil
}

func testSimpleTransfer(f *TestFixture) error {
	fmt.Println("\n=== Test: Simple Transfer ===")

	f.createAccount(10, 700, 10, 0)
	f.createAccount(11, 700, 10, 0)

	errCode, err := f.createTransfer("transfer1", 10, 11, 1000, 700, 10, 0)
	if err != nil || errCode != ErrOK {
		return fmt.Errorf("transfer failed")
	}

	account1, _ := f.lookupAccount(10)
	account2, _ := f.lookupAccount(11)

	if account1["debits_posted"] != 1000 {
		return fmt.Errorf("debit account should have 1000 debits")
	}
	if account2["credits_posted"] != 1000 {
		return fmt.Errorf("credit account should have 1000 credits")
	}

	fmt.Println("✓ Transfer completed successfully")
	return nil
}

func testTwoPhaseTransfers(f *TestFixture) error {
	fmt.Println("\n=== Test: Two-Phase Transfers ===")

	// Create accounts
	f.createAccount(30, 700, 10, 0)
	f.createAccount(31, 700, 10, 0)

	// Create pending
	errCode, _ := f.createTransfer("pending1", 30, 31, 500, 700, 10, FlagPending)
	if errCode != ErrOK {
		return fmt.Errorf("pending transfer failed")
	}

	account1, _ := f.lookupAccount(30)
	if account1["debits_pending"] != 500 {
		return fmt.Errorf("should have pending debits")
	}

	// Post pending
	errCode, _ = f.createTransferWithPending("post1", 30, 31, 500, "pending1", 700, 10, FlagPostPending)
	if errCode != ErrOK {
		return fmt.Errorf("post transfer failed")
	}

	account1, _ = f.lookupAccount(30)
	if account1["debits_pending"] != 0 || account1["debits_posted"] != 500 {
		return fmt.Errorf("debits should move from pending to posted")
	}

	// Test void
	f.createAccount(40, 700, 10, 0)
	f.createAccount(41, 700, 10, 0)
	f.createTransfer("pending2", 40, 41, 700, 700, 10, FlagPending)

	errCode, _ = f.createTransferWithPending("void1", 40, 41, 700, "pending2", 700, 10, FlagVoidPending)
	if errCode != ErrOK {
		return fmt.Errorf("void transfer failed")
	}

	account2, _ := f.lookupAccount(40)
	if account2["debits_pending"] != 0 || account2["debits_posted"] != 0 {
		return fmt.Errorf("balances should be cleared after void")
	}

	fmt.Println("✓ Two-phase transfers successful")
	return nil
}

func testGetAccountTransfers(f *TestFixture) error {
	fmt.Println("\n=== Test: Get Account Transfers ===")

	// Create accounts
	f.createAccount(100, 700, 10, 0)
	f.createAccount(101, 700, 10, 0)

	// Create transfers
	for i := 0; i < 3; i++ {
		transferID := fmt.Sprintf("query_tx_%d", i)
		errCode, _ := f.createTransfer(transferID, 100, 101, uint64(100*(i+1)), 700, 10, 0)
		if errCode != ErrOK {
			return fmt.Errorf("transfer %d failed", i)
		}
	}

	// Query all transfers
	filter := f.encoder.EncodeAccountFilter(100, 0, ^uint64(0), 10, FilterDebits|FilterCredits)
	result, err := f.client.EvalSha(f.ctx, f.getTransfersSHA, []string{}, string(filter)).Result()
	if err != nil {
		return fmt.Errorf("get transfers failed: %w", err)
	}

	transfersBlob := result.(string)
	numTransfers := len(transfersBlob) / 128
	if numTransfers != 3 {
		return fmt.Errorf("expected 3 transfers, got %d", numTransfers)
	}

	// Query with limit
	filter = f.encoder.EncodeAccountFilter(100, 0, ^uint64(0), 2, FilterDebits)
	result, _ = f.client.EvalSha(f.ctx, f.getTransfersSHA, []string{}, string(filter)).Result()

	transfersBlob = result.(string)
	numTransfers = len(transfersBlob) / 128
	if numTransfers != 2 {
		return fmt.Errorf("expected 2 transfers with limit, got %d", numTransfers)
	}

	fmt.Println("✓ Get account transfers successful")
	return nil
}

func testGetAccountBalances(f *TestFixture) error {
	fmt.Println("\n=== Test: Get Account Balances ===")

	// Create account with HISTORY flag
	f.createAccount(200, 700, 10, FlagHistory)
	f.createAccount(201, 700, 10, 0)

	// Create transfers to generate balance history
	f.createTransfer("balance_tx_0", 200, 201, 150, 700, 10, 0)
	f.createTransfer("balance_tx_1", 200, 201, 150, 700, 10, 0)

	// Query balance history
	filter := f.encoder.EncodeAccountFilter(200, 0, ^uint64(0), 10, 0)
	result, err := f.client.EvalSha(f.ctx, f.getBalancesSHA, []string{}, string(filter)).Result()
	if err != nil {
		return fmt.Errorf("get balances failed: %w", err)
	}

	balancesBlob := result.(string)
	numBalances := len(balancesBlob) / 128
	if numBalances != 2 {
		return fmt.Errorf("expected 2 balance snapshots, got %d", numBalances)
	}

	// Verify balances increase
	balance1 := binary.LittleEndian.Uint64([]byte(balancesBlob)[24:32])  // First snapshot debits_posted
	balance2 := binary.LittleEndian.Uint64([]byte(balancesBlob)[128+24 : 128+32])  // Second snapshot

	if balance1 != 150 || balance2 != 300 {
		return fmt.Errorf("balances should increase (150, 300): got %d, %d", balance1, balance2)
	}

	// Test account without HISTORY flag
	filter = f.encoder.EncodeAccountFilter(201, 0, ^uint64(0), 10, 0)
	result, _ = f.client.EvalSha(f.ctx, f.getBalancesSHA, []string{}, string(filter)).Result()

	balancesBlob = result.(string)
	if len(balancesBlob) != 0 {
		return fmt.Errorf("account without HISTORY should return empty")
	}

	fmt.Println("✓ Get account balances successful")
	return nil
}

func testMultipleTransfers(f *TestFixture) error {
	fmt.Println("\n=== Test: Multiple Transfers ===")

	f.createAccount(80, 700, 10, 0)
	f.createAccount(81, 700, 10, 0)

	for i := 0; i < 5; i++ {
		transferID := fmt.Sprintf("multi_transfer_%d", i)
		errCode, _ := f.createTransfer(transferID, 80, 81, 100, 700, 10, 0)
		if errCode != ErrOK {
			return fmt.Errorf("transfer %d failed", i)
		}
	}

	account1, _ := f.lookupAccount(80)
	account2, _ := f.lookupAccount(81)

	if account1["debits_posted"] != 500 || account2["credits_posted"] != 500 {
		return fmt.Errorf("total balances incorrect")
	}

	fmt.Println("✓ Multiple transfers successful")
	return nil
}

func main() {
	fmt.Println("============================================================")
	fmt.Println("Lua Beetle Comprehensive Functional Tests (Go)")
	fmt.Println("============================================================")

	f, err := setupTest()
	if err != nil {
		fmt.Printf("❌ Setup failed: %v\n", err)
		os.Exit(1)
	}
	defer f.cleanup()

	tests := []struct {
		name string
		fn   func(*TestFixture) error
	}{
		{"Create Account", testCreateAccount},
		{"Duplicate Account", testDuplicateAccount},
		{"Linked Accounts", testLinkedAccounts},
		{"Linked Accounts Rollback", testLinkedAccountsRollback},
		{"Simple Transfer", testSimpleTransfer},
		{"Two-Phase Transfers", testTwoPhaseTransfers},
		{"Lookup Transfer", testLookupTransfer},
		{"Get Account Transfers", testGetAccountTransfers},
		{"Get Account Balances", testGetAccountBalances},
		{"Linked Transfers Rollback", testLinkedTransfersRollback},
		{"Multiple Transfers", testMultipleTransfers},
	}

	for _, test := range tests {
		if err := test.fn(f); err != nil {
			fmt.Printf("❌ Test failed: %s - %v\n", test.name, err)
			os.Exit(1)
		}
	}

	fmt.Println("\n============================================================")
	fmt.Println("✅ All tests passed!")
	fmt.Println("============================================================")
}
