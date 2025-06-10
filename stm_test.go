package block_stm

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	storetypes "cosmossdk.io/store/types"
	"github.com/test-go/testify/require"
)

func accountName(i int64) string {
	return fmt.Sprintf("account%05d", i)
}

func testBlock(size int, accounts int) *MockBlock {
	txs := make([]Tx, size)
	g := rand.New(rand.NewSource(0))
	for i := 0; i < size; i++ {
		sender := g.Int63n(int64(accounts))
		receiver := g.Int63n(int64(accounts))
		txs[i] = BankTransferTx(i, accountName(sender), accountName(receiver), 1)
	}
	return NewMockBlock(txs)
}

func iterateBlock(size int, accounts int) *MockBlock {
	txs := make([]Tx, size)
	g := rand.New(rand.NewSource(0))
	for i := 0; i < size; i++ {
		sender := g.Int63n(int64(accounts))
		receiver := g.Int63n(int64(accounts))
		txs[i] = IterateTx(i, accountName(sender), accountName(receiver), 1)
	}
	return NewMockBlock(txs)
}

func noConflictBlock(size int) *MockBlock {
	txs := make([]Tx, size)
	for i := 0; i < size; i++ {
		sender := accountName(int64(i))
		txs[i] = BankTransferTx(i, sender, sender, 1)
	}
	return NewMockBlock(txs)
}

func worstCaseBlock(size int) *MockBlock {
	txs := make([]Tx, size)
	for i := 0; i < size; i++ {
		// all transactions are from the same account
		sender := "account0"
		txs[i] = BankTransferTx(i, sender, sender, 1)
	}
	return NewMockBlock(txs)
}

func determisticBlock() *MockBlock {
	return NewMockBlock([]Tx{
		NoopTx(0, "account0"),
		NoopTx(1, "account1"),
		NoopTx(2, "account1"),
		NoopTx(3, "account1"),
		NoopTx(4, "account3"),
		NoopTx(5, "account1"),
		NoopTx(6, "account4"),
		NoopTx(7, "account5"),
		NoopTx(8, "account6"),
	})
}

// conflictBlock 生成指定冲突率的测试block
// 设计思路：构建一个长的转账链条作为主链，冲突率决定主链长度
// conflictRate: 0.0-1.0之间，表示参与主链的交易比例
// size: 交易总数
func conflictBlock(size int, conflictRate float64) *MockBlock {
	txs := make([]Tx, size)
	g := rand.New(rand.NewSource(0))
	
	// 计算主链长度（参与冲突的交易数量）
	mainChainLength := int(float64(size) * conflictRate)
	
	// 构建主链：account0 -> account1 -> account2 -> ... -> account(mainChainLength)
	// 这形成了一个必须串行执行的依赖链
	for i := 0; i < mainChainLength; i++ {
		sender := accountName(int64(i))
		receiver := accountName(int64(i + 1))
		txs[i] = BankTransferTx(i, sender, receiver, 1)
	}
	
	// 构建侧枝：剩余的交易，可以是独立交易或短依赖链
	// 但侧枝长度不会超过主链
	remainingTxs := size - mainChainLength
	if remainingTxs > 0 {
		// 计算侧枝数量，每个侧枝最大长度为主链长度的1/3（确保不超过主链）
		maxSideBranchLength := max(1, mainChainLength/3)
		if maxSideBranchLength == 0 {
			maxSideBranchLength = 1
		}
		
		txIdx := mainChainLength
		baseAccountOffset := mainChainLength + 100 // 避免与主链账户冲突
		
		for txIdx < size {
			// 随机决定侧枝长度（1到maxSideBranchLength之间）
			sideBranchLength := 1 + g.Intn(maxSideBranchLength)
			if txIdx + sideBranchLength > size {
				sideBranchLength = size - txIdx
			}
			
			// 创建一个侧枝
			branchBaseAccount := baseAccountOffset + (txIdx-mainChainLength)*10
			for j := 0; j < sideBranchLength; j++ {
				sender := accountName(int64(branchBaseAccount + j))
				receiver := accountName(int64(branchBaseAccount + j + 1))
				txs[txIdx] = BankTransferTx(txIdx, sender, receiver, 1)
				txIdx++
			}
		}
	}
	
	return NewMockBlock(txs)
}

// 50%冲突率测试案例
func conflict50Block(size int) *MockBlock {
	return conflictBlock(size, 0.5)
}

// 60%冲突率测试案例  
func conflict60Block(size int) *MockBlock {
	return conflictBlock(size, 0.6)
}

// 70%冲突率测试案例
func conflict70Block(size int) *MockBlock {
	return conflictBlock(size, 0.7)
}

// 80%冲突率测试案例
func conflict80Block(size int) *MockBlock {
	return conflictBlock(size, 0.8)
}

// 90%冲突率测试案例
func conflict90Block(size int) *MockBlock {
	return conflictBlock(size, 0.9)
}

// 100%冲突率测试案例（与worstCaseBlock类似，但更明确）
func conflict100Block(size int) *MockBlock {
	return conflictBlock(size, 1.0)
}

func TestSTM(t *testing.T) {
	stores := map[storetypes.StoreKey]int{StoreKeyAuth: 0, StoreKeyBank: 1}
	testCases := []struct {
		name      string
		blk       *MockBlock
		executors int
	}{
		{
			name:      "testBlock(100,80),10",
			blk:       testBlock(100, 80),
			executors: 10,
		},
		{
			name:      "testBlock(100,3),10",
			blk:       testBlock(100, 3),
			executors: 10,
		},
		{
			name:      "determisticBlock(),5",
			blk:       determisticBlock(),
			executors: 5,
		},
		{
			name:      "noConflictBlock(100),5",
			blk:       noConflictBlock(100),
			executors: 5,
		},
		{
			name:      "worstCaseBlock(100),5",
			blk:       worstCaseBlock(100),
			executors: 5,
		},
		{
			name:      "iterateBlock(100,80),10",
			blk:       iterateBlock(100, 80),
			executors: 10,
		},
		{
			name:      "iterateBlock(100,10),10",
			blk:       iterateBlock(100, 10),
			executors: 10,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			storage := NewMultiMemDB(stores)
			require.NoError(t,
				ExecuteBlock(context.Background(), tc.blk.Size(), stores, storage, tc.executors, tc.blk.ExecuteTx),
			)
			for _, err := range tc.blk.Results {
				require.NoError(t, err)
			}

			crossCheck := NewMultiMemDB(stores)
			runSequential(crossCheck, tc.blk)

			// check parallel execution matches sequential execution
			for store := range stores {
				require.True(t, StoreEqual(crossCheck.GetKVStore(store), storage.GetKVStore(store)))
			}

			// check total nonce increased the same amount as the number of transactions
			var total uint64
			store := storage.GetKVStore(StoreKeyAuth)
			it := store.Iterator(nil, nil)
			defer it.Close()

			for ; it.Valid(); it.Next() {
				if !bytes.HasPrefix(it.Key(), []byte("nonce")) {
					continue
				}
				total += binary.BigEndian.Uint64(it.Value())
				continue
			}
			require.Equal(t, uint64(tc.blk.Size()), total)
		})
	}
}

func StoreEqual(a, b storetypes.KVStore) bool {
	// compare with iterators
	iter1 := a.Iterator(nil, nil)
	iter2 := b.Iterator(nil, nil)
	defer iter1.Close()
	defer iter2.Close()

	for {
		if !iter1.Valid() && !iter2.Valid() {
			return true
		}
		if !iter1.Valid() || !iter2.Valid() {
			return false
		}
		if !bytes.Equal(iter1.Key(), iter2.Key()) || !bytes.Equal(iter1.Value(), iter2.Value()) {
			return false
		}
		iter1.Next()
		iter2.Next()
	}
}

// TestConflictRateFunctions 测试新的冲突率函数是否正常工作
func TestConflictRateFunctions(t *testing.T) {
	testCases := []struct {
		name     string
		blockFn  func(int) *MockBlock
		size     int
		expected string
	}{
		{"conflict50", conflict50Block, 100, "50% conflict rate"},
		{"conflict60", conflict60Block, 100, "60% conflict rate"},
		{"conflict70", conflict70Block, 100, "70% conflict rate"},
		{"conflict80", conflict80Block, 100, "80% conflict rate"},
		{"conflict90", conflict90Block, 100, "90% conflict rate"},
		{"conflict100", conflict100Block, 100, "100% conflict rate"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			block := tc.blockFn(tc.size)
			require.NotNil(t, block)
			require.Equal(t, tc.size, block.Size())
			require.Equal(t, tc.size, len(block.Txs))
			t.Logf("Successfully created %s with %d transactions", tc.expected, block.Size())
		})
	}
}

// TestConflictAnalysis 分析不同冲突策略的依赖关系
func TestConflictAnalysis(t *testing.T) {
	size := 20 // 使用小一些的size便于分析
	
	testCases := []struct {
		name    string
		blockFn func(int) *MockBlock
	}{
		{"worst-case", worstCaseBlock},     // 原始的最坏情况：所有交易访问同一账户
		{"conflict-100%", conflict100Block}, // 新的100%冲突：长链条
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			block := tc.blockFn(size)
			
			t.Logf("=== %s 交易依赖分析 ===", tc.name)
			
			// 分析账户访问模式 - 通过实际执行交易来观察
			stores := map[storetypes.StoreKey]int{StoreKeyAuth: 0, StoreKeyBank: 1}
			
			// 记录每个交易访问的账户
			accountAccess := make(map[string][]int)
			
			for i, tx := range block.Txs {
				// 创建一个临时存储来执行交易
				tempStorage := NewMultiMemDB(stores)
				
				// 执行交易前后，比较存储状态来推断访问的账户
				beforeAuth := getAllKeys(tempStorage.GetKVStore(StoreKeyAuth))
				beforeBank := getAllKeys(tempStorage.GetKVStore(StoreKeyBank))
				
				// 执行交易
				err := tx(tempStorage)
				require.NoError(t, err)
				
				// 获取执行后的键
				afterAuth := getAllKeys(tempStorage.GetKVStore(StoreKeyAuth))
				afterBank := getAllKeys(tempStorage.GetKVStore(StoreKeyBank))
				
				// 找出新增或修改的键（表示被访问的账户）
				var accessedAccounts []string
				for _, key := range afterAuth {
					if !contains(beforeAuth, key) {
						if bytes.HasPrefix([]byte(key), []byte("nonce")) {
							account := strings.TrimPrefix(key, "nonce")
							accessedAccounts = append(accessedAccounts, account)
						}
					}
				}
				for _, key := range afterBank {
					if !contains(beforeBank, key) {
						if bytes.HasPrefix([]byte(key), []byte("balance")) {
							account := strings.TrimPrefix(key, "balance")
							if !containsString(accessedAccounts, account) {
								accessedAccounts = append(accessedAccounts, account)
							}
						}
					}
				}
				
				t.Logf("Transaction %d 访问账户: %v", i, accessedAccounts)
				
				// 记录账户访问
				for _, account := range accessedAccounts {
					accountAccess[account] = append(accountAccess[account], i)
				}
			}
			
			// 分析冲突情况
			t.Logf("账户访问统计:")
			conflictCount := 0
			maxConflictSize := 0
			for account, txIndices := range accountAccess {
				if len(txIndices) > 1 {
					conflictCount++
					if len(txIndices) > maxConflictSize {
						maxConflictSize = len(txIndices)
					}
					t.Logf("  账户 %s 被 %d 个交易访问: %v (冲突)", account, len(txIndices), txIndices)
				} else {
					t.Logf("  账户 %s 被 1 个交易访问: %v (无冲突)", account, txIndices)
				}
			}
			
			t.Logf("冲突统计: %d 个账户有冲突，最大冲突度: %d", conflictCount, maxConflictSize)
		})
	}
}

// 辅助函数：获取store中的所有键
func getAllKeys(store storetypes.KVStore) []string {
	var keys []string
	iter := store.Iterator(nil, nil)
	defer iter.Close()
	
	for ; iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	return keys
}

// 辅助函数：检查切片是否包含某个元素
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// 辅助函数：检查字符串切片是否包含某个元素
func containsString(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}