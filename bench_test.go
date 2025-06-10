package block_stm

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	storetypes "cosmossdk.io/store/types"
	"github.com/test-go/testify/require"
)

func BenchmarkBlockSTM(b *testing.B) {
	stores := map[storetypes.StoreKey]int{StoreKeyAuth: 0, StoreKeyBank: 1}
	for i := 0; i < 26; i++ {
		key := storetypes.NewKVStoreKey(strconv.FormatInt(int64(i), 10))
		stores[key] = i + 2
	}
	storage := NewMultiMemDB(stores)
	testCases := []struct {
		name  string
		block *MockBlock
	}{
		{"random-10000/100", testBlock(10000, 100)},
		{"no-conflict-10000", noConflictBlock(10000)},
		{"worst-case-10000", worstCaseBlock(10000)},
		{"iterate-10000/100", iterateBlock(10000, 100)},
		{"conflict-50%-10000", conflict50Block(10000)},
		{"conflict-60%-10000", conflict60Block(10000)},
		{"conflict-70%-10000", conflict70Block(10000)},
		{"conflict-80%-10000", conflict80Block(10000)},
		{"conflict-90%-10000", conflict90Block(10000)},
		{"conflict-100%-10000", conflict100Block(10000)},
	}
	for _, tc := range testCases {
		b.Run(tc.name+"-sequential", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runSequential(storage, tc.block)
			}
		})
		for _, worker := range []int{1, 5, 10, 15, 20} {
			b.Run(tc.name+"-worker-"+strconv.Itoa(worker), func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					require.NoError(
						b,
						ExecuteBlock(context.Background(), tc.block.Size(), stores, storage, worker, tc.block.ExecuteTx),
					)
				}
			})
		}
	}
}

// BenchmarkBlockSTMWithSpeedup 运行benchmark并计算加速比
func BenchmarkBlockSTMWithSpeedup(b *testing.B) {
	stores := map[storetypes.StoreKey]int{StoreKeyAuth: 0, StoreKeyBank: 1}
	for i := 0; i < 26; i++ {
		key := storetypes.NewKVStoreKey(strconv.FormatInt(int64(i), 10))
		stores[key] = i + 2
	}
	
	testCases := []struct {
		name  string
		block *MockBlock
	}{
		// {"random-10000/100", testBlock(10000, 100)},
		// {"no-conflict-10000", noConflictBlock(10000)},
		// {"worst-case-10000", worstCaseBlock(10000)},
		// {"iterate-10000/100", iterateBlock(10000, 100)},
		// {"conflict-50%-10000", conflict50Block(10000)},
		// {"conflict-60%-10000", conflict60Block(10000)},
		// {"conflict-70%-10000", conflict70Block(10000)},
		// {"conflict-80%-10000", conflict80Block(10000)},
		// {"conflict-90%-10000", conflict90Block(10000)},
		{"conflict-100%-2000", conflict100Block(2000)},
	}
	
	workers := []int{1, 2, 4, 8, 16, 32}
	iterations := 5 // 每个测试运行的次数
	
	fmt.Printf("\n=== Block-STM Performance Analysis ===\n")
	fmt.Printf("%-20s | %-8s | %-12s | %-12s | %-8s\n", "Test Case", "Workers", "Sequential", "Parallel", "Speedup")
	fmt.Printf("%-20s-+-%-8s-+-%-12s-+-%-12s-+-%-8s\n", "--------------------", "--------", "------------", "------------", "--------")
	
	for _, tc := range testCases {
		// 测量Sequential时间
		var sequentialTotal time.Duration
		for i := 0; i < iterations; i++ {
			storage := NewMultiMemDB(stores)
			start := time.Now()
			runSequential(storage, tc.block)
			sequentialTotal += time.Since(start)
		}
		sequentialAvg := sequentialTotal / time.Duration(iterations)
		
		// 测试不同worker数量的并行执行
		for _, worker := range workers {
			var parallelTotal time.Duration
			for i := 0; i < iterations; i++ {
				storage := NewMultiMemDB(stores)
				start := time.Now()
				err := ExecuteBlock(context.Background(), tc.block.Size(), stores, storage, worker, tc.block.ExecuteTx)
				if err != nil {
					b.Fatalf("ExecuteBlock failed: %v", err)
				}
				parallelTotal += time.Since(start)
			}
			parallelAvg := parallelTotal / time.Duration(iterations)
			
			// 计算加速比
			speedup := float64(sequentialAvg) / float64(parallelAvg)
			
			fmt.Printf("%-20s | %-8d | %-12v | %-12v | %-7.2fx\n",
				tc.name, worker, sequentialAvg, parallelAvg, speedup)
		}
		fmt.Printf("%-20s-+-%-8s-+-%-12s-+-%-12s-+-%-8s\n", "--------------------", "--------", "------------", "------------", "--------")
	}
	
	// 跳过默认的benchmark运行
	b.Skip("This is a custom speedup analysis, skipping default benchmark")
}

func runSequential(storage MultiStore, block *MockBlock) {
	for i, tx := range block.Txs {
		block.Results[i] = tx(storage)
	}
}