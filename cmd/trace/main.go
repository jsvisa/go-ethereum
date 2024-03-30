package main

import (
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"slices"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/eth/tracers"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rpc"

	// Force-load the tracer engines to trigger registration
	_ "github.com/ethereum/go-ethereum/eth/tracers/js"
	_ "github.com/ethereum/go-ethereum/eth/tracers/native"
)

var (
	errStateNotFound = errors.New("state not found")
	errBlockNotFound = errors.New("block not found")
)

type testBackend struct {
	chainConfig *params.ChainConfig
	engine      consensus.Engine
	chaindb     ethdb.Database
	chain       *core.BlockChain

	refHook func() // Hook is invoked when the requested state is referenced
	relHook func() // Hook is invoked when the requested state is released
}

// newTestBackend creates a new test backend. OBS: After test is done, teardown must be
// invoked in order to release associated resources.
func newTestBackend(n int, gspec *core.Genesis, generator func(i int, b *core.BlockGen)) *testBackend {
	backend := &testBackend{
		chainConfig: gspec.Config,
		engine:      ethash.NewFaker(),
		chaindb:     rawdb.NewMemoryDatabase(),
	}
	// Generate blocks for testing
	_, blocks, _ := core.GenerateChainWithGenesis(gspec, backend.engine, n, generator)

	// Import the canonical chain
	cacheConfig := &core.CacheConfig{
		TrieCleanLimit:    256,
		TrieDirtyLimit:    256,
		TrieTimeLimit:     5 * time.Minute,
		SnapshotLimit:     0,
		TrieDirtyDisabled: true, // Archive mode
	}
	chain, err := core.NewBlockChain(backend.chaindb, cacheConfig, gspec, nil, backend.engine, vm.Config{}, nil, nil)
	if err != nil {
		log.Crit("failed to create tester chain: %v", err)
	}
	if n, err := chain.InsertChain(blocks); err != nil {
		log.Crit("block %d: failed to insert into chain: %v", n, err)
	}
	backend.chain = chain
	return backend
}

func (b *testBackend) HeaderByHash(ctx context.Context, hash common.Hash) (*types.Header, error) {
	return b.chain.GetHeaderByHash(hash), nil
}

func (b *testBackend) HeaderByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Header, error) {
	if number == rpc.PendingBlockNumber || number == rpc.LatestBlockNumber {
		return b.chain.CurrentHeader(), nil
	}
	return b.chain.GetHeaderByNumber(uint64(number)), nil
}

func (b *testBackend) BlockByHash(ctx context.Context, hash common.Hash) (*types.Block, error) {
	return b.chain.GetBlockByHash(hash), nil
}

func (b *testBackend) BlockByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Block, error) {
	if number == rpc.PendingBlockNumber || number == rpc.LatestBlockNumber {
		return b.chain.GetBlockByNumber(b.chain.CurrentBlock().Number.Uint64()), nil
	}
	return b.chain.GetBlockByNumber(uint64(number)), nil
}

func (b *testBackend) GetTransaction(ctx context.Context, txHash common.Hash) (bool, *types.Transaction, common.Hash, uint64, uint64, error) {
	tx, hash, blockNumber, index := rawdb.ReadTransaction(b.chaindb, txHash)
	return tx != nil, tx, hash, blockNumber, index, nil
}

func (b *testBackend) RPCGasCap() uint64 {
	return 25000000
}

func (b *testBackend) ChainConfig() *params.ChainConfig {
	return b.chainConfig
}

func (b *testBackend) Engine() consensus.Engine {
	return b.engine
}

func (b *testBackend) ChainDb() ethdb.Database {
	return b.chaindb
}

// teardown releases the associated resources.
func (b *testBackend) teardown() {
	b.chain.Stop()
}

func (b *testBackend) StateAtBlock(ctx context.Context, block *types.Block, reexec uint64, base *state.StateDB, readOnly bool, preferDisk bool) (*state.StateDB, tracers.StateReleaseFunc, error) {
	statedb, err := b.chain.StateAt(block.Root())
	if err != nil {
		return nil, nil, errStateNotFound
	}
	if b.refHook != nil {
		b.refHook()
	}
	release := func() {
		if b.relHook != nil {
			b.relHook()
		}
	}
	return statedb, release, nil
}

func (b *testBackend) StateAtTransaction(ctx context.Context, block *types.Block, txIndex int, reexec uint64) (*types.Transaction, vm.BlockContext, *state.StateDB, tracers.StateReleaseFunc, error) {
	parent := b.chain.GetBlock(block.ParentHash(), block.NumberU64()-1)
	if parent == nil {
		return nil, vm.BlockContext{}, nil, nil, errBlockNotFound
	}
	statedb, release, err := b.StateAtBlock(ctx, parent, reexec, nil, true, false)
	if err != nil {
		return nil, vm.BlockContext{}, nil, nil, errStateNotFound
	}
	if txIndex == 0 && len(block.Transactions()) == 0 {
		return nil, vm.BlockContext{}, statedb, release, nil
	}
	// Recompute transactions up to the target index.
	signer := types.MakeSigner(b.chainConfig, block.Number(), block.Time())
	for idx, tx := range block.Transactions() {
		msg, _ := core.TransactionToMessage(tx, signer, block.BaseFee())
		txContext := core.NewEVMTxContext(msg)
		context := core.NewEVMBlockContext(block.Header(), b.chain, nil)
		if idx == txIndex {
			return tx, context, statedb, release, nil
		}
		vmenv := vm.NewEVM(context, txContext, statedb, b.chainConfig, vm.Config{})
		if _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(tx.Gas())); err != nil {
			return nil, vm.BlockContext{}, nil, nil, fmt.Errorf("transaction %#x failed: %v", tx.Hash(), err)
		}
		statedb.Finalise(vmenv.ChainConfig().IsEIP158(block.Number()))
	}
	return nil, vm.BlockContext{}, nil, nil, fmt.Errorf("transaction index %d out of range for block %#x", txIndex, block.Hash())
}

type Account struct {
	key  *ecdsa.PrivateKey
	addr common.Address
}

func newAccounts(n int) (accounts []Account) {
	for i := 0; i < n; i++ {
		key, _ := crypto.GenerateKey()
		addr := crypto.PubkeyToAddress(key.PublicKey)
		accounts = append(accounts, Account{key: key, addr: addr})
	}
	slices.SortFunc(accounts, func(a, b Account) int { return a.addr.Cmp(b.addr) })
	return accounts
}

func newRPCBalance(balance *big.Int) **hexutil.Big {
	rpcBalance := (*hexutil.Big)(balance)
	return &rpcBalance
}

func newRPCBytes(bytes []byte) *hexutil.Bytes {
	rpcBytes := hexutil.Bytes(bytes)
	return &rpcBytes
}

func newStates(keys []common.Hash, vals []common.Hash) *map[common.Hash]common.Hash {
	if len(keys) != len(vals) {
		panic("invalid input")
	}
	m := make(map[common.Hash]common.Hash)
	for i := 0; i < len(keys); i++ {
		m[keys[i]] = vals[i]
	}
	return &m
}

func main() {
	log.SetDefault(log.NewLogger(log.NewTerminalHandlerWithLevel(os.Stderr, log.LevelInfo, true)))

	// Initialize test accounts
	accounts := newAccounts(4)
	genesis := &core.Genesis{
		Config:   params.TestChainConfig,
		Coinbase: accounts[3].addr,
		Alloc: types.GenesisAlloc{
			accounts[0].addr: {Balance: big.NewInt(params.Ether)},
			accounts[1].addr: {Balance: big.NewInt(params.Ether)},
			accounts[2].addr: {Balance: big.NewInt(params.Ether)},
		},
	}
	genBlocks := 1
	signer := types.NewLondonSigner(genesis.Config.ChainID)
	// signer := types.HomesteadSigner{}
	// var txHashes []common.Hash
	// txFee := big.NewInt(0)
	backend := newTestBackend(genBlocks, genesis, func(i int, b *core.BlockGen) {
		b.SetCoinbase(accounts[3].addr)
		// Transfer from account[0] to account[1]
		//    value: 1000 wei
		//    fee:   0 wei
		feeCap := b.BaseFee()
		feeCap.Add(feeCap, big.NewInt(10*1_000_000_000))
		feeTip := big.NewInt(5 * 1_000_000_000)
		for j := 0; j < 127; j++ {
			tx, _ := types.SignTx(types.NewTx(&types.DynamicFeeTx{
				ChainID:   genesis.Config.ChainID,
				Nonce:     uint64(i + j),
				To:        &accounts[1].addr,
				Value:     big.NewInt(1000),
				Gas:       params.TxGas,
				GasFeeCap: feeCap,
				GasTipCap: feeTip,
				Data:      nil}),
				signer, accounts[0].key)
			b.AddTx(tx)
			// txHashes = append(txHashes, tx.Hash())
			// txFee.Add(txFee, new(big.Int).Mul(feeTip, big.NewInt(int64(params.TxGas))))
		}
		// transfer all tx fees from the miner to account[2]
		value := b.GetBalance(accounts[3].addr).ToBig()
		value.Sub(value, new(big.Int).Mul(feeTip, big.NewInt(int64(params.TxGas))))
		tx, _ := types.SignTx(types.NewTx(&types.DynamicFeeTx{
			Nonce:     uint64(0),
			To:        &accounts[2].addr,
			Value:     value,
			Gas:       params.TxGas,
			GasFeeCap: b.BaseFee(),
			Data:      nil}),
			signer, accounts[3].key)
		b.AddTx(tx)
	})
	defer backend.chain.Stop()
	api := tracers.NewAPI(backend)

	var (
		callTracer     = "callTracer"
		prestateTracer = "prestateTracer"
	)
	var testSuite = []struct {
		blockNumber rpc.BlockNumber
		config      *tracers.TraceConfig
		want        string
		expectErr   error
	}{
		// callTracer
		{
			blockNumber: rpc.BlockNumber(genBlocks),
			config:      &tracers.TraceConfig{Tracer: &callTracer},
			// want:        fmt.Sprintf(`[{"txHash":"%v","result":{"gas":21000,"failed":false,"returnValue":"","structLogs":[]}}]`, txHash),
			want: `[]`,
		},
		// prestateTracer
		{
			blockNumber: rpc.BlockNumber(genBlocks),
			config:      &tracers.TraceConfig{Tracer: &prestateTracer},
			// want:        fmt.Sprintf(`[{"txHash":"%v","result":{"gas":21000,"failed":false,"returnValue":"","structLogs":[]}}]`, txHash),
			want: `[]`,
		},
	}
	for i, tc := range testSuite {
		result, err := api.TraceBlockByNumber(context.Background(), tc.blockNumber, tc.config)
		if tc.expectErr != nil {
			if err == nil {
				log.Crit("test failed", "idx", i, "want", tc.expectErr, "have", nil)
				continue
			}
			if !errors.Is(err, tc.expectErr) {
				log.Crit("test failed error mismatch", "idx", i, "want", tc.expectErr, "have", err)
			}
			continue
		}
		if err != nil {
			log.Crit("test failed", "idx", i, "err", err)
			continue
		}
		have, _ := json.Marshal(result)
		want := tc.want
		if string(have) != want {
			log.Error("test failed", "idx", i, "want", want, "have", string(have))
		}
	}

}
