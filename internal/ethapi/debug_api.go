package ethapi

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/davecgh/go-spew/spew"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"
)

// DebugAPI is the collection of Ethereum APIs exposed over the debugging
// namespace.
type DebugAPI struct {
	b Backend
}

// NewDebugAPI creates a new instance of DebugAPI.
func NewDebugAPI(b Backend) *DebugAPI {
	return &DebugAPI{b: b}
}

// GetRawHeader retrieves the RLP encoding for a single header.
func (api *DebugAPI) GetRawHeader(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (hexutil.Bytes, error) {
	var hash common.Hash
	if h, ok := blockNrOrHash.Hash(); ok {
		hash = h
	} else {
		block, err := api.b.BlockByNumberOrHash(ctx, blockNrOrHash)
		if err != nil {
			return nil, err
		}
		hash = block.Hash()
	}
	header, _ := api.b.HeaderByHash(ctx, hash)
	if header == nil {
		return nil, fmt.Errorf("header #%d not found", hash)
	}
	return rlp.EncodeToBytes(header)
}

// GetRawBlock retrieves the RLP encoded for a single block.
func (api *DebugAPI) GetRawBlock(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (hexutil.Bytes, error) {
	var hash common.Hash
	if h, ok := blockNrOrHash.Hash(); ok {
		hash = h
	} else {
		block, err := api.b.BlockByNumberOrHash(ctx, blockNrOrHash)
		if err != nil {
			return nil, err
		}
		hash = block.Hash()
	}
	block, _ := api.b.BlockByHash(ctx, hash)
	if block == nil {
		return nil, fmt.Errorf("block #%d not found", hash)
	}
	return rlp.EncodeToBytes(block)
}

// GetRawReceipts retrieves the binary-encoded receipts of a single block.
func (api *DebugAPI) GetRawReceipts(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) ([]hexutil.Bytes, error) {
	var hash common.Hash
	if h, ok := blockNrOrHash.Hash(); ok {
		hash = h
	} else {
		block, err := api.b.BlockByNumberOrHash(ctx, blockNrOrHash)
		if err != nil {
			return nil, err
		}
		hash = block.Hash()
	}
	receipts, err := api.b.GetReceipts(ctx, hash)
	if err != nil {
		return nil, err
	}
	result := make([]hexutil.Bytes, len(receipts))
	for i, receipt := range receipts {
		b, err := receipt.MarshalBinary()
		if err != nil {
			return nil, err
		}
		result[i] = b
	}
	return result, nil
}

// GetRawTransaction returns the bytes of the transaction for the given hash.
func (s *DebugAPI) GetRawTransaction(ctx context.Context, hash common.Hash) (hexutil.Bytes, error) {
	// Retrieve a finalized transaction, or a pooled otherwise
	tx, _, _, _, err := s.b.GetTransaction(ctx, hash)
	if err != nil {
		return nil, err
	}
	if tx == nil {
		if tx = s.b.GetPoolTransaction(hash); tx == nil {
			// Transaction not found anywhere, abort
			return nil, nil
		}
	}
	return tx.MarshalBinary()
}

// PrintBlock retrieves a block and returns its pretty printed form.
func (api *DebugAPI) PrintBlock(ctx context.Context, number uint64) (string, error) {
	block, _ := api.b.BlockByNumber(ctx, rpc.BlockNumber(number))
	if block == nil {
		return "", fmt.Errorf("block #%d not found", number)
	}
	return spew.Sdump(block), nil
}

// ChaindbProperty returns leveldb properties of the key-value database.
func (api *DebugAPI) ChaindbProperty(property string) (string, error) {
	if property == "" {
		property = "leveldb.stats"
	} else if !strings.HasPrefix(property, "leveldb.") {
		property = "leveldb." + property
	}
	return api.b.ChainDb().Stat(property)
}

// ChaindbCompact flattens the entire key-value database into a single level,
// removing all unused slots and merging all keys.
func (api *DebugAPI) ChaindbCompact() error {
	for b := byte(0); b < 255; b++ {
		log.Info("Compacting chain database", "range", fmt.Sprintf("0x%0.2X-0x%0.2X", b, b+1))
		if err := api.b.ChainDb().Compact([]byte{b}, []byte{b + 1}); err != nil {
			log.Error("Database compaction failed", "err", err)
			return err
		}
	}
	return nil
}

// SetHead rewinds the head of the blockchain to a previous block.
func (api *DebugAPI) SetHead(number hexutil.Uint64) {
	api.b.SetHead(uint64(number))
}

// AccountRangeMaxResults is the maximum number of results to be returned per call
const AccountRangeMaxResults = 256

// DumpBlock retrieves the entire state of the database at a given block.
func (api *DebugAPI) DumpBlock(ctx context.Context, blockNr rpc.BlockNumber) (state.Dump, error) {
	opts := &state.DumpConfig{
		OnlyWithAddresses: true,
		Max:               AccountRangeMaxResults, // Sanity limit over RPC
	}
	stateDb, header, err := api.b.StateAndHeaderByNumber(ctx, blockNr)
	if err != nil {
		return state.Dump{}, err
	}
	if header == nil {
		return state.Dump{}, fmt.Errorf("block #%d not found", blockNr)
	}
	return stateDb.RawDump(opts), nil
}

// Preimage is a debug API function that returns the preimage for a sha3 hash, if known.
func (api *DebugAPI) Preimage(ctx context.Context, hash common.Hash) (hexutil.Bytes, error) {
	if preimage := rawdb.ReadPreimage(api.b.ChainDb(), hash); preimage != nil {
		return preimage, nil
	}
	return nil, errors.New("unknown preimage")
}
