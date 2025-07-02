package pathdb

import (
	"sort"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

const pruneBatchSize = 1_000_000

type historyPruner struct {
	db      ethdb.KeyValueStore
	taskCh  chan uint64
	stopCh  chan struct{}
	running atomic.Bool
}

func newHistoryPruner(db ethdb.KeyValueStore) *historyPruner {
	hp := &historyPruner{
		db:     db,
		taskCh: make(chan uint64, 1),
		stopCh: make(chan struct{}),
	}
	go hp.run()

	return hp
}

func (hp *historyPruner) close() {
	close(hp.stopCh)
}

// SendPruneTask queues a prune request. If a prune is already running, the new task is ignored.
func (hp *historyPruner) SendPruneTask(oldestHistoryID uint64) {
	if hp.running.Load() {
		log.Info("Pruner busy, ignoring new prune task", "oldestHistoryID", oldestHistoryID)
		return
	}
	select {
	case hp.taskCh <- oldestHistoryID:
		log.Info("Prune task queued", "oldestHistoryID", oldestHistoryID)
	default:
		log.Info("Prune task already queued, ignoring", "oldestHistoryID", oldestHistoryID)
	}
}

func (hp *historyPruner) run() {
	for {
		select {
		case oldestHistoryID := <-hp.taskCh:
			hp.running.Store(true)
			hp.pruneBatch(oldestHistoryID)
			hp.running.Store(false)
		case <-hp.stopCh:
			log.Info("StateHistoryPruner stopped")
			return
		}
	}
}

func (hp *historyPruner) pruneBatch(oldestHistoryID uint64) {
	log.Info("StateHistoryPruner: pruning batch", "oldestHistoryID", oldestHistoryID)
	start := time.Now()
	batch := hp.db.NewBatch()
	prunedAccounts := 0
	prunedStorages := 0

	// Prune up to pruneBatchSize account indices
	it := hp.db.NewIterator(rawdb.StateHistoryAccountMetadataPrefix, nil)
	defer it.Release()
	for it.Next() {
		if prunedAccounts >= pruneBatchSize {
			break
		}
		select {
		case <-hp.stopCh:
			log.Info("StateHistoryPruner interrupted during account pruning")
			return
		default:
		}
		key := it.Key()
		if len(key) != len(rawdb.StateHistoryAccountMetadataPrefix)+common.HashLength {
			continue
		}
		addrHash := common.BytesToHash(key[len(rawdb.StateHistoryAccountMetadataPrefix):])
		blob := it.Value()
		descList, err := parseIndex(blob)
		if err != nil || len(descList) == 0 {
			continue
		}
		idx := sort.Search(len(descList), func(i int) bool {
			return descList[i].max >= oldestHistoryID
		})
		if idx == 0 {
			continue
		}
		pruned := descList[:idx]
		remain := descList[idx:]
		for _, desc := range pruned {
			rawdb.DeleteAccountHistoryIndexBlock(batch, addrHash, desc.id)
		}
		if len(remain) == 0 {
			rawdb.DeleteAccountHistoryIndex(batch, addrHash)
		} else {
			buf := make([]byte, 0, indexBlockDescSize*len(remain))
			for _, desc := range remain {
				buf = append(buf, desc.encode()...)
			}
			rawdb.WriteAccountHistoryIndex(batch, addrHash, buf)
		}
		prunedAccounts++
	}

	// Prune up to pruneBatchSize storage indices
	it2 := hp.db.NewIterator(rawdb.StateHistoryStorageMetadataPrefix, nil)
	defer it2.Release()
	for it2.Next() {
		if prunedStorages >= pruneBatchSize {
			break
		}
		select {
		case <-hp.stopCh:
			log.Info("StateHistoryPruner interrupted during storage pruning")
			return
		default:
		}
		key := it2.Key()
		if len(key) != len(rawdb.StateHistoryStorageMetadataPrefix)+common.HashLength*2 {
			continue
		}
		addrHash := common.BytesToHash(key[len(rawdb.StateHistoryStorageMetadataPrefix) : len(rawdb.StateHistoryStorageMetadataPrefix)+common.HashLength])
		storageHash := common.BytesToHash(key[len(rawdb.StateHistoryStorageMetadataPrefix)+common.HashLength:])
		blob := it2.Value()
		descList, err := parseIndex(blob)
		if err != nil || len(descList) == 0 {
			continue
		}
		idx := sort.Search(len(descList), func(i int) bool {
			return descList[i].max >= oldestHistoryID
		})
		if idx == 0 {
			continue
		}
		pruned := descList[:idx]
		remain := descList[idx:]
		for _, desc := range pruned {
			rawdb.DeleteStorageHistoryIndexBlock(batch, addrHash, storageHash, desc.id)
		}
		if len(remain) == 0 {
			rawdb.DeleteStorageHistoryIndex(batch, addrHash, storageHash)
		} else {
			buf := make([]byte, 0, indexBlockDescSize*len(remain))
			for _, desc := range remain {
				buf = append(buf, desc.encode()...)
			}
			rawdb.WriteStorageHistoryIndex(batch, addrHash, storageHash, buf)
		}
		prunedStorages++
	}

	if err := batch.Write(); err != nil {
		log.Error("StateHistoryPruner: failed to prune batch", "err", err)
	} else {
		log.Info("StateHistoryPruner: pruned batch", "accounts", prunedAccounts, "storages", prunedStorages, "elapsed", time.Since(start))
	}
}
