// Copyright 2024 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package pathdb

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"golang.org/x/sync/errgroup"
)

// storageKey returns a key for uniquely identifying the storage slot.
func storageKey(accountHash common.Hash, slotHash common.Hash) [64]byte {
	var key [64]byte
	copy(key[:32], accountHash[:])
	copy(key[32:], slotHash[:])
	return key
}

// lookup is an internal structure used to efficiently determine the layer in
// which a state entry resides.
type lookup struct {
	nodes map[common.Hash]map[string][]common.Hash

	// accounts represents the mutation history for specific accounts.
	// The key is the account address hash, and the value is a slice
	// of **diff layer** IDs indicating where the account was modified,
	// with the order from oldest to newest.
	accounts map[common.Hash][]common.Hash

	// storages represents the mutation history for specific storage
	// slot. The key is the account address hash and the storage key
	// hash, the value is a slice of **diff layer** IDs indicating
	// where the slot was modified, with the order from oldest to newest.
	storages map[[64]byte][]common.Hash

	// descendant is the callback indicating whether the layer with
	// given root is a descendant of the one specified by `ancestor`.
	descendant func(state common.Hash, ancestor common.Hash) bool
}

// newLookup initializes the lookup structure.
func newLookup(head layer, descendant func(state common.Hash, ancestor common.Hash) bool) *lookup {
	var (
		current = head
		layers  []layer
	)
	for current != nil {
		layers = append(layers, current)
		current = current.parentLayer()
	}
	l := &lookup{
		nodes:      make(map[common.Hash]map[string][]common.Hash),
		accounts:   make(map[common.Hash][]common.Hash),
		storages:   make(map[[64]byte][]common.Hash),
		descendant: descendant,
	}
	// Apply the diff layers from bottom to top
	for i := len(layers) - 1; i >= 0; i-- {
		switch diff := layers[i].(type) {
		case *diskLayer:
			continue
		case *diffLayer:
			l.addLayer(diff)
		}
	}
	return l
}

// accountTip traverses the layer list associated with the given account in
// reverse order to locate the first entry that either matches the specified
// stateID or is a descendant of it.
//
// If found, the account data corresponding to the supplied stateID resides
// in that layer. Otherwise, two scenarios are possible:
//
// (a) the account remains unmodified from the current disk layer up to the state
// layer specified by the stateID: fallback to the disk layer for data retrieval,
// (b) or the layer specified by the stateID is stale: reject the data retrieval.
func (l *lookup) accountTip(accountHash common.Hash, stateID common.Hash, base common.Hash) common.Hash {
	// Traverse the mutation history from latest to oldest one. Several
	// scenarios are possible:
	//
	// Chain:
	//     D->C1->C2->C3->C4 (HEAD)
	//      ->C1'->C2'->C3'
	// State:
	//     x: [C1, C1', C3', C3]
	//     y: []
	//
	// - (x, C4) => C3
	// - (x, C3) => C3
	// - (x, C2) => C1
	// - (x, C3') => C3'
	// - (x, C2') => C1'
	// - (y, C4) => D
	// - (y, C3') => D
	// - (y, C0) => null
	list := l.accounts[accountHash]
	for i := len(list) - 1; i >= 0; i-- {
		// If the current state matches the stateID, or the requested state is a
		// descendant of it, return the current state as the most recent one
		// containing the modified data. Otherwise, the current state may be ahead
		// of the requested one or belong to a different branch.
		if list[i] == stateID || l.descendant(stateID, list[i]) {
			return list[i]
		}
	}
	// No layer matching the stateID or its descendants was found. Use the
	// current disk layer as a fallback.
	if base == stateID || l.descendant(stateID, base) {
		return base
	}
	// The layer associated with 'stateID' is not the descendant of the current
	// disk layer, it's already stale, return nothing.
	return common.Hash{}
}

// storageTip traverses the layer list associated with the given account and
// slot hash in reverse order to locate the first entry that either matches
// the specified stateID or is a descendant of it.
//
// If found, the storage data corresponding to the supplied stateID resides
// in that layer. Otherwise, two scenarios are possible:
//
// (a) the storage slot remains unmodified from the current disk layer up to
// the state layer specified by the stateID: fallback to the disk layer for
// data retrieval, (b) or the layer specified by the stateID is stale: reject
// the data retrieval.
func (l *lookup) storageTip(accountHash common.Hash, slotHash common.Hash, stateID common.Hash, base common.Hash) common.Hash {
	list := l.storages[storageKey(accountHash, slotHash)]
	for i := len(list) - 1; i >= 0; i-- {
		// If the current state matches the stateID, or the requested state is a
		// descendant of it, return the current state as the most recent one
		// containing the modified data. Otherwise, the current state may be ahead
		// of the requested one or belong to a different branch.
		if list[i] == stateID || l.descendant(stateID, list[i]) {
			return list[i]
		}
	}
	// No layer matching the stateID or its descendants was found. Use the
	// current disk layer as a fallback.
	if base == stateID || l.descendant(stateID, base) {
		return base
	}
	// The layer associated with 'stateID' is not the descendant of the current
	// disk layer, it's already stale, return nothing.
	return common.Hash{}
}

func (l *lookup) nodeTip(accountHash common.Hash, path string, stateID common.Hash, base common.Hash) common.Hash {
	list := l.nodes[accountHash][path]
	for i := len(list) - 1; i >= 0; i-- {
		// If the current state matches the stateID, or the requested state is a
		// descendant of it, return the current state as the most recent one
		// containing the modified data. Otherwise, the current state may be ahead
		// of the requested one or belong to a different branch.
		if list[i] == stateID || l.descendant(stateID, list[i]) {
			return list[i]
		}
	}
	// No layer matching the stateID or its descendants was found. Use the
	// current disk layer as a fallback.
	if base == stateID || l.descendant(stateID, base) {
		return base
	}
	// The layer associated with 'stateID' is not the descendant of the current
	// disk layer, it's already stale, return nothing.
	return common.Hash{}
}

// addLayer traverses the state data retained in the specified diff layer and
// integrates it into the lookup set.
//
// This function assumes that all layers older than the provided one have already
// been processed, ensuring that layers are processed strictly in a bottom-to-top
// order.
func (l *lookup) addLayer(diff *diffLayer) {
	defer func(now time.Time) {
		lookupAddLayerTimer.UpdateSince(now)
	}(time.Now())

	var (
		wg    sync.WaitGroup
		st    = time.Now()
		state = diff.rootHash()
	)

	var accountTime, storageTime, trieTime time.Duration
	wg.Add(1)
	go func() {
		defer wg.Done()
		st := time.Now()
		for accountHash := range diff.states.accountData {
			list, exists := l.accounts[accountHash]
			if !exists {
				list = make([]common.Hash, 0, 16) // TODO(rjl493456442) use sync pool
			}
			list = append(list, state)
			l.accounts[accountHash] = list
		}
		accountTime = time.Since(st)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		st := time.Now()
		for accountHash, slots := range diff.states.storageData {
			for slotHash := range slots {
				key := storageKey(accountHash, slotHash)
				list, exists := l.storages[key]
				if !exists {
					list = make([]common.Hash, 0, 16) // TODO(rjl493456442) use sync pool
				}
				list = append(list, state)
				l.storages[key] = list
			}
		}
		storageTime = time.Since(st)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		st := time.Now()
		l.addNodes(state, diff.nodes.accountNodes, diff.nodes.storageNodes)
		trieTime = time.Since(st)
	}()
	wg.Wait()

	storages1 := 0
	for _, slots := range diff.states.storageData {
		storages1 += len(slots)
	}
	storages2 := 0
	for _, slots := range diff.nodes.storageNodes {
		storages2 += len(slots)
	}
	log.Info("PathDB lookup add layer", "id", diff.id, "block", diff.block, "accountTime", accountTime, "storageTime", storageTime, "trieTime", trieTime, "elapsed", time.Since(st),
		"accounts", len(diff.states.accountData), "storages-keys", len(diff.states.storageData), "storages-slots", storages1,
		"accounts-trie", len(diff.nodes.accountNodes), "storages-trie-keys", len(diff.nodes.storageNodes), "storages-trie-slots", storages2,
	)
}

func (l *lookup) addNodes(state common.Hash, accountNodes map[string]*trienode.Node, storageNodes map[common.Hash]map[string]*trienode.Node) {
	// Calculate total work to determine optimal worker count
	totalWork := len(accountNodes)
	for _, subset := range storageNodes {
		totalWork += len(subset)
	}

	// Use more workers for larger workloads, but cap to avoid overhead
	workers := runtime.NumCPU() / 2
	if workers > totalWork {
		workers = totalWork
	}
	if workers < 1 {
		workers = 1
	}

	// For small workloads, process directly without goroutines
	if totalWork < 100 || workers == 1 {
		l.addNodesSequential(state, accountNodes, storageNodes)
		return
	}

	// Split work into equal-sized batches for better load balancing
	type workChunk struct {
		hash  common.Hash
		nodes map[string]*trienode.Node
	}

	// Calculate batch size to distribute work evenly
	batchSize := (totalWork + workers - 1) / workers

	var (
		mu        sync.Mutex
		wg        sync.WaitGroup
		chunkChan = make(chan workChunk, workers)
	)
	log.Info("PathDB lookup add nodes", "workers", workers, "batchSize", batchSize, "totalWork", totalWork)

	// Start workers first
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range chunkChan {
				l.processNodeChunk(state, chunk.hash, chunk.nodes, &mu)
			}
		}()
	}

	// Split into batches and send directly to channel
	currentBatch := make(map[string]*trienode.Node, batchSize)
	currentBatchSize := 0
	currentAccountHash := common.Hash{}

	// Process account nodes first
	for path, node := range accountNodes {
		if currentBatchSize >= batchSize {
			// Flush current batch
			if len(currentBatch) > 0 {
				chunkChan <- workChunk{currentAccountHash, currentBatch}
			}
			// Start new batch
			currentBatch = make(map[string]*trienode.Node, batchSize)
			currentBatchSize = 0
			currentAccountHash = common.Hash{}
		}
		currentBatch[path] = node
		currentBatchSize++
	}

	// Process storage nodes - keep nodes from same account together
	for accountHash, subset := range storageNodes {
		// Start a new batch for each account to avoid mixing
		if len(currentBatch) > 0 {
			chunkChan <- workChunk{currentAccountHash, currentBatch}
			currentBatch = make(map[string]*trienode.Node)
			currentBatchSize = 0
		}
		currentAccountHash = accountHash

		for path, node := range subset {
			if currentBatchSize >= batchSize {
				// Flush current batch
				if len(currentBatch) > 0 {
					chunkChan <- workChunk{currentAccountHash, currentBatch}
				}
				// Start new batch
				currentBatch = make(map[string]*trienode.Node, batchSize)
				currentBatchSize = 0
			}
			currentBatch[path] = node
			currentBatchSize++
		}
	}

	// Add the last batch if it has content
	if len(currentBatch) > 0 {
		chunkChan <- workChunk{currentAccountHash, currentBatch}
	}

	close(chunkChan)
	wg.Wait()
}

// addNodesSequential processes nodes without goroutines for small workloads
func (l *lookup) addNodesSequential(state common.Hash, accountNodes map[string]*trienode.Node, storageNodes map[common.Hash]map[string]*trienode.Node) {
	if len(accountNodes) > 0 {
		l.processNodeChunk(state, common.Hash{}, accountNodes, nil)
	}

	if len(storageNodes) > 0 {
		for accountHash, subset := range storageNodes {
			if len(subset) > 0 {
				l.processNodeChunk(state, accountHash, subset, nil)
			}
		}
	}
}

// processNodeChunk processes a chunk of nodes for a specific account
func (l *lookup) processNodeChunk(state common.Hash, accountHash common.Hash, nodes map[string]*trienode.Node, lock *sync.Mutex) {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	}

	store := l.nodes[accountHash]
	if store == nil {
		store = make(map[string][]common.Hash, len(nodes))
		l.nodes[accountHash] = store
	}

	// Process nodes with optimized memory allocation
	for path := range nodes {
		if _, exists := store[path]; !exists {
			store[path] = make([]common.Hash, 0, 16)
		}
		store[path] = append(store[path], state)
	}
}

// removeFromList removes the specified element from the provided list.
// It returns a flag indicating whether the element was found and removed.
func removeFromList(list []common.Hash, element common.Hash) (bool, []common.Hash) {
	// Traverse the list from oldest to newest to quickly locate the element.
	for i := 0; i < len(list); i++ {
		if list[i] == element {
			if i != 0 {
				list = append(list[:i], list[i+1:]...)
			} else {
				// Remove the first element by shifting the slice forward.
				// Pros: zero-copy.
				// Cons: may retain large backing array, causing memory leaks.
				// Mitigation: release the array if capacity exceeds threshold.
				list = list[1:]
				if cap(list) > 1024 {
					list = append(make([]common.Hash, 0, len(list)), list...)
				}
			}
			return true, list
		}
	}
	return false, nil
}

// removeLayer traverses the state data retained in the specified diff layer and
// unlink them from the lookup set.
func (l *lookup) removeLayer(diff *diffLayer) error {
	defer func(now time.Time) {
		lookupRemoveLayerTimer.UpdateSince(now)
	}(time.Now())

	var (
		eg    errgroup.Group
		state = diff.rootHash()
	)
	eg.Go(func() error {
		for accountHash := range diff.states.accountData {
			found, list := removeFromList(l.accounts[accountHash], state)
			if !found {
				return fmt.Errorf("account lookup is not found, %x, state: %x", accountHash, state)
			}
			if len(list) != 0 {
				l.accounts[accountHash] = list
			} else {
				delete(l.accounts, accountHash)
			}
		}
		return nil
	})

	eg.Go(func() error {
		for accountHash, slots := range diff.states.storageData {
			for slotHash := range slots {
				key := storageKey(accountHash, slotHash)
				found, list := removeFromList(l.storages[key], state)
				if !found {
					return fmt.Errorf("storage lookup is not found, %x %x, state: %x", accountHash, slotHash, state)
				}
				if len(list) != 0 {
					l.storages[key] = list
				} else {
					delete(l.storages, key)
				}
			}
		}
		return nil
	})

	eg.Go(func() error {
		accountHash := common.Hash{}
		for path := range diff.nodes.accountNodes {
			found, list := removeFromList(l.nodes[accountHash][path], state)
			if !found {
				return fmt.Errorf("account lookup is not found, %x, state: %x", accountHash, state)
			}
			if len(list) != 0 {
				l.nodes[accountHash][path] = list
			} else {
				delete(l.nodes[accountHash], path)
			}
		}
		return nil
	})
	eg.Go(func() error {
		for accountHash := range diff.nodes.storageNodes {
			for path := range diff.nodes.accountNodes {
				found, list := removeFromList(l.nodes[accountHash][path], state)
				if !found {
					return fmt.Errorf("account lookup is not found, %x, state: %x", accountHash, state)
				}
				if len(list) != 0 {
					l.nodes[accountHash][path] = list
				} else {
					delete(l.nodes[accountHash], path)
				}
			}
		}
		return nil
	})
	return eg.Wait()
}
