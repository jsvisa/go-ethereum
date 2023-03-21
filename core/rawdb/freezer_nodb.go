package rawdb

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/gofrs/flock"
)

// nodbFreezer is a freezer without any ancient data, only record 'frozen' , the next recycle block number form kvstore.
type nodbFreezer struct {
	db ethdb.KeyValueStore // Meta database
	// WARNING: The `frozen` field is accessed atomically. On 32 bit platforms, only
	// 64-bit aligned fields can be atomic. The struct is guaranteed to be so aligned,
	// so take advantage of that (https://golang.org/pkg/sync/atomic/#pkg-note-BUG).
	frozen    uint64 // BlockNumber of next frozen block
	tail      uint64 // Number of the first stored item in the freezer
	threshold uint64 // Number of recent blocks not to freeze (params.FullImmutabilityThreshold apart from tests)

	instanceLock *flock.Flock // File-system lock to prevent double opens
	quit         chan struct{}
	closeOnce    sync.Once
}

// newNodbFreezer creates a chain freezer that deletes data enough 'old'.
func newNodbFreezer(datadir string, db ethdb.KeyValueStore, offset uint64) (*nodbFreezer, error) {
	if info, err := os.Lstat(datadir); !os.IsNotExist(err) {
		if info.Mode()&os.ModeSymlink != 0 {
			log.Warn("Symbolic link ancient database is not supported", "path", datadir)
			return nil, errSymlinkDatadir
		}
	}

	flockFile := filepath.Join(datadir, "../FREEZER_NODB_FLOCK")
	if err := os.MkdirAll(filepath.Dir(flockFile), 0755); err != nil {
		return nil, err
	}
	// Leveldb uses LOCK as the filelock filename. To prevent the
	// name collision, we use FLOCK as the lock name.
	lock := flock.New(flockFile)
	if locked, err := lock.TryLock(); err != nil {
		return nil, err
	} else if !locked {
		return nil, errors.New("locking failed")
	}

	freezer := &nodbFreezer{
		db:           db,
		frozen:       offset,
		threshold:    params.FullImmutabilityThreshold,
		instanceLock: lock,
		quit:         make(chan struct{}),
	}

	// delete ancient dir
	if err := os.RemoveAll(datadir); err != nil && !os.IsNotExist(err) {
		log.Warn("remove the ancient dir failed.", "path", datadir, "error", err)
		return nil, err
	}
	log.Info("Opened ancientdb with nodata mode", "database", datadir, "frozen", freezer.frozen)
	return freezer, nil
}

// Close terminates the chain prunedfreezer.
func (f *nodbFreezer) Close() error {
	var err error
	f.closeOnce.Do(func() {
		close(f.quit)
		f.Sync()
		err = f.instanceLock.Unlock()
	})
	return err
}

// HasAncient returns an indicator whether the specified ancient data exists, return nil.
func (f *nodbFreezer) HasAncient(kind string, number uint64) (bool, error) {
	return false, nil
}

// Ancient retrieves an ancient binary blob from prunedfreezer, return nil.
func (f *nodbFreezer) Ancient(kind string, number uint64) ([]byte, error) {
	if _, ok := chainFreezerNoSnappy[kind]; ok {
		if number >= atomic.LoadUint64(&f.frozen) {
			return nil, errOutOfBounds
		}
		return nil, nil
	}
	return nil, errUnknownTable
}

// Ancients returns the last of the frozen items.
func (f *nodbFreezer) Ancients() (uint64, error) {
	return atomic.LoadUint64(&f.frozen), nil
}

// Tail returns the number of first stored item in the freezer.
func (f *nodbFreezer) Tail() (uint64, error) {
	return atomic.LoadUint64(&f.tail), nil
}

// AncientSize returns the ancient size of the specified category, return 0.
func (f *nodbFreezer) AncientSize(kind string) (uint64, error) {
	if _, ok := chainFreezerNoSnappy[kind]; ok {
		return 0, nil
	}
	return 0, errUnknownTable
}

// AppendAncient update frozen.
//
// Notably, this function is lock free but kind of thread-safe. All out-of-order
// injection will be rejected. But if two injections with same number happen at
// the same time, we can get into the trouble.
func (f *nodbFreezer) AppendAncient(number uint64, hash, header, body, receipts, td []byte) (err error) {
	if atomic.LoadUint64(&f.frozen) != number {
		return errOutOrderInsertion
	}
	atomic.AddUint64(&f.frozen, 1)
	return nil
}

// TruncateHead discards any recent data above the provided threshold number.
func (f *nodbFreezer) TruncateHead(items uint64) error {
	if atomic.LoadUint64(&f.frozen) <= items {
		return nil
	}
	atomic.StoreUint64(&f.frozen, items)
	WriteFrozenOfAncientFreezer(f.db, atomic.LoadUint64(&f.frozen))
	return nil
}

// TruncateTail discards any recent data above the provided threshold number, always success.
func (f *nodbFreezer) TruncateTail(tail uint64) error {
	if atomic.LoadUint64(&f.tail) >= tail {
		return nil
	}
	atomic.StoreUint64(&f.tail, tail)
	return nil
}

// Sync flushes meta data tables to disk.
func (f *nodbFreezer) Sync() error {
	WriteFrozenOfAncientFreezer(f.db, atomic.LoadUint64(&f.frozen))
	return nil
}

// freeze is a background thread that periodically checks the blockchain for any
// import progress and moves ancient data from the fast database into the freezer.
//
// This functionality is deliberately broken off from block importing to avoid
// incurring additional data shuffling delays on block propagation.
func (f *nodbFreezer) freeze() {
	nfdb := &nofreezedb{KeyValueStore: f.db}

	var backoff bool
	for {
		select {
		case <-f.quit:
			log.Info("Freezer shutting down")
			return
		default:
		}
		if backoff {
			select {
			case <-time.NewTimer(freezerRecheckInterval).C:
			case <-f.quit:
				return
			}
		}

		// Retrieve the freezing threshold.
		hash := ReadHeadBlockHash(nfdb)
		if hash == (common.Hash{}) {
			log.Debug("Current full block hash unavailable") // new chain, empty database
			backoff = true
			continue
		}
		number := ReadHeaderNumber(nfdb, hash)
		threshold := atomic.LoadUint64(&f.threshold)

		switch {
		case number == nil:
			log.Error("Current full block number unavailable", "hash", hash)
			backoff = true
			continue

		case *number < threshold:
			log.Debug("Current full block not old enough", "number", *number, "hash", hash, "delay", threshold)
			backoff = true
			continue

		case *number-threshold <= f.frozen:
			log.Debug("Ancient blocks frozen already", "number", *number, "hash", hash, "frozen", f.frozen)
			backoff = true
			continue
		}
		head := ReadHeader(nfdb, hash, *number)
		if head == nil {
			log.Error("Stable state block unavailable", "number", *number, "hash", hash)
			backoff = true
			continue
		}

		// Seems we have data ready to be frozen, process in usable batches
		limit := *number - threshold
		// keep more data
		limit -= 1024

		if limit < f.frozen {
			log.Debug("Stable state block has prune", "limit", limit, "frozen", f.frozen)
			backoff = true
			continue
		}

		if limit-f.frozen > freezerBatchLimit {
			limit = f.frozen + freezerBatchLimit
		}
		var (
			start    = time.Now()
			first    = f.frozen
			ancients = make([]common.Hash, 0, limit-f.frozen)
		)
		for f.frozen <= limit {
			// Retrieves all the components of the canonical block
			hash := ReadCanonicalHash(nfdb, f.frozen)
			if hash == (common.Hash{}) {
				log.Error("Canonical hash missing, can't freeze", "number", f.frozen)
			}
			log.Trace("Deep froze ancient block", "number", f.frozen, "hash", hash)
			// Inject all the components into the relevant data tables
			if err := f.AppendAncient(f.frozen, nil, nil, nil, nil, nil); err != nil {
				log.Error("Append ancient err", "number", f.frozen, "hash", hash, "err", err)
				break
			}
			if hash != (common.Hash{}) {
				ancients = append(ancients, hash)
			}
		}
		// Batch of blocks have been frozen, flush them before wiping from leveldb
		if err := f.Sync(); err != nil {
			log.Crit("Failed to flush frozen tables", "err", err)
		}
		backoff = f.frozen-first >= freezerBatchLimit
		gcKvStore(f.db, ancients, first, f.frozen, start)
	}
}

func (f *nodbFreezer) ReadAncients(fn func(ethdb.AncientReaderOp) error) (err error) {
	return fn(f)
}

func (f *nodbFreezer) AncientRange(kind string, start, count, maxBytes uint64) ([][]byte, error) {
	return nil, errNotSupported
}

func (f *nodbFreezer) ModifyAncients(func(ethdb.AncientWriteOp) error) (int64, error) {
	return 0, nil
}

func (f *nodbFreezer) MigrateTable(kind string, convert convertLegacyFn) error {
	return nil
}

// delete leveldb data that save to ancientdb, split from func freeze
func gcKvStore(db ethdb.KeyValueStore, ancients []common.Hash, first uint64, frozen uint64, start time.Time) {
	// Wipe out all data from the active database
	batch := db.NewBatch()
	for i := 0; i < len(ancients); i++ {
		// Always keep the genesis block in active database
		if blockNumber := first + uint64(i); blockNumber != 0 {
			DeleteBlockWithoutNumber(batch, ancients[i], blockNumber)
			DeleteCanonicalHash(batch, blockNumber)
		}
	}
	if err := batch.Write(); err != nil {
		log.Crit("Failed to delete frozen canonical blocks", "err", err)
	}
	batch.Reset()

	// Wipe out side chains also and track dangling side chians
	var dangling []common.Hash
	for number := first; number < frozen; number++ {
		// Always keep the genesis block in active database
		if number != 0 {
			dangling = ReadAllHashes(db, number)
			for _, hash := range dangling {
				log.Trace("Deleting side chain", "number", number, "hash", hash)
				DeleteBlock(batch, hash, number)
			}
		}
	}
	if err := batch.Write(); err != nil {
		log.Crit("Failed to delete frozen side blocks", "err", err)
	}
	batch.Reset()

	// Step into the future and delete and dangling side chains
	if frozen > 0 {
		tip := frozen
		nfdb := &nofreezedb{KeyValueStore: db}
		for len(dangling) > 0 {
			drop := make(map[common.Hash]struct{})
			for _, hash := range dangling {
				log.Debug("Dangling parent from freezer", "number", tip-1, "hash", hash)
				drop[hash] = struct{}{}
			}
			children := ReadAllHashes(db, tip)
			for i := 0; i < len(children); i++ {
				// Dig up the child and ensure it's dangling
				child := ReadHeader(nfdb, children[i], tip)
				if child == nil {
					log.Error("Missing dangling header", "number", tip, "hash", children[i])
					continue
				}
				if _, ok := drop[child.ParentHash]; !ok {
					children = append(children[:i], children[i+1:]...)
					i--
					continue
				}
				// Delete all block data associated with the child
				log.Debug("Deleting dangling block", "number", tip, "hash", children[i], "parent", child.ParentHash)
				DeleteBlock(batch, children[i], tip)
			}
			dangling = children
			tip++
		}
		if err := batch.Write(); err != nil {
			log.Crit("Failed to delete dangling side blocks", "err", err)
		}
	}

	// Log something friendly for the user
	context := []interface{}{
		"blocks", frozen - first, "elapsed", common.PrettyDuration(time.Since(start)), "number", frozen - 1,
	}
	if n := len(ancients); n > 0 {
		context = append(context, []interface{}{"hash", ancients[n-1]}...)
	}
	log.Info("Deep froze chain segment", context...)
}
