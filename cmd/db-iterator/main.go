package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/redis/go-redis/v9"
)

var (
	datadir  = flag.String("datadir", "", "The path of pebble datadir(generally, path ends with chaindata)")
	redisURL = flag.String("redis-url", "127.0.0.1:6379", "Redis connection URL")
	redisDB  = flag.Int("redis-db", 1, "Redis database")
	workers  = flag.Int("workers", runtime.NumCPU(), "How many Redis workers used to push results")
)

const (
	// minCache is the minimum amount of memory in megabytes to allocate to pebble
	// read and write caching, split half and half.
	minCache = 16

	// minHandles is the minimum number of files handles to allocate to the open
	// database files.
	minHandles = 16
)

func New(file string, cache int, handles int, readonly bool) (*pebble.DB, error) {
	// Ensure we have some minimal caching and file guarantees
	if cache < minCache {
		cache = minCache
	}
	if handles < minHandles {
		handles = minHandles
	}
	logger := log.New("database", file)
	logger.Info("Allocated cache and file handles", "cache", common.StorageSize(cache*1024*1024), "handles", handles)

	// The max memtable size is limited by the uint32 offsets stored in
	// internal/arenaskl.node, DeferredBatchOp, and flushableBatchEntry.
	// Taken from https://github.com/cockroachdb/pebble/blob/master/open.go#L38
	maxMemTableSize := 4<<30 - 1 // Capped by 4 GB

	// Two memory tables is configured which is identical to leveldb,
	// including a frozen memory table and another live one.
	memTableLimit := 2
	memTableSize := cache * 1024 * 1024 / 2 / memTableLimit
	if memTableSize > maxMemTableSize {
		memTableSize = maxMemTableSize
	}
	opt := &pebble.Options{
		// Pebble has a single combined cache area and the write
		// buffers are taken from this too. Assign all available
		// memory allowance for cache.
		Cache:        pebble.NewCache(int64(cache * 1024 * 1024)),
		MaxOpenFiles: handles,

		// The size of memory table(as well as the write buffer).
		// Note, there may have more than two memory tables in the system.
		MemTableSize: uint64(memTableSize),

		// MemTableStopWritesThreshold places a hard limit on the size
		// of the existent MemTables(including the frozen one).
		// Note, this must be the number of tables not the size of all memtables
		// according to https://github.com/cockroachdb/pebble/blob/master/options.go#L738-L742
		// and to https://github.com/cockroachdb/pebble/blob/master/db.go#L1892-L1903.
		MemTableStopWritesThreshold: memTableLimit,

		// Per-level options. Options for at least one level must be specified. The
		// options for the last level are used for all subsequent levels.
		Levels: []pebble.LevelOptions{
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
		},
		ReadOnly: readonly,
	}
	// Disable seek compaction explicitly. Check https://github.com/ethereum/go-ethereum/pull/20130
	// for more details.
	opt.Experimental.ReadSamplingMultiplier = -1

	// Open the db and recover any potential corruptions
	db, err := pebble.Open(file, opt)
	return db, err
}

func iterate(db *pebble.DB, start []byte, end []byte, taskChan chan task) {
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	if err != nil {
		log.Crit("failed to create iterator", "err", err)
	}
	defer iter.Close()

	count := 0
	ct := time.Now()
	st := time.Now()
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		val := iter.Value()

		taskChan <- task{key, val}

		count++
		if since := time.Since(st); since > 10*time.Second {
			log.Info("Iterator ethdb", "range", fmt.Sprintf("%#x-%#x", start, end), "count", count, "elapsed", common.PrettyDuration(time.Since(ct)), "tps", count/int(time.Since(ct).Seconds()))
			st = time.Now()
		}
	}
}

type task struct {
	key []byte
	val []byte
}

func main() {
	flag.Parse()
	log.SetDefault(log.NewLogger(log.NewTerminalHandlerWithLevel(os.Stderr, log.LevelInfo, true)))

	rdb := redis.NewClient(&redis.Options{
		Addr:     *redisURL,
		DB:       *redisDB,
		PoolSize: 2**workers + 1,
	})
	defer rdb.Close()

	edb, err := New(*datadir, 16*1024, 50000, true)
	if err != nil {
		log.Crit("failed to open pebble db", "err", err)
	}

	var (
		taskChan = make(chan task, 8192**workers)
		done     = make(chan bool)
	)

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				log.Info("Chan stats", "#task", len(taskChan), "%util", len(taskChan)*100.0/cap(taskChan))
			}
		}

	}()

	for i := 0; i < *workers; i++ {
		go func() {
			ctx := context.Background()
			for t := range taskChan {
				klen := len(t.key)
				vlen := len(t.val)
				if vlen >= 100*1024 {
					log.Error("Large value", "key", common.Bytes2Hex(t.key), "vlen", vlen)
				}

				kKey := fmt.Sprintf("k-%x-%d", string(t.key[0]), klen)
				if _, err := rdb.Incr(ctx, kKey).Result(); err != nil {
					log.Crit("failed to increase redis", "rkey", kKey, "err", err)
				}

				vKey := fmt.Sprintf("v-%d", vlen)
				if _, err := rdb.Incr(ctx, vKey).Result(); err != nil {
					log.Crit("failed to increase redis", "rkey", vKey, "err", err)
				}

				kvKey := fmt.Sprintf("kv-%x-%d-%d", string(t.key[0]), klen, vlen)
				if _, err := rdb.Incr(ctx, kvKey).Result(); err != nil {
					log.Crit("failed to increase redis", "rkey", kvKey, "err", err)
				}
			}
		}()
	}

	var wg sync.WaitGroup
	for b := 0x00; b <= 0xf0; b += 0x10 {
		var (
			start = []byte{byte(b)}
			end   = []byte{byte(b + 0x10)}
		)
		if b == 0xf0 {
			end = nil
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			iterate(edb, start, end, taskChan)
		}()
	}

	wg.Wait()
	close(taskChan)
	done <- true
}
