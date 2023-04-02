package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
	"github.com/ethereum/go-ethereum/log"
	"github.com/redis/go-redis/v9"
)

var (
	datadir  = flag.String("datadir", "", "datadir")
	redisURL = flag.String("redis-url", "127.0.0.1:6379", "redis url")
	redisDB  = flag.Int("redis-db", 1, "redis db")
)

func main() {
	flag.Parse()
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stdout, log.TerminalFormat(true))))

	rdb := redis.NewClient(&redis.Options{
		Addr: *redisURL,
		DB:   *redisDB,
	})

	edb, err := pebble.New(*datadir, 4096, 50000, "chaindata", true)
	if err != nil {
		log.Crit("failed to open pebble db", "err", err)
	}
	ctx := context.Background()

	keyChan := make(chan []byte, 8192)
	valChan := make(chan []byte, 8192)

	go func() {
		for key := range keyChan {
			rKey := fmt.Sprintf("k-%x-%d", string(key[0]), len(key))
			if _, err := rdb.Incr(ctx, rKey).Result(); err != nil {
				log.Crit("failed to increase redis", "rkey", rKey, "err", err)
			}
		}
	}()
	go func() {
		for val := range valChan {
			rKey := fmt.Sprintf("v-%d", len(val))
			if _, err := rdb.Incr(ctx, rKey).Result(); err != nil {
				log.Crit("failed to increase redis", "rkey", rKey, "err", err)
			}
		}
	}()

	it := edb.NewIterator(nil, nil)
	count := 0
	st := time.Now()
	for it.Next() {
		key := it.Key()
		val := it.Value()

		keyChan <- key
		valChan <- val

		count += 1
		if count%10000 == 0 {
			log.Info("Iterator ethdb", "count", count, "elapsed", common.PrettyDuration(time.Since(st)))
		}
	}

	close(keyChan)
	close(valChan)
}
