package main

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type PartitionLag struct {
	lag       int64
	partition int32
}
type ByLag []PartitionLag

func (a ByLag) Len() int           { return len(a) }
func (a ByLag) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByLag) Less(i, j int) bool { return a[i].lag > a[j].lag }

type LagCache struct {
	monitor           Monitor // keep a monitor to refresh cache for lag
	consumerGroup     string  // consumer and topic to monitor the lag
	topic             string
	TTL               time.Duration // we can be aggressive here as its in async
	mu                sync.Mutex
	partitionsLag     []PartitionLag
	partitions        int
	lastPartitionUsed int32 // to use as round-robin in case anything goes wrong
	close             chan bool
}

func NewLagCache(cg, topic string, mCfg MonitorConfig, ttl time.Duration) (*LagCache, error) {
	monitor, err := NewMonitor(mCfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create kafka monitor")
	}
	parts, err := monitor.GetNumberOfPartitions(topic)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get no of partitions for topic")
	}

	lc := &LagCache{
		monitor:       monitor,
		mu:            sync.Mutex{},
		TTL:           ttl,
		consumerGroup: cg,
		topic:         topic,
		partitions:    parts,
		partitionsLag: make([]PartitionLag, parts),
		close:         make(chan bool, 1),
	}
	go lc.RefreshCache()

	return lc, nil
}

func (lc *LagCache) RefreshCache() {
	refresh := func() {
		_, _, lags, err := lc.monitor.GetConsumerGroupLag(lc.consumerGroup, lc.topic)
		if err != nil {
			fmt.Println("kafka admin error - failed to get lag, fallback to round robin")
			lags = make(map[int32]int64)
			return
		}
		// meaning - consumer group lag is there
		lc.mu.Lock()
		defer lc.mu.Unlock()
		lc.partitionsLag = diffFromMax(lags)
	}
	refresh() // first time fill the cache from current lag
	for {
		select {
		case <-lc.close:
			fmt.Println("stopping the cache refresh")
			return
		case <-time.After(lc.TTL):
			refresh()
			fmt.Println("cache refreshed")
		}
	}
}

func (lc *LagCache) GetPartitionWithMinLag() int32 {
	part := (lc.lastPartitionUsed + 1) % int32(lc.partitions) // default round-robin
	// if there is only one partition or monitoring is failing from kafka side
	// if there is only one partition - no matter lag just push in 0
	// if kafka lag call failed, having nil in partitionsLag - use round-robin
	if len(lc.partitionsLag) <= 1 {
		lc.lastPartitionUsed = part
		return part
	}
	// read the first element from lag, having that first element has the minimum lag as array is sorted all the time
	lc.mu.Lock()
	defer lc.mu.Unlock()
	// sort lag before using first element
	// TODO - this can be optimized, sort only in refresh-cache as all new numbers, after that should be promotion or demotion of the elements
	// can be done in doubly linkedList
	sort.Sort(ByLag(lc.partitionsLag))
	if lc.partitionsLag[0].lag > 0 {
		minLag := lc.partitionsLag[0]
		part = minLag.partition
		lc.lastPartitionUsed = part
		minLag.lag = minLag.lag - 1
		lc.partitionsLag[0] = minLag
	}
	return part
}

// diffFromMax - creates an array of lags, its not actual lag but distance from max lag in all the partitions
// if one partition has lag MAX and others are less than that - array will be diff from MAX for each partition which is basically how many msgs we push so that all the partitions
// will have same lag assuming consumer is not processing anything at the moment.. over time it will keep creating the balance among all partitions
func diffFromMax(inputMap map[int32]int64) []PartitionLag {
	if len(inputMap) == 0 {
		return nil
	}

	var maxVal int64
	for _, v := range inputMap {
		if v > maxVal {
			maxVal = v
		}
	}

	// Create a new map to store the differences
	var result []PartitionLag
	for k, v := range inputMap {
		if v != -1 {
			p := PartitionLag{partition: k, lag: maxVal - v}
			result = append(result, p)
		}

	}
	sort.Sort(ByLag(result))
	return result
}
