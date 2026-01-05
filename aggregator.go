package main

import (
	"sync"
	"time"
)

// 聚合结构（读写计数）
type CountPair struct {
	Reads  int64
	Writes int64
}

type StripeOperation struct {
	StripeID   int64
	BlockIndex int
	BlockType  string // "Data" or "Parity"
	ReadWrite  string // "Read" or "Write"
	OptionTime time.Time
}

// 全局统计容器（并发安全通过 mutex）
type Aggregator struct {
	dayMu  sync.RWMutex
	dayMap map[string]*CountPair // key: "01-02"

	hourMu  sync.RWMutex
	hourMap map[string]*CountPair // key: "01-02 15"

	minuteMu     sync.RWMutex
	minuteMap    map[string]*CountPair
	minuteVolMu  sync.RWMutex
	minuteVolMap map[string]map[string]*CountPair // key: "01-02 15:04" -> VolumeID -> CountPair

	minuteOrder        []string
	minuteBufLimit     int
	onEvict            func(string, map[string]*CountPair)
	enableMinuteVolume bool

	volMu  sync.RWMutex
	volMap map[string]*CountPair // key: VolumeID

	hasStart bool
	start    time.Time
	hasEnd   bool
	end      time.Time

	stripeMu        sync.Mutex
	targetVolume    string
	stripeUpdateMap map[int]int // Key: Number of blocks updated (1-10), Value: Count

	// map[StripeID][]CountPair
	// Index 0-(DataBlocks-1): Data Blocks
	// Index DataBlocks-(TotalBlocks-1): Parity Blocks
	stripeBlockHeatMap map[int64][]CountPair
	blockSize          int64
	dataBlocks         int
	parityBlocks       int

	stripeOps []StripeOperation
}

func NewAggregator() *Aggregator {
	return &Aggregator{
		dayMap:             make(map[string]*CountPair),
		hourMap:            make(map[string]*CountPair),
		minuteMap:          make(map[string]*CountPair),
		minuteVolMap:       make(map[string]map[string]*CountPair),
		minuteOrder:        make([]string, 0, 256),
		minuteBufLimit:     240,
		enableMinuteVolume: true,
		volMap:             make(map[string]*CountPair),
		stripeUpdateMap:    make(map[int]int),
		stripeBlockHeatMap: make(map[int64][]CountPair),
		stripeOps:          make([]StripeOperation, 0),
		blockSize:          64 * 1024,
		dataBlocks:         10,
		parityBlocks:       4,
	}
}

func (ag *Aggregator) SetTargetVolume(vol string)                        { ag.targetVolume = vol }
func (ag *Aggregator) SetMinuteBufLimit(n int)                           { ag.minuteBufLimit = n }
func (ag *Aggregator) EnableMinuteVolume(enable bool)                    { ag.enableMinuteVolume = enable }
func (ag *Aggregator) SetOnEvict(fn func(string, map[string]*CountPair)) { ag.onEvict = fn }
func (ag *Aggregator) SetStripeConfig(blockSize int64, dataBlocks, parityBlocks int) {
	if blockSize > 0 {
		ag.blockSize = blockSize
	}
	if dataBlocks > 0 {
		ag.dataBlocks = dataBlocks
	}
	if parityBlocks >= 0 {
		ag.parityBlocks = parityBlocks
	}
}
func (ag *Aggregator) SetTimeRange(from, to *time.Time) {
	if from != nil {
		ag.hasStart = true
		ag.start = *from
	} else {
		ag.hasStart = false
	}
	if to != nil {
		ag.hasEnd = true
		ag.end = *to
	} else {
		ag.hasEnd = false
	}
}

func (ag *Aggregator) addRecord(ts time.Time, ioType string, vol string, offset, size int64) {
	// normalize ioType to "0" (read) or "1" (write)
	ioType = normalizeIOType(ioType)
	if ag.hasStart && ts.Before(ag.start) {
		return
	}
	if ag.hasEnd && ts.After(ag.end) {
		return
	}

	if ag.targetVolume != "" && vol == ag.targetVolume {
		// Stripe analysis logic
		totalBlocks := int64(ag.dataBlocks + ag.parityBlocks)

		startBlock := offset / ag.blockSize
		endBlock := (offset + size - 1) / ag.blockSize

		// map[StripeID] -> set of block indices
		stripesTouched := make(map[int64]map[int]bool)

		for b := startBlock; b <= endBlock; b++ {
			stripeID := b / totalBlocks
			blockIdx := int(b % totalBlocks)

			if _, ok := stripesTouched[stripeID]; !ok {
				stripesTouched[stripeID] = make(map[int]bool)
			}
			stripesTouched[stripeID][blockIdx] = true
		}

		ag.stripeMu.Lock()
		for stripeID, touchedBlocks := range stripesTouched {
			if ioType == "1" {
				count := len(touchedBlocks)
				ag.stripeUpdateMap[count]++
			}

			// Heatmap update
			// Get or create the counters for this stripe
			counters, ok := ag.stripeBlockHeatMap[stripeID]
			if !ok {
				counters = make([]CountPair, int(totalBlocks))
				ag.stripeBlockHeatMap[stripeID] = counters
			}

			// Update Blocks
			for blockIdx := range touchedBlocks {
				if blockIdx >= 0 && blockIdx < int(totalBlocks) {
					if ioType == "0" {
						counters[blockIdx].Reads++
					} else {
						counters[blockIdx].Writes++
					}

					// Record detailed stripe operation
					bType := "Data"
					if blockIdx >= ag.dataBlocks {
						bType = "Parity"
					}
					rw := "Read"
					if ioType == "1" {
						rw = "Write"
					}
					ag.stripeOps = append(ag.stripeOps, StripeOperation{
						StripeID:   stripeID,
						BlockIndex: blockIdx,
						BlockType:  bType,
						ReadWrite:  rw,
						OptionTime: ts,
					})
				}
			}
		}
		ag.stripeMu.Unlock()
	}

	dayKey := ts.Format("01-02")
	hourKey := ts.Format("01-02 15")
	minuteKey := ts.Format("01-02 15:04")

	// day
	ag.dayMu.Lock()
	cp, ok := ag.dayMap[dayKey]
	if !ok {
		cp = &CountPair{}
		ag.dayMap[dayKey] = cp
	}
	if ioType == "0" {
		cp.Reads++
	} else {
		cp.Writes++
	}
	ag.dayMu.Unlock()

	// hour
	ag.hourMu.Lock()
	hcp, ok := ag.hourMap[hourKey]
	if !ok {
		hcp = &CountPair{}
		ag.hourMap[hourKey] = hcp
	}
	if ioType == "0" {
		hcp.Reads++
	} else {
		hcp.Writes++
	}
	ag.hourMu.Unlock()

	// minute
	ag.minuteMu.Lock()
	mcp, ok := ag.minuteMap[minuteKey]
	if !ok {
		mcp = &CountPair{}
		ag.minuteMap[minuteKey] = mcp
	}
	if ioType == "0" {
		mcp.Reads++
	} else {
		mcp.Writes++
	}
	ag.minuteMu.Unlock()

	// minute-volume
	if ag.enableMinuteVolume {
		var evictedKey string
		var evictedMap map[string]*CountPair
		ag.minuteVolMu.Lock()
		mv, ok := ag.minuteVolMap[minuteKey]
		if !ok {
			mv = make(map[string]*CountPair)
			ag.minuteVolMap[minuteKey] = mv
			ag.minuteOrder = append(ag.minuteOrder, minuteKey)
		}
		vmin, ok := mv[vol]
		if !ok {
			vmin = &CountPair{}
			mv[vol] = vmin
		}
		if ioType == "0" {
			vmin.Reads++
		} else {
			vmin.Writes++
		}
		if ag.minuteBufLimit > 0 && len(ag.minuteOrder) > ag.minuteBufLimit {
			evictedKey = ag.minuteOrder[0]
			evictedMap = ag.minuteVolMap[evictedKey]
			delete(ag.minuteVolMap, evictedKey)
			ag.minuteOrder = ag.minuteOrder[1:]
		}
		ag.minuteVolMu.Unlock()
		if evictedMap != nil && ag.onEvict != nil {
			ag.onEvict(evictedKey, evictedMap)
		}
	}

	// volume
	ag.volMu.Lock()
	vp, ok := ag.volMap[vol]
	if !ok {
		vp = &CountPair{}
		ag.volMap[vol] = vp
	}
	if ioType == "0" {
		vp.Reads++
	} else {
		vp.Writes++
	}
	ag.volMu.Unlock()
}
