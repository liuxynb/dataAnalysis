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
	}
}

func (ag *Aggregator) SetMinuteBufLimit(n int) { ag.minuteBufLimit = n }
func (ag *Aggregator) EnableMinuteVolume(enable bool) { ag.enableMinuteVolume = enable }
func (ag *Aggregator) SetOnEvict(fn func(string, map[string]*CountPair)) { ag.onEvict = fn }

func (ag *Aggregator) addRecord(ts time.Time, ioType string, vol string) {
	// normalize ioType to "0" (read) or "1" (write)
	ioType = normalizeIOType(ioType)

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
		if ioType == "0" { vmin.Reads++ } else { vmin.Writes++ }
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
