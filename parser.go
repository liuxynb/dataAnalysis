package main

import (
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// parserWorker: 从 lineCh 读取行并解析
func parserWorker(lineCh <-chan string, agg *Aggregator, totalParsed *uint64, parseErrCount *uint64) {
	for line := range lineCh {
		// parse CSV line robustly using encoding/csv (handles quotes)
		r := csv.NewReader(strings.NewReader(line))
		r.FieldsPerRecord = -1
		rec, err := r.Read()
		if err != nil {
			atomic.AddUint64(parseErrCount, 1)
			continue
		}
		if len(rec) < 5 {
			atomic.AddUint64(parseErrCount, 1)
			continue
		}
		// fields: Timestamp,Offset,Size,IOType,VolumeID
		tsStr := strings.TrimSpace(rec[0])
		ioType := strings.TrimSpace(rec[3])
		volID := strings.TrimSpace(rec[4])

		// parse timestamp as unix seconds
		tsInt, err := strconv.ParseInt(tsStr, 10, 64)
		if err != nil {
			atomic.AddUint64(parseErrCount, 1)
			continue
		}
		ts := time.Unix(tsInt, 0).UTC().Local()

		agg.addRecord(ts, ioType, volID)
		atomic.AddUint64(totalParsed, 1)
	}
}

// normalizeIOType: 更健壮的 IO type 解析，返回 "0" 或 "1"
func normalizeIOType(s string) string {
	// possible formats: "Read(0)", "Write(1)", "0", "1", "\"Read(0)\"" etc.
	s = strings.TrimSpace(s)
	s = strings.Trim(s, "\"")
	l := strings.ToLower(s)

	// try extract digit inside parentheses anywhere
	if idx := strings.Index(l, "("); idx >= 0 {
		// find closing ')'
		if j := strings.Index(l[idx:], ")"); j >= 0 {
			inner := strings.TrimSpace(l[idx+1 : idx+j])
			if inner == "0" || inner == "1" {
				return inner
			}
		}
	}

	// explicit words
	if strings.Contains(l, "read") {
		return "0"
	}
	if strings.Contains(l, "write") {
		return "1"
	}

	// plain digits
	if l == "0" || l == "1" {
		return l
	}

	// prefix check
	if strings.HasPrefix(l, "r") {
		return "0"
	}
	if strings.HasPrefix(l, "w") {
		return "1"
	}

	// fallback: conservative treat as write
	fmt.Printf("警告: 未知 IOType='%s'，按 write 处理\n", s)
	return "1"
}
