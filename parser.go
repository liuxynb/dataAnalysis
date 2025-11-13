package main

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

type Parser interface {
	Parse(line string) (time.Time, string, string, bool)
}

func parserWorker(lineCh <-chan string, parser Parser, agg *Aggregator, totalParsed *uint64, parseErrCount *uint64) {
	for line := range lineCh {
		ts, ioType, volID, ok := parser.Parse(line)
		if !ok {
			atomic.AddUint64(parseErrCount, 1)
			continue
		}
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
