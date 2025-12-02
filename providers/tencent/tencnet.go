package tencent

import (
	"encoding/csv"
	"strconv"
	"strings"
	"time"
)

type Parser struct{}

func NewParser() *Parser { return &Parser{} }

func (p *Parser) Parse(line string) (time.Time, string, string, int64, int64, bool) {
	r := csv.NewReader(strings.NewReader(line))
	r.FieldsPerRecord = -1
	rec, err := r.Read()
	if err != nil || len(rec) < 5 {
		return time.Time{}, "", "", 0, 0, false
	}
	tsStr := strings.TrimSpace(rec[0])
	offsetStr := strings.TrimSpace(rec[1])
	sizeStr := strings.TrimSpace(rec[2])
	ioType := strings.TrimSpace(rec[3])
	volID := strings.TrimSpace(rec[4])

	offset, _ := strconv.ParseInt(offsetStr, 10, 64)
	size, _ := strconv.ParseInt(sizeStr, 10, 64)

	tsInt, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return time.Time{}, "", "", 0, 0, false
	}
	ts := time.Unix(tsInt, 0).UTC().Local()
	return ts, ioType, volID, offset, size, true
}
