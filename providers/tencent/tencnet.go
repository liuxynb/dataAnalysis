package tencent

import (
	"encoding/csv"
	"strconv"
	"strings"
	"time"
)

type Parser struct{}

func NewParser() *Parser { return &Parser{} }

func (p *Parser) Parse(line string) (time.Time, string, string, bool) {
	r := csv.NewReader(strings.NewReader(line))
	r.FieldsPerRecord = -1
	rec, err := r.Read()
	if err != nil || len(rec) < 5 {
		return time.Time{}, "", "", false
	}
	tsStr := strings.TrimSpace(rec[0])
	ioType := strings.TrimSpace(rec[3])
	volID := strings.TrimSpace(rec[4])
	tsInt, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return time.Time{}, "", "", false
	}
	ts := time.Unix(tsInt, 0).UTC().Local()
	return ts, ioType, volID, true
}
