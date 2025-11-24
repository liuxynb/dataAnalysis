package msrc

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
	if err != nil || len(rec) < 7 {
		return time.Time{}, "", "", false
	}
	if strings.EqualFold(strings.TrimSpace(rec[0]), "Timestamp") {
		return time.Time{}, "", "", false
	}

	tsStr := strings.TrimSpace(rec[0])
	host := strings.TrimSpace(rec[1])
	disk := strings.TrimSpace(rec[2])
	typ := strings.TrimSpace(rec[3])

	ft, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return time.Time{}, "", "", false
	}
	const winEpochDiffSeconds = 11644473600
	secs := ft / 10000000
	nanos := (ft % 10000000) * 100
	ts := time.Unix(secs-winEpochDiffSeconds, nanos).UTC().Local()

	volID := host + "-" + disk
	return ts, typ, volID, true
}
