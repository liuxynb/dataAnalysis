package alicloud

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
	if strings.EqualFold(strings.TrimSpace(rec[0]), "device_id") {
		return time.Time{}, "", "", false
	}
	deviceID := strings.TrimSpace(rec[0])
	opcode := strings.TrimSpace(rec[1])
	tsMicrosStr := strings.TrimSpace(rec[4])
	tsMicros, err := strconv.ParseInt(tsMicrosStr, 10, 64)
	if err != nil {
		return time.Time{}, "", "", false
	}
	ts := time.Unix(tsMicros/1e6, (tsMicros%1e6)*1e3).UTC().Local()
	return ts, opcode, deviceID, true
}
