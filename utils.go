package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Helper: Calculate Read/Write Ratio string
func calculateRatio(reads, writes int64) string {
	if reads > 0 {
		return fmt.Sprintf("1:%.2f", float64(writes)/float64(reads))
	} else if writes > 0 {
		return "0"
	}
	return "N/A"
}

// Helper: Calculate Read Ratio Percentage string
func calculateReadRatioPercent(reads, total int64) string {
	if total > 0 {
		return fmt.Sprintf("%.2f", 100.0*float64(reads)/float64(total))
	}
	return "0"
}

// Helper: Write generic CSV
func writeCSV(path string, header []string, rows [][]string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	if err := w.Write(header); err != nil {
		return err
	}
	if err := w.WriteAll(rows); err != nil {
		return err
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	fmt.Printf("已写出: %s\n", path)
	return nil
}

// Generic function to write time-based stats (Day/Hour/Minute)
func writeTimeStats(path string, statsMap map[string]*CountPair, timeHeader string, mu *sync.RWMutex) error {
	mu.RLock()
	keys := make([]string, 0, len(statsMap))
	for k := range statsMap {
		keys = append(keys, k)
	}

	// Snapshot data
	snapshot := make(map[string]CountPair, len(statsMap))
	for k, v := range statsMap {
		snapshot[k] = *v
	}
	mu.RUnlock()

	sort.Strings(keys)

	header := []string{timeHeader, "Reads", "Writes", "TotalOps", "Read/Write Ratio (read:write)"}
	rows := make([][]string, 0, len(keys))

	for _, k := range keys {
		cp := snapshot[k]
		total := cp.Reads + cp.Writes
		rows = append(rows, []string{
			k,
			strconv.FormatInt(cp.Reads, 10),
			strconv.FormatInt(cp.Writes, 10),
			strconv.FormatInt(total, 10),
			calculateRatio(cp.Reads, cp.Writes),
		})
	}
	return writeCSV(path, header, rows)
}

// writeDayCSV 输出每天统计
func writeDayCSV(path string, ag *Aggregator) error {
	return writeTimeStats(path, ag.dayMap, "Date", &ag.dayMu)
}

// writeHourCSV 输出每小时统计
func writeHourCSV(path string, ag *Aggregator) error {
	return writeTimeStats(path, ag.hourMap, "Hour", &ag.hourMu)
}

// writeMinuteCSV 输出每分钟统计
func writeMinuteCSV(path string, ag *Aggregator) error {
	return writeTimeStats(path, ag.minuteMap, "Minute", &ag.minuteMu)
}

// Helper for volume rows generation
type volRow struct {
	vid                  string
	reads, writes, total int64
}

func generateVolumeRows(mv map[string]*CountPair) []volRow {
	rows := make([]volRow, 0, len(mv))
	for vid, cp := range mv {
		rows = append(rows, volRow{vid: vid, reads: cp.Reads, writes: cp.Writes, total: cp.Reads + cp.Writes})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].total > rows[j].total })
	return rows
}

func formatVolumeRows(vRows []volRow) [][]string {
	rows := make([][]string, len(vRows))
	for i, r := range vRows {
		rows[i] = []string{
			r.vid,
			strconv.FormatInt(r.reads, 10),
			strconv.FormatInt(r.writes, 10),
			strconv.FormatInt(r.total, 10),
			calculateReadRatioPercent(r.reads, r.total),
		}
	}
	return rows
}

// writeVolumeCSV 输出每个卷的统计（按总操作数降序）
func writeVolumeCSV(path string, ag *Aggregator) error {
	ag.volMu.RLock()
	// Create a snapshot
	snapshot := make(map[string]*CountPair, len(ag.volMap))
	for k, v := range ag.volMap {
		snapshot[k] = &CountPair{Reads: v.Reads, Writes: v.Writes}
	}
	ag.volMu.RUnlock()

	vRows := generateVolumeRows(snapshot)
	header := []string{"VolumeID", "Reads", "Writes", "TotalOps", "ReadRatio(%)"}
	return writeCSV(path, header, formatVolumeRows(vRows))
}

func readVolumeStatsCSV(path string) (map[string]*CountPair, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	// Skip header
	if _, err := r.Read(); err != nil {
		return nil, nil // Empty file or header only
	}
	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}

	res := make(map[string]*CountPair)
	for _, row := range records {
		if len(row) < 3 {
			continue
		}
		vid := row[0]
		reads, _ := strconv.ParseInt(row[1], 10, 64)
		writes, _ := strconv.ParseInt(row[2], 10, 64)
		res[vid] = &CountPair{Reads: reads, Writes: writes}
	}
	return res, nil
}

func writeMinuteVolumeCSV(dir string, minuteKey string, mv map[string]*CountPair, merge bool) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	name := strings.ReplaceAll(strings.ReplaceAll(minuteKey, ":", "-"), " ", "_")
	fp := filepath.Join(dir, "volume_"+name+".csv")

	data := make(map[string]*CountPair)

	// If merge is enabled, try to load existing data
	if merge {
		if _, err := os.Stat(fp); err == nil {
			existing, err := readVolumeStatsCSV(fp)
			if err != nil {
				fmt.Printf("Warning: failed to read existing CSV %s: %v\n", fp, err)
			} else if existing != nil {
				data = existing
			}
		}
	}

	// Merge new data
	for vid, cp := range mv {
		if _, ok := data[vid]; !ok {
			data[vid] = &CountPair{}
		}
		data[vid].Reads += cp.Reads
		data[vid].Writes += cp.Writes
	}

	vRows := generateVolumeRows(data)
	header := []string{"VolumeID", "Reads", "Writes", "TotalOps", "ReadRatio(%)"}
	return writeCSV(fp, header, formatVolumeRows(vRows))
}

func writeVolumeByMinuteDir(dir string, ag *Aggregator, merge bool) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	ag.minuteVolMu.RLock()
	keys := make([]string, 0, len(ag.minuteVolMap))
	for k := range ag.minuteVolMap {
		keys = append(keys, k)
	}

	snapshot := make(map[string]map[string]*CountPair)
	for _, k := range keys {
		srcMv := ag.minuteVolMap[k]
		dstMv := make(map[string]*CountPair, len(srcMv))
		for vol, cp := range srcMv {
			dstMv[vol] = &CountPair{Reads: cp.Reads, Writes: cp.Writes}
		}
		snapshot[k] = dstMv
	}
	ag.minuteVolMu.RUnlock()

	sort.Strings(keys)

	for _, k := range keys {
		mv := snapshot[k]
		if err := writeMinuteVolumeCSV(dir, k, mv, merge); err != nil {
			return err
		}
	}
	return nil
}

func printTopVolumes(ag *Aggregator, top int) {
	ag.volMu.RLock()
	snapshot := make(map[string]*CountPair, len(ag.volMap))
	for k, v := range ag.volMap {
		snapshot[k] = &CountPair{Reads: v.Reads, Writes: v.Writes}
	}
	ag.volMu.RUnlock()

	vRows := generateVolumeRows(snapshot)

	n := top
	if len(vRows) < n {
		n = len(vRows)
	}
	fmt.Printf("Top %d Volumes (by total ops):\n", n)
	for i := 0; i < n; i++ {
		r := vRows[i]
		readRatio := 100.0 * float64(r.reads) / float64(maxInt64(1, r.total))
		fmt.Printf("%2d) Volume %s: Reads=%d Writes=%d Total=%d ReadRatio=%.2f%%\n",
			i+1, r.vid, r.reads, r.writes, r.total, readRatio)
	}
}

func writeStripeStats(path string, ag *Aggregator) error {
	ag.stripeMu.Lock()
	keys := make([]int, 0, len(ag.stripeUpdateMap))
	for k := range ag.stripeUpdateMap {
		keys = append(keys, k)
	}
	// Copy map data
	statsCopy := make(map[int]int, len(ag.stripeUpdateMap))
	for k, v := range ag.stripeUpdateMap {
		statsCopy[k] = v
	}
	ag.stripeMu.Unlock()

	sort.Ints(keys)

	header := []string{"UpdatedBlocksInStripe", "Count"}
	rows := make([][]string, len(keys))
	for i, k := range keys {
		rows[i] = []string{strconv.Itoa(k), strconv.Itoa(statsCopy[k])}
	}
	return writeCSV(path, header, rows)
}

func writeStripeHeatMap(path string, ag *Aggregator) error {
	ag.stripeMu.Lock()
	stripeIDs := make([]int64, 0, len(ag.stripeBlockHeatMap))

	// Deep copy needed
	dataCopy := make(map[int64][]CountPair, len(ag.stripeBlockHeatMap))
	for k, v := range ag.stripeBlockHeatMap {
		stripeIDs = append(stripeIDs, k)
		// Deep copy the slice
		newSlice := make([]CountPair, len(v))
		copy(newSlice, v)
		dataCopy[k] = newSlice
	}

	// Capture config under lock
	dBlocks := ag.dataBlocks
	// pBlocks := ag.parityBlocks // not strictly needed for logic if we iterate all

	ag.stripeMu.Unlock()

	sort.Slice(stripeIDs, func(i, j int) bool { return stripeIDs[i] < stripeIDs[j] })

	var rows [][]string
	for _, sid := range stripeIDs {
		counters := dataCopy[sid]
		for idx := 0; idx < len(counters); idx++ {
			reads := counters[idx].Reads
			writes := counters[idx].Writes
			if reads == 0 && writes == 0 {
				continue
			}
			blockType := "Data"
			if idx >= dBlocks {
				blockType = "Parity"
			}
			rows = append(rows, []string{
				strconv.FormatInt(sid, 10),
				strconv.Itoa(idx),
				blockType,
				strconv.FormatInt(reads, 10),
				strconv.FormatInt(writes, 10),
				strconv.FormatInt(reads+writes, 10),
			})
		}
	}

	header := []string{"StripeID", "BlockIndex", "BlockType", "Reads", "Writes", "TotalOps"}
	return writeCSV(path, header, rows)
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// writeStripeOpsCSV 输出指定卷的详细条带操作日志
func writeStripeOpsCSV(path string, ag *Aggregator) error {
	ag.stripeMu.Lock()
	// Copy slice
	ops := make([]StripeOperation, len(ag.stripeOps))
	copy(ops, ag.stripeOps)
	ag.stripeMu.Unlock()

	sort.Slice(ops, func(i, j int) bool {
		return ops[i].OptionTime.Before(ops[j].OptionTime)
	})

	header := []string{"StripeID", "BlockIndex", "BlockType", "Read/Write", "OptionTime"}
	rows := make([][]string, len(ops))
	for i, op := range ops {
		rows[i] = []string{
			strconv.FormatInt(op.StripeID, 10),
			strconv.Itoa(op.BlockIndex),
			op.BlockType,
			op.ReadWrite,
			op.OptionTime.Format("2006-01-02 15:04:05.000000"),
		}
	}
	return writeCSV(path, header, rows)
}
