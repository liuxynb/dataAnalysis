package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

// writeDayCSV 输出每天统计
func writeDayCSV(path string, ag *Aggregator) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()

	// header
	if err := w.Write([]string{"Date", "Reads", "Writes", "TotalOps", "Read/Write Ratio (read:write)"}); err != nil {
		return err
	}

	ag.dayMu.RLock()
	keys := make([]string, 0, len(ag.dayMap))
	for k := range ag.dayMap {
		keys = append(keys, k)
	}
	ag.dayMu.RUnlock()

	sort.Strings(keys)
	for _, k := range keys {
		ag.dayMu.RLock()
		cp := ag.dayMap[k]
		reads := cp.Reads
		writes := cp.Writes
		ag.dayMu.RUnlock()
		total := reads + writes
		ratio := "N/A"
		if reads > 0 {
			ratio = fmt.Sprintf("1:%.2f", float64(writes)/float64(reads))
		} else if writes > 0 {
			ratio = "0"
		}
		row := []string{
			k,
			strconv.FormatInt(reads, 10),
			strconv.FormatInt(writes, 10),
			strconv.FormatInt(total, 10),
			ratio,
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	fmt.Printf("已写出: %s\n", path)
	return nil
}

// writeHourCSV 输出每小时统计
func writeHourCSV(path string, ag *Aggregator) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.Write([]string{"Hour", "Reads", "Writes", "TotalOps", "Read/Write Ratio (read:write)"}); err != nil {
		return err
	}

	ag.hourMu.RLock()
	keys := make([]string, 0, len(ag.hourMap))
	for k := range ag.hourMap {
		keys = append(keys, k)
	}
	ag.hourMu.RUnlock()

	sort.Strings(keys)
	for _, k := range keys {
		ag.hourMu.RLock()
		cp := ag.hourMap[k]
		reads := cp.Reads
		writes := cp.Writes
		ag.hourMu.RUnlock()
		total := reads + writes
		ratio := "N/A"
		if reads > 0 {
			ratio = fmt.Sprintf("1:%.2f", float64(writes)/float64(reads))
		} else if writes > 0 {
			ratio = "0"
		}
		if err := w.Write([]string{
			k,
			strconv.FormatInt(reads, 10),
			strconv.FormatInt(writes, 10),
			strconv.FormatInt(total, 10),
			ratio,
		}); err != nil {
			return err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	fmt.Printf("已写出: %s\n", path)
	return nil
}

func writeMinuteCSV(path string, ag *Aggregator) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.Write([]string{"Minute", "Reads", "Writes", "TotalOps", "Read/Write Ratio (read:write)"}); err != nil {
		return err
	}

	ag.minuteMu.RLock()
	keys := make([]string, 0, len(ag.minuteMap))
	for k := range ag.minuteMap {
		keys = append(keys, k)
	}
	ag.minuteMu.RUnlock()

	sort.Strings(keys)
	for _, k := range keys {
		ag.minuteMu.RLock()
		cp := ag.minuteMap[k]
		reads := cp.Reads
		writes := cp.Writes
		ag.minuteMu.RUnlock()
		total := reads + writes
		ratio := "N/A"
		if reads > 0 {
			ratio = fmt.Sprintf("1:%.2f", float64(writes)/float64(reads))
		} else if writes > 0 {
			ratio = "0"
		}
		if err := w.Write([]string{
			k,
			strconv.FormatInt(reads, 10),
			strconv.FormatInt(writes, 10),
			strconv.FormatInt(total, 10),
			ratio,
		}); err != nil {
			return err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	fmt.Printf("已写出: %s\n", path)
	return nil
}

// writeVolumeCSV 输出每个卷的统计（按总操作数降序）
func writeVolumeCSV(path string, ag *Aggregator) error {
	type vrow struct {
		vid    string
		reads  int64
		writes int64
		total  int64
	}
	ag.volMu.RLock()
	rows := make([]vrow, 0, len(ag.volMap))
	for vid, cp := range ag.volMap {
		rows = append(rows, vrow{
			vid: vid, reads: cp.Reads, writes: cp.Writes, total: cp.Reads + cp.Writes,
		})
	}
	ag.volMu.RUnlock()

	// sort by total desc
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].total > rows[j].total
	})

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write([]string{"VolumeID", "Reads", "Writes", "TotalOps", "ReadRatio(%)"}); err != nil {
		return err
	}
	for _, r := range rows {
		ratio := "0"
		if r.total > 0 {
			ratio = fmt.Sprintf("%.2f", 100.0*float64(r.reads)/float64(r.total))
		}
		if err := w.Write([]string{
			r.vid,
			strconv.FormatInt(r.reads, 10),
			strconv.FormatInt(r.writes, 10),
			strconv.FormatInt(r.total, 10),
			ratio,
		}); err != nil {
			return err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	fmt.Printf("已写出: %s\n", path)
	return nil
}

func writeMinuteVolumeCSV(dir string, minuteKey string, mv map[string]*CountPair) error {
	if err := os.MkdirAll(dir, 0755); err != nil { return err }
	type v struct{ vid string; reads, writes, total int64 }
	rows := make([]v, 0, len(mv))
	for vid, cp := range mv { rows = append(rows, v{vid: vid, reads: cp.Reads, writes: cp.Writes, total: cp.Reads + cp.Writes}) }
	sort.Slice(rows, func(i, j int) bool { return rows[i].total > rows[j].total })
	name := strings.ReplaceAll(strings.ReplaceAll(minuteKey, ":", "-"), " ", "_")
	fp := dir + "/volume_" + name + ".csv"
	f, err := os.Create(fp)
	if err != nil { return err }
	w := csv.NewWriter(f)
	if err := w.Write([]string{"VolumeID", "Reads", "Writes", "TotalOps", "ReadRatio(%)"}); err != nil { f.Close(); return err }
	for _, r := range rows {
		ratio := "0"
		if r.total > 0 { ratio = fmt.Sprintf("%.2f", 100.0*float64(r.reads)/float64(r.total)) }
		if err := w.Write([]string{r.vid, strconv.FormatInt(r.reads,10), strconv.FormatInt(r.writes,10), strconv.FormatInt(r.total,10), ratio}); err != nil { f.Close(); return err }
	}
	w.Flush()
	if err := w.Error(); err != nil { f.Close(); return err }
	f.Close()
	fmt.Printf("已写出: %s\n", fp)
	return nil
}

func writeVolumeByMinuteDir(dir string, ag *Aggregator) error {
	if err := os.MkdirAll(dir, 0755); err != nil { return err }
	ag.minuteVolMu.RLock()
	keys := make([]string, 0, len(ag.minuteVolMap))
	for k := range ag.minuteVolMap { keys = append(keys, k) }
	ag.minuteVolMu.RUnlock()
	sort.Strings(keys)
	for _, k := range keys {
		ag.minuteVolMu.RLock()
		mv := ag.minuteVolMap[k]
		ag.minuteVolMu.RUnlock()
		type v struct{ vid string; reads, writes, total int64 }
		rows := make([]v, 0, len(mv))
		for vid, cp := range mv {
			rows = append(rows, v{vid: vid, reads: cp.Reads, writes: cp.Writes, total: cp.Reads + cp.Writes})
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].total > rows[j].total })
		name := strings.ReplaceAll(strings.ReplaceAll(k, ":", "-"), " ", "_")
		fp := dir + "/volume_" + name + ".csv"
		f, err := os.Create(fp)
		if err != nil { return err }
		w := csv.NewWriter(f)
		if err := w.Write([]string{"VolumeID", "Reads", "Writes", "TotalOps", "ReadRatio(%)"}); err != nil { f.Close(); return err }
		for _, r := range rows {
			ratio := "0"
			if r.total > 0 { ratio = fmt.Sprintf("%.2f", 100.0*float64(r.reads)/float64(r.total)) }
			if err := w.Write([]string{r.vid, strconv.FormatInt(r.reads,10), strconv.FormatInt(r.writes,10), strconv.FormatInt(r.total,10), ratio}); err != nil { f.Close(); return err }
		}
		w.Flush()
		if err := w.Error(); err != nil { f.Close(); return err }
		f.Close()
		fmt.Printf("已写出: %s\n", fp)
	}
	return nil
}

func printTopVolumes(ag *Aggregator, top int) {
	type vrow struct {
		vid    string
		reads  int64
		writes int64
		total  int64
	}
	ag.volMu.RLock()
	rows := make([]vrow, 0, len(ag.volMap))
	for vid, cp := range ag.volMap {
		rows = append(rows, vrow{vid: vid, reads: cp.Reads, writes: cp.Writes, total: cp.Reads + cp.Writes})
	}
	ag.volMu.RUnlock()

	sort.Slice(rows, func(i, j int) bool {
		return rows[i].total > rows[j].total
	})

	n := top
	if len(rows) < n {
		n = len(rows)
	}
	fmt.Printf("Top %d Volumes (by total ops):\n", n)
	for i := 0; i < n; i++ {
		r := rows[i]
		readRatio := 100.0 * float64(r.reads) / float64(maxInt64(1, r.total))
		fmt.Printf("%2d) Volume %s: Reads=%d Writes=%d Total=%d ReadRatio=%.2f%%\n",
			i+1, r.vid, r.reads, r.writes, r.total, readRatio)
	}
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
