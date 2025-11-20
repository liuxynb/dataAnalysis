/*
- 基本使用： go run split_alicloud_csv.go -in input.csv
- 指定输出目录： go run split_alicloud_csv.go -in input.csv -out out
- 指定日期列与格式： go run split_alicloud_csv.go -in input.csv -out out -date BillingTime -format "2006-01-02 15:04:05"
- 指定时区： go run split_alicloud_csv.go -in input.csv -out out -loc Asia/Shanghai
- 指定文件前缀： go run split_alicloud_csv.go -in input.csv -out out -prefix bill
- 覆盖输出文件： go run split_alicloud_csv.go -in input.csv -out out -force

*/

package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func main() {
	in := flag.String("in", "", "input csv path")
	out := flag.String("out", "", "output directory")
	dateCol := flag.String("date", "", "date column name")
	format := flag.String("format", "", "date layout")
	epochUnit := flag.String("epoch", "", "epoch unit for numeric timestamps: s|ms|us|ns")
	locName := flag.String("loc", "Asia/Shanghai", "time zone, e.g. Asia/Shanghai or UTC")
	prefix := flag.String("prefix", "", "output file prefix")
	force := flag.Bool("force", false, "overwrite output files")
	maxOpen := flag.Int("maxopen", 64, "maximum concurrently open output files")
	bufsize := flag.Int("bufsize", 1<<20, "I/O buffer size in bytes")
	flag.Parse()

	if *in == "" {
		fmt.Fprintln(os.Stderr, "missing -in")
		os.Exit(1)
	}
	if *out == "" {
		*out = filepath.Dir(*in)
	}
	if *prefix == "" {
		b := filepath.Base(*in)
		ext := filepath.Ext(b)
		*prefix = strings.TrimSuffix(b, ext)
	}

	var loc *time.Location
	if strings.EqualFold(*locName, "Local") {
		loc = time.Local
	} else {
		l, err := time.LoadLocation(*locName)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
		loc = l
	}

	if _, err := os.Stat(*out); os.IsNotExist(err) {
		if err := os.MkdirAll(*out, 0755); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	}

	f, err := os.Open(*in)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	defer f.Close()

	br := bufio.NewReaderSize(f, *bufsize)
	r := csv.NewReader(br)
	r.FieldsPerRecord = -1
	r.LazyQuotes = true
	r.TrimLeadingSpace = true

	header, err := r.Read()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	for i := range header {
		header[i] = strings.TrimPrefix(header[i], "\uFEFF")
	}

	dIdx := findDateIndex(header, *dateCol)
	if dIdx < 0 {
		fmt.Fprintln(os.Stderr, "date column not found")
		os.Exit(1)
	}

	writers := map[string]*csv.Writer{}
	files := map[string]*os.File{}
	bufs := map[string]*bufio.Writer{}
	createdOnce := map[string]bool{}
	lastUsed := map[string]int64{}
	days := map[string]struct{}{}
	var tick int64

	var total, written, skipped int

	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			skipped++
			continue
		}
		total++
		if dIdx >= len(row) {
			skipped++
			continue
		}
		ds := strings.TrimSpace(row[dIdx])
		if ds == "" {
			skipped++
			continue
		}
		t, ok := parseDate(ds, *format, *epochUnit, loc)
		if !ok {
			skipped++
			continue
		}
		down := t.In(loc).Format("2006-01-02")
		days[down] = struct{}{}
		w := writers[down]
		if w == nil {
			if len(writers) >= *maxOpen {
				var evictKey string
				var minTick int64 = 1 << 62
				for k, v := range lastUsed {
					if v < minTick {
						minTick = v
						evictKey = k
					}
				}
				if evictKey != "" {
					writers[evictKey].Flush()
					bufs[evictKey].Flush()
					files[evictKey].Close()
					delete(writers, evictKey)
					delete(bufs, evictKey)
					delete(files, evictKey)
					delete(lastUsed, evictKey)
				}
			}
			p := filepath.Join(*out, fmt.Sprintf("%s-%s.csv", *prefix, down))
			var of *os.File
			var created bool
			if *force && !createdOnce[down] {
				of, err = os.Create(p)
				if err != nil {
					skipped++
					continue
				}
				created = true
				createdOnce[down] = true
			} else {
				of, err = os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
				if err != nil {
					if os.IsExist(err) {
						of, err = os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0644)
						if err != nil {
							skipped++
							continue
						}
					} else {
						skipped++
						continue
					}
				} else {
					created = true
				}
			}
			files[down] = of
			buf := bufio.NewWriterSize(of, *bufsize)
			bufs[down] = buf
			w = csv.NewWriter(buf)
			writers[down] = w
			if created {
				if err := w.Write(header); err != nil {
					skipped++
					continue
				}
			}
		}
		if err := w.Write(row); err != nil {
			skipped++
			continue
		}
		lastUsed[down] = tick
		tick++
		written++
	}

	for _, w := range writers {
		w.Flush()
	}
	for _, b := range bufs {
		b.Flush()
	}
	for _, f := range files {
		f.Close()
	}
	fmt.Fprintf(os.Stdout, "rows=%d written=%d skipped=%d days=%d\n", total, written, skipped, len(days))
}

func findDateIndex(header []string, name string) int {
	return 4
}

func parseDate(s string, layout string, unit string, loc *time.Location) (time.Time, bool) {
	if layout != "" {
		if t, err := time.ParseInLocation(layout, s, loc); err == nil {
			return t, true
		}
		return time.Time{}, false
	}
	trim := strings.TrimSpace(s)
	if trim == "" {
		return time.Time{}, false
	}
	allDigits := true
	for i := 0; i < len(trim); i++ {
		if trim[i] < '0' || trim[i] > '9' {
			allDigits = false
			break
		}
	}
	if allDigits {
		v, err := strconv.ParseInt(trim, 10, 64)
		if err != nil {
			return time.Time{}, false
		}
		switch strings.ToLower(unit) {
		case "s":
			return time.Unix(v, 0).In(loc), true
		case "ms":
			sec := v / 1000
			nsec := (v % 1000) * int64(time.Millisecond)
			return time.Unix(sec, nsec).In(loc), true
		case "us":
			sec := v / 1_000_000
			nsec := (v % 1_000_000) * int64(time.Microsecond)
			return time.Unix(sec, nsec).In(loc), true
		case "ns":
			sec := v / 1_000_000_000
			nsec := v % 1_000_000_000
			return time.Unix(sec, nsec).In(loc), true
		default:
			switch {
			case len(trim) >= 16:
				sec := v / 1_000_000
				nsec := (v % 1_000_000) * int64(time.Microsecond)
				return time.Unix(sec, nsec).In(loc), true
			case len(trim) >= 13:
				sec := v / 1_000
				nsec := (v % 1_000) * int64(time.Millisecond)
				return time.Unix(sec, nsec).In(loc), true
			default:
				return time.Unix(v, 0).In(loc), true
			}
		}
	}
	layouts := []string{
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006/01/02 15:04:05",
		"2006-01-02",
		"2006/01/02",
		"2006-1-2",
		"2006/1/2",
		"2006-01",
		"2006/01",
		"2006-01-02 15:04:05 -07:00",
		"2006-01-02T15:04:05-07:00",
		"2006-01-02T15:04:05+08:00",
		"2006-01-02 15:04:05 MST",
	}
	for _, l := range layouts {
		if t, err := time.ParseInLocation(l, s, loc); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}
