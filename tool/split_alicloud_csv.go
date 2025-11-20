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
	"bytes"
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
	memlimit := flag.Int("memlimit", 64<<20, "memory budget for HDD batching, bytes")
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

	dIdx := parseDateIndexArg(*dateCol)
	if dIdx < 0 {
		fmt.Fprintln(os.Stderr, "invalid -date")
		os.Exit(1)
	}

	memBufs := map[string]*bytes.Buffer{}
	dayCreated := map[string]bool{}
	daySizes := map[string]int{}
	lastUsed := map[string]int64{}
	days := map[string]struct{}{}
	var tick int64
	var memUsed int

	var total, written, skipped int

	for {
		line, err := br.ReadBytes('\n')
		if err == io.EOF {
			if len(line) == 0 {
				break
			}
		} else if err != nil {
			skipped++
			continue
		}
		if len(line) == 0 {
			continue
		}
		total++
		dsb := nthField(line, dIdx)
		if len(dsb) == 0 {
			skipped++
			continue
		}
		t, ok := parseDate(string(dsb), *format, *epochUnit, loc)
		if !ok {
			skipped++
			continue
		}
		down := t.In(loc).Format("2006-01-02")
		days[down] = struct{}{}
		b := memBufs[down]
		if b == nil {
			nb := &bytes.Buffer{}
			memBufs[down] = nb
			b = nb
		}
		prev := b.Len()
		b.Write(bytes.TrimRight(line, "\r\n"))
		b.WriteByte('\n')
		delta := b.Len() - prev
		memUsed += delta
		daySizes[down] = b.Len()
		lastUsed[down] = tick
		tick++
		written++

		if memUsed > *memlimit {
			var target string
			var maxSize int
			for k, sz := range daySizes {
				if sz > maxSize {
					maxSize = sz
					target = k
				}
			}
			if target == "" {
				var minTick int64 = 1 << 62
				for k, v := range lastUsed {
					if v < minTick {
						minTick = v
						target = k
					}
				}
			}
			if target != "" && memBufs[target].Len() > 0 {
				p := filepath.Join(*out, fmt.Sprintf("%s-%s.csv", *prefix, target))
				var of *os.File
				if *force && !dayCreated[target] {
					of, err = os.Create(p)
					if err != nil {
						skipped++
						continue
					}
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

					}
				}
				bw := bufio.NewWriterSize(of, *bufsize)
				if _, err := bw.Write(memBufs[target].Bytes()); err != nil {
					bw.Flush()
					of.Close()
					skipped++
					continue
				}
				bw.Flush()
				of.Close()
				memUsed -= memBufs[target].Len()
				memBufs[target].Reset()
				daySizes[target] = 0
				dayCreated[target] = true
			}
		}
	}

	for day, buf := range memBufs {
		if buf.Len() == 0 {
			continue
		}
		p := filepath.Join(*out, fmt.Sprintf("%s-%s.csv", *prefix, day))
		var of *os.File
		if *force && !dayCreated[day] {
			of, err = os.Create(p)
			if err != nil {
				skipped++
				continue
			}
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
			}
		}
		bw := bufio.NewWriterSize(of, *bufsize)
		if _, err := bw.Write(buf.Bytes()); err != nil {
			bw.Flush()
			of.Close()
			skipped++
			continue
		}
		bw.Flush()
		of.Close()
	}
	fmt.Fprintf(os.Stdout, "rows=%d written=%d skipped=%d days=%d\n", total, written, skipped, len(days))
}

func parseDateIndexArg(arg string) int {
	if strings.TrimSpace(arg) == "" {
		return 4
	}
	if i, err := strconv.Atoi(strings.TrimSpace(arg)); err == nil && i >= 0 {
		return i
	}
	return 4
}

func nthField(line []byte, idx int) []byte {
	start := 0
	pos := 0
	for i, c := range line {
		if c == ',' {
			if pos == idx {
				return bytes.TrimSpace(line[start:i])
			}
			pos++
			start = i + 1
		}
	}
	if pos == idx {
		return bytes.TrimSpace(line[start:])
	}
	return nil
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
