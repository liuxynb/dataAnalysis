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
	"strings"
	"time"
)

func main() {
	in := flag.String("in", "", "input csv path")
	out := flag.String("out", "", "output directory")
	dateCol := flag.String("date", "", "date column name")
	format := flag.String("format", "", "date layout")
	locName := flag.String("loc", "Asia/Shanghai", "time zone, e.g. Asia/Shanghai or UTC")
	prefix := flag.String("prefix", "", "output file prefix")
	force := flag.Bool("force", false, "overwrite output files")
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

	r := csv.NewReader(bufio.NewReader(f))
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
		t, ok := parseDate(ds, *format, loc)
		if !ok {
			skipped++
			continue
		}
		month := fmt.Sprintf("%04d-%02d", t.Year(), int(t.Month()))
		w := writers[month]
		if w == nil {
			p := filepath.Join(*out, fmt.Sprintf("%s-%s.csv", *prefix, month))
			var of *os.File
			var created bool
			if *force {
				of, err = os.Create(p)
				if err != nil {
					skipped++
					continue
				}
				created = true
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
			files[month] = of
			w = csv.NewWriter(of)
			writers[month] = w
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
		written++
	}

	for _, w := range writers {
		w.Flush()
	}
	for _, f := range files {
		f.Close()
	}
	fmt.Fprintf(os.Stdout, "rows=%d written=%d skipped=%d files=%d\n", total, written, skipped, len(writers))
}

func findDateIndex(header []string, name string) int {
	if name != "" {
		for i, h := range header {
			if strings.EqualFold(strings.TrimSpace(h), strings.TrimSpace(name)) {
				return i
			}
		}
		return -1
	}
	candidates := []string{
		"BillingTime", "BillingDate", "PayTime", "Time", "Date", "UsageStartTime",
		"StartTime", "CreateTime", "OrderCreateTime", "InstanceCreateTime", "PaymentTime",
	}
	lhdr := make([]string, len(header))
	for i := range header {
		lhdr[i] = strings.ToLower(strings.TrimSpace(header[i]))
	}
	for _, c := range candidates {
		lc := strings.ToLower(c)
		for i, h := range lhdr {
			if h == lc {
				return i
			}
		}
	}
	return -1
}

func parseDate(s string, layout string, loc *time.Location) (time.Time, bool) {
	if layout != "" {
		if t, err := time.ParseInLocation(layout, s, loc); err == nil {
			return t, true
		}
		return time.Time{}, false
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
