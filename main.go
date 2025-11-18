package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"ana/providers/alicloud"
	"ana/providers/tencent"
)

func listGzFiles(dir string) ([]string, error) {
	var paths []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			ext := filepath.Ext(path)
			if ext == ".gz" || ext == ".tgz" || strings.HasSuffix(path, ".tar.gz") {
				paths = append(paths, path)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return paths, nil
}

func main() {
	// CLI flags
	dir := flag.String("d", "", "directory containing .gz trace files (recursive)")
	outDir := flag.String("o", "output", "output directory")
	workers := flag.Int("w", 0, "number of parser workers (default: numCPU)")
	provider := flag.String("provider", "", "trace provider: alicloud|tencent")
	flag.Parse()

	if *dir == "" {
		fmt.Println("请使用 -d 指定包含 .gz 的目录")
		os.Exit(1)
	}
	if *workers <= 0 {
		*workers = runtime.NumCPU()
	}
	if strings.ToLower(*provider) != "alicloud" && strings.ToLower(*provider) != "tencent" {
		fmt.Println("请使用 -provider 指定 alicloud 或 tencent")
		os.Exit(1)
	}

	if err := os.MkdirAll(*outDir, 0755); err != nil {
		fmt.Printf("创建输出目录失败: %v\n", err)
		os.Exit(1)
	}

	paths, err := listGzFiles(*dir)
	if err != nil {
		fmt.Printf("遍历目录失败: %v\n", err)
		os.Exit(1)
	}
	if len(paths) == 0 {
		fmt.Println("目录内未找到 .gz 文件")
		os.Exit(1)
	}
	fmt.Printf("文件数: %d\n输出目录: %s\n并发 worker: %d\n", len(paths), *outDir, *workers)

	// channel for raw lines
	lineCh := make(chan string, 10000)
	var wg sync.WaitGroup
	agg := NewAggregator()

	var totalParsed uint64
	var parseErrCount uint64

	// choose provider parser
	var p Parser
	switch strings.ToLower(*provider) {
	case "alicloud":
		p = alicloud.NewParser()
	default:
		p = tencent.NewParser()
	}

	// start workers
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			parserWorker(lineCh, p, agg, &totalParsed, &parseErrCount)
		}(i)
	}

	// producer: read tar.gz and stream lines into lineCh
	for _, p := range paths {
		prev := atomic.LoadUint64(&totalParsed) + atomic.LoadUint64(&parseErrCount)
		cnt, err := streamLinesFromGzAuto(p, lineCh)
		if err != nil {
			fmt.Printf("读取 trace 文件失败: %v\n", err)
			close(lineCh)
			wg.Wait()
			os.Exit(1)
		}
		for {
			if atomic.LoadUint64(&totalParsed)+atomic.LoadUint64(&parseErrCount) >= prev+cnt {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if err := writeDayCSV(filepath.Join(*outDir, "time_stats_day.csv"), agg); err != nil {
			fmt.Printf("写 day CSV 失败: %v\n", err)
		}
		if err := writeHourCSV(filepath.Join(*outDir, "time_stats_hour.csv"), agg); err != nil {
			fmt.Printf("写 hour CSV 失败: %v\n", err)
		}
		if err := writeMinuteCSV(filepath.Join(*outDir, "time_stats_minute.csv"), agg); err != nil {
			fmt.Printf("写 minute CSV 失败: %v\n", err)
		}
		if err := writeVolumeByMinuteDir(filepath.Join(*outDir, "volume_stats_minute"), agg); err != nil {
			fmt.Printf("写 volume-by-minute 失败: %v\n", err)
		}
	}

	// close channel and wait workers
	close(lineCh)
	wg.Wait()

	fmt.Printf("解析完成。成功解析行数(估计): %d，解析错误(估计): %d\n",
		atomic.LoadUint64(&totalParsed), atomic.LoadUint64(&parseErrCount))

	// 写出 CSV 文件
	if err := writeDayCSV(filepath.Join(*outDir, "time_stats_day.csv"), agg); err != nil {
		fmt.Printf("写 day CSV 失败: %v\n", err)
	}
	if err := writeHourCSV(filepath.Join(*outDir, "time_stats_hour.csv"), agg); err != nil {
		fmt.Printf("写 hour CSV 失败: %v\n", err)
	}
	if err := writeMinuteCSV(filepath.Join(*outDir, "time_stats_minute.csv"), agg); err != nil {
		fmt.Printf("写 minute CSV 失败: %v\n", err)
	}
	if err := writeVolumeByMinuteDir(filepath.Join(*outDir, "volume_stats_minute"), agg); err != nil {
		fmt.Printf("写 volume-by-minute 失败: %v\n", err)
	}

	// 输出 top volumes
	printTopVolumes(agg, 10)
	fmt.Println("全部完成。")
}
