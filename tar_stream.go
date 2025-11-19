package main

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"strings"
)

var scannerMaxBytes = 10 * 1024 * 1024
func SetMaxLineBytes(n int) { if n > 0 { scannerMaxBytes = n } }

func streamLinesFromTarGz(path string, lineCh chan<- string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	gzr, err := gzip.NewReader(f)
	if err != nil {
		return 0, err
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)
	var n uint64
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return n, err
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}
		fmt.Printf("正在处理 tar 内文件: %s (size=%d)\n", header.Name, header.Size)
		scanner := bufio.NewScanner(tr)
		buf := make([]byte, 1024*1024)
		scanner.Buffer(buf, scannerMaxBytes)

		for scanner.Scan() {
			line := scanner.Text()
			if len(strings.TrimSpace(line)) == 0 {
				continue
			}
			lineCh <- line
			n++
		}
		if err := scanner.Err(); err != nil {
			fmt.Printf("扫描文件 %s 错误: %v\n", header.Name, err)
		}
	}
	return n, nil
}

func streamLinesFromPlainGz(path string, lineCh chan<- string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	gzr, err := gzip.NewReader(f)
	if err != nil {
		return 0, err
	}
	defer gzr.Close()

	scanner := bufio.NewScanner(gzr)
	buf := make([]byte, 1024*1024)
	scanner.Buffer(buf, scannerMaxBytes)

	var n uint64
	for scanner.Scan() {
		line := scanner.Text()
		if len(strings.TrimSpace(line)) == 0 {
			continue
		}
		lineCh <- line
		n++
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("扫描文件 %s 错误: %v\n", path, err)
	}
	return n, nil
}

func streamLinesFromGzAuto(path string, lineCh chan<- string) (uint64, error) {
	if strings.HasSuffix(path, ".tar.gz") || strings.HasSuffix(path, ".tgz") {
		return streamLinesFromTarGz(path, lineCh)
	}
	return streamLinesFromPlainGz(path, lineCh)
}
