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

func streamLinesFromTarGz(path string, lineCh chan<- string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	gzr, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if header.Typeflag != tar.TypeReg {
			// skip directories or non-regular files
			continue
		}
		fmt.Printf("正在处理 tar 内文件: %s (size=%d)\n", header.Name, header.Size)
		// Use bufio.Scanner for line-by-line reading
		scanner := bufio.NewScanner(tr)
		// 增大缓冲以防行很长
		const maxCapacity = 10 * 1024 * 1024
		buf := make([]byte, 1024*1024)
		scanner.Buffer(buf, maxCapacity)

		for scanner.Scan() {
			line := scanner.Text()
			// skip empty lines
			if len(strings.TrimSpace(line)) == 0 {
				continue
			}
			// send to workers
			lineCh <- line
		}
		if err := scanner.Err(); err != nil {
			fmt.Printf("扫描文件 %s 错误: %v\n", header.Name, err)
		}
	}
	return nil
}

func streamLinesFromPlainGz(path string, lineCh chan<- string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	gzr, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gzr.Close()

	scanner := bufio.NewScanner(gzr)
	const maxCapacity = 10 * 1024 * 1024
	buf := make([]byte, 1024*1024)
	scanner.Buffer(buf, maxCapacity)

	for scanner.Scan() {
		line := scanner.Text()
		if len(strings.TrimSpace(line)) == 0 {
			continue
		}
		lineCh <- line
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("扫描文件 %s 错误: %v\n", path, err)
	}
	return nil
}

func streamLinesFromGzAuto(path string, lineCh chan<- string) error {
	if strings.HasSuffix(path, ".tar.gz") || strings.HasSuffix(path, ".tgz") {
		return streamLinesFromTarGz(path, lineCh)
	}
	return streamLinesFromPlainGz(path, lineCh)
}
