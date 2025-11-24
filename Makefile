BINARY ?= bin/ana
PROVIDER ?= tencent
DIR ?=
OUT_DIR ?= out_dir
WORKERS ?=
FROM ?=
TO ?=
QUEUE_SIZE ?=
MAX_LINE_MB ?=
MINUTE_BUF ?=
NO_MINUTE_VOLUME ?=

.PHONY: help build run exec fmt vet tidy test clean

help:
	@echo "用法:"
	@echo "  make build                # 构建二进制到 $(BINARY)"
	@echo "  make run ...              # 使用 go run 直接运行"
	@echo "  make exec ...             # 使用已构建的二进制运行"
	@echo "  make fmt vet tidy test clean"
	@echo ""
	@echo "参数说明:"
	@echo "  DIR                输入目录，支持 .csv/.gz/.tar.gz（递归）"
	@echo "  PROVIDER           alicloud|tencent|msrc，默认: $(PROVIDER)"
	@echo "  OUT_DIR            输出目录，默认: $(OUT_DIR)"
	@echo "  WORKERS            并发 worker 数，默认: CPU 核心数"
	@echo "  FROM, TO           统计时间范围（本地时区），格式: YYYY-MM-DD[ HH:MM[:SS]] 或 RFC3339"
	@echo "  QUEUE_SIZE         读取通道缓冲大小，默认: 10000"
	@echo "  MAX_LINE_MB        单行最大字节数上限(MB)，默认: 10"
	@echo "  MINUTE_BUF         分钟级卷统计缓存上限，默认: 120"
	@echo "  NO_MINUTE_VOLUME   设为 1 禁用分钟卷统计"
	@echo ""
	@echo "示例:"
	@echo "  make run DIR=/data/alicloud PROVIDER=alicloud OUT_DIR=out FROM=\"2025-11-20 10:00\" TO=\"2025-11-20 12:00\""
	@echo "  make run DIR=/data/tencent  PROVIDER=tencent  OUT_DIR=out WORKERS=8"

build:
	mkdir -p $(dir $(BINARY))
	GO111MODULE=on go build -o $(BINARY) .

run:
	GO111MODULE=on go run . -d "$(DIR)" -o "$(OUT_DIR)" $(if $(WORKERS),-w $(WORKERS),) -provider "$(PROVIDER)" $(if $(FROM),-from "$(FROM)",) $(if $(TO),-to "$(TO)",) $(if $(QUEUE_SIZE),-queue_size $(QUEUE_SIZE),) $(if $(MAX_LINE_MB),-max_line_mb $(MAX_LINE_MB),) $(if $(MINUTE_BUF),-minute_buf $(MINUTE_BUF),) $(if $(NO_MINUTE_VOLUME),-no_minute_volume,)

exec: build
	"$(BINARY)" -d "$(DIR)" -o "$(OUT_DIR)" $(if $(WORKERS),-w $(WORKERS),) -provider "$(PROVIDER)" $(if $(FROM),-from "$(FROM)",) $(if $(TO),-to "$(TO)",) $(if $(QUEUE_SIZE),-queue_size $(QUEUE_SIZE),) $(if $(MAX_LINE_MB),-max_line_mb $(MAX_LINE_MB),) $(if $(MINUTE_BUF),-minute_buf $(MINUTE_BUF),) $(if $(NO_MINUTE_VOLUME),-no_minute_volume,)

fmt:
	go fmt ./...

vet:
	go vet ./...

tidy:
	go mod tidy

test:
	go test ./...

clean:
	rm -rf "$(BINARY)" "$(OUT_DIR)"