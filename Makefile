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
TARGET_VOL ?=

.PHONY: help build run exec run-tencent run-alicloud run-msrc fmt vet tidy test clean outclean open

help:
	@echo "用法:"
	@echo "  make build                # 构建二进制到 $(BINARY)"
	@echo "  make run ...              # 使用 go run 直接运行"
	@echo "  make exec ...             # 使用已构建的二进制运行"
	@echo "  make run-tencent ...      # 便捷：provider=tencent"
	@echo "  make run-alicloud ...     # 便捷：provider=alicloud"
	@echo "  make run-msrc ...         # 便捷：provider=msrc"
	@echo "  make open                 # 打开输出目录（macOS）"
	@echo "  make outclean             # 仅清理输出目录"
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
	@echo "  TARGET_VOL         指定统计条带更新的目标 Volume ID"
	@echo "  注意: 必须设置 DIR（否则 run/exec 会报错）"
	@echo ""
	@echo "示例:"
	@echo "  make run DIR=/data/alicloud PROVIDER=alicloud OUT_DIR=out FROM=\"2025-11-20 10:00\" TO=\"2025-11-20 12:00\""
	@echo "  make run DIR=/data/tencent  PROVIDER=tencent  OUT_DIR=out WORKERS=8"

build:
	mkdir -p $(dir $(BINARY))
	GO111MODULE=on go build -o $(BINARY) .

run:
	@if [ -z "$(DIR)" ]; then echo "错误: 需要设置 DIR，例如 DIR=/path/to/traces"; exit 1; fi
	GO111MODULE=on go run . -d "$(DIR)" -o "$(OUT_DIR)" $(if $(WORKERS),-w $(WORKERS),) -provider "$(PROVIDER)" $(if $(FROM),-from "$(FROM)",) $(if $(TO),-to "$(TO)",) $(if $(QUEUE_SIZE),-queue_size $(QUEUE_SIZE),) $(if $(MAX_LINE_MB),-max_line_mb $(MAX_LINE_MB),) $(if $(MINUTE_BUF),-minute_buf $(MINUTE_BUF),) $(if $(NO_MINUTE_VOLUME),-no_minute_volume,) $(if $(TARGET_VOL),-target_vol $(TARGET_VOL),)

exec: build
	@if [ -z "$(DIR)" ]; then echo "错误: 需要设置 DIR，例如 DIR=/path/to/traces"; exit 1; fi
	"$(BINARY)" -d "$(DIR)" -o "$(OUT_DIR)" $(if $(WORKERS),-w $(WORKERS),) -provider "$(PROVIDER)" $(if $(FROM),-from "$(FROM)",) $(if $(TO),-to "$(TO)",) $(if $(QUEUE_SIZE),-queue_size $(QUEUE_SIZE),) $(if $(MAX_LINE_MB),-max_line_mb $(MAX_LINE_MB),) $(if $(MINUTE_BUF),-minute_buf $(MINUTE_BUF),) $(if $(NO_MINUTE_VOLUME),-no_minute_volume,) $(if $(TARGET_VOL),-target_vol $(TARGET_VOL),)

run-tencent:
	$(MAKE) run PROVIDER=tencent

run-alicloud:
	$(MAKE) run PROVIDER=alicloud

run-msrc:
	$(MAKE) run PROVIDER=msrc

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

outclean:
	rm -rf "$(OUT_DIR)"

open:
	open "$(OUT_DIR)"
