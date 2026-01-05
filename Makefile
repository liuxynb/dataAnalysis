BINARY_NAME := ana
BINARY_DIR := bin
BINARY := $(BINARY_DIR)/$(BINARY_NAME)
TOOL_BINARY := $(BINARY_DIR)/split_tool

# 默认参数
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
STRIPE_BLOCK_SIZE ?=
DATA_BLOCKS ?=
PARITY_BLOCKS ?=

# Go 相关变量
GOCMD := go
GOBUILD := $(GOCMD) build
GOCLEAN := $(GOCMD) clean
GOTEST := $(GOCMD) test
GOGET := $(GOCMD) get
GOFMT := $(GOCMD) fmt
GOVET := $(GOCMD) vet
GOTIDY := $(GOCMD) mod tidy

# 平台相关
PLATFORMS := linux darwin windows
ARCHS := amd64 arm64

.PHONY: help build build-all build-tool run exec run-tencent run-alicloud run-msrc fmt vet lint tidy test clean outclean open

# 默认目标
help:
	@echo "======================================================================"
	@echo "Build Targets:"
	@echo "  make build                构建当前平台二进制到 $(BINARY)"
	@echo "  make build-tool           构建辅助工具到 $(TOOL_BINARY)"
	@echo "  make build-all            构建多平台二进制 (Linux/macOS/Windows)"
	@echo "  make build-linux          构建 Linux 二进制"
	@echo "  make build-darwin         构建 macOS 二进制"
	@echo "  make build-windows        构建 Windows 二进制"
	@echo ""
	@echo "Run Targets:"
	@echo "  make run ...              使用 go run 直接运行"
	@echo "  make exec ...             使用已构建的二进制运行"
	@echo "  make run-tencent ...      便捷运行：provider=tencent"
	@echo "  make run-alicloud ...     便捷运行：provider=alicloud"
	@echo "  make run-msrc ...         便捷运行：provider=msrc"
	@echo ""
	@echo "Development Targets:"
	@echo "  make fmt                  格式化代码"
	@echo "  make vet                  静态检查"
	@echo "  make lint                 运行 golangci-lint (如果已安装)"
	@echo "  make tidy                 整理依赖"
	@echo "  make test                 运行测试"
	@echo "  make clean                清理构建产物"
	@echo "  make outclean             清理输出目录"
	@echo ""
	@echo "Parameters:"
	@echo "  DIR                [必须] 输入目录，支持 .csv/.gz/.tar.gz（递归）"
	@echo "  PROVIDER           [可选] alicloud|tencent|msrc，默认: $(PROVIDER)"
	@echo "  OUT_DIR            [可选] 输出目录，默认: $(OUT_DIR)"
	@echo "  WORKERS            [可选] 并发 worker 数，默认: CPU 核心数"
	@echo "  FROM, TO           [可选] 统计时间范围，格式: YYYY-MM-DD[ HH:MM[:SS]]"
	@echo "  TARGET_VOL         [可选] 指定统计条带更新的目标 Volume ID"
	@echo "  STRIPE_BLOCK_SIZE  [可选] Stripe block size (bytes), 默认 65536"
	@echo "  DATA_BLOCKS        [可选] Data blocks count, 默认 10"
	@echo "  PARITY_BLOCKS      [可选] Parity blocks count, 默认 4"
	@echo "======================================================================"
	@echo "Example:"
	@echo "  make run DIR=./data PROVIDER=tencent TARGET_VOL=vol-12345"
	@echo "======================================================================"

# 构建主程序
build:
	@echo "Building $(BINARY_NAME) for local os/arch..."
	@mkdir -p $(BINARY_DIR)
	GO111MODULE=on $(GOBUILD) -o $(BINARY) .

# 构建辅助工具
build-tool:
	@echo "Building split tool..."
	@mkdir -p $(BINARY_DIR)
	GO111MODULE=on $(GOBUILD) -o $(TOOL_BINARY) tool/split_alicloud_csv.go

# 交叉编译辅助函数
define build_platform
	@echo "Building for $(1)/$(2)..."
	@mkdir -p $(BINARY_DIR)
	GOOS=$(1) GOARCH=$(2) GO111MODULE=on $(GOBUILD) -o $(BINARY_DIR)/$(BINARY_NAME)-$(1)-$(2)$(3) .
endef

# 特定平台构建
build-linux:
	$(call build_platform,linux,amd64,)
	$(call build_platform,linux,arm64,)

build-darwin:
	$(call build_platform,darwin,amd64,)
	$(call build_platform,darwin,arm64,)

build-windows:
	$(call build_platform,windows,amd64,.exe)

# 构建所有平台
build-all: build-linux build-darwin build-windows

# 运行命令
RUN_ARGS := -d "$(DIR)" -o "$(OUT_DIR)" -provider "$(PROVIDER)"
ifneq ($(WORKERS),)
	RUN_ARGS += -w $(WORKERS)
endif
ifneq ($(FROM),)
	RUN_ARGS += -from "$(FROM)"
endif
ifneq ($(TO),)
	RUN_ARGS += -to "$(TO)"
endif
ifneq ($(QUEUE_SIZE),)
	RUN_ARGS += -queue_size $(QUEUE_SIZE)
endif
ifneq ($(MAX_LINE_MB),)
	RUN_ARGS += -max_line_mb $(MAX_LINE_MB)
endif
ifneq ($(MINUTE_BUF),)
	RUN_ARGS += -minute_buf $(MINUTE_BUF)
endif
ifneq ($(NO_MINUTE_VOLUME),)
	RUN_ARGS += -no_minute_volume
endif
ifneq ($(TARGET_VOL),)
	RUN_ARGS += -target_vol "$(TARGET_VOL)"
endif
ifneq ($(STRIPE_BLOCK_SIZE),)
	RUN_ARGS += -stripe_block_size $(STRIPE_BLOCK_SIZE)
endif
ifneq ($(DATA_BLOCKS),)
	RUN_ARGS += -data_blocks $(DATA_BLOCKS)
endif
ifneq ($(PARITY_BLOCKS),)
	RUN_ARGS += -parity_blocks $(PARITY_BLOCKS)
endif

check-dir:
	@if [ -z "$(DIR)" ]; then echo "Error: DIR is required. Usage: make run DIR=/path/to/data"; exit 1; fi

run: check-dir
	GO111MODULE=on $(GOCMD) run . $(RUN_ARGS)

exec: build check-dir
	"$(BINARY)" $(RUN_ARGS)

run-tencent:
	$(MAKE) run PROVIDER=tencent

run-alicloud:
	$(MAKE) run PROVIDER=alicloud

run-msrc:
	$(MAKE) run PROVIDER=msrc

# 开发工具
fmt:
	$(GOFMT) ./...

vet:
	$(GOVET) ./...

lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not installed. Skipping."; \
	fi

tidy:
	$(GOTIDY)

test:
	$(GOTEST) -v ./...

clean:
	@echo "Cleaning build artifacts..."
	$(GOCLEAN)
	rm -rf $(BINARY_DIR)

outclean:
	@echo "Cleaning output directory..."
	rm -rf "$(OUT_DIR)"

open:
ifeq ($(shell uname), Darwin)
	open "$(OUT_DIR)"
else
	@echo "Open command only supported on macOS"
endif
