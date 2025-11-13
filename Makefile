BINARY ?= bin/ana
PROVIDER ?= tencent
DIR ?=
OUT_DIR ?= out_dir
WORKERS ?=

.PHONY: help build run exec fmt vet tidy test clean

help:
	@echo "用法:"
	@echo "  make build"
	@echo "  make run DIR=/path/to/gz PROVIDER=tencent OUT_DIR=out_dir [WORKERS=N]"
	@echo "  make exec DIR=/path/to/gz PROVIDER=tencent OUT_DIR=out_dir [WORKERS=N]"
	@echo "  make fmt vet tidy test clean"

build:
	mkdir -p $(dir $(BINARY))
	GO111MODULE=on go build -o $(BINARY) .

run:
	GO111MODULE=on go run . -d "$(DIR)" -o "$(OUT_DIR)" $(if $(WORKERS),-w $(WORKERS),) -provider "$(PROVIDER)"

exec: build
	"$(BINARY)" -d "$(DIR)" -o "$(OUT_DIR)" $(if $(WORKERS),-w $(WORKERS),) -provider "$(PROVIDER)"

fmt:
	go fmt ./...

vet:
	go vet ./...

tidy:
	go mod tidy

test:
	go test ./...

clean:
	rm -rf "$(BINARY)"