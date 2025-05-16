# Makefile for lib_lsm project

GO := $(shell which go) 
ZIG := $(shell which zig)

# Source files
SOURCES := $(wildcard ./src/*)
GO_MAIN := src/main.go

# Build configuration
BUILD_CACHE := .zig-cache
BUILD_OUT := zig-out
TARGET := $(BUILD_OUT)/lib/liblib_lsm.a

# Build options
ZIG_COMMON_FLAGS := --summary all --verbose
ZIG_RELEASE_OPTS := -Dcpu=x86_64 -Doptimize=ReleaseFast
ZIG_DEBUG_OPTS := -Dcpu=x86_64 -Doptimize=Debug

# Runtime options
DATA_DIR := /home/blong14/Developer/git/lib_lsm/.tmp/data
MODE := singlethreaded
# MODE := multithreaded
SST_CAPACITY := 1000000

# Help command
.PHONY: help
help:
	@echo "lib_lsm Makefile Usage:"
	@echo "======================="
	@echo "make                - Build the project"
	@echo "make build          - Build the project (same as default)"
	@echo "make rust           - Build Rust bindings"
	@echo "make go             - Build Go bindings"
	@echo "make clean          - Remove build artifacts"
	@echo "make bench          - Run benchmarks"
	@echo "make fmt            - Format code"
	@echo "make test           - Run tests"
	@echo "make run            - Run lsmctl with default options"
	@echo "make profile        - Run with profiling enabled"
	@echo "make debug          - Run in debug mode"
	@echo "make help           - Display this help message"

# Default target
.PHONY: all
all: build

# Main build target
.PHONY: build
build: $(TARGET)
	@echo "Build finished"

# Library target
$(TARGET): $(SOURCES)
	$(ZIG) build $(ZIG_RELEASE_OPTS) $(ZIG_COMMON_FLAGS)

# Language-specific builds
.PHONY: rust go
rust: $(SOURCES)
	$(ZIG) build $(ZIG_RELEASE_OPTS) $(ZIG_COMMON_FLAGS) rust

go: $(SOURCES)
	$(ZIG) build $(ZIG_RELEASE_OPTS) $(ZIG_COMMON_FLAGS) go

# Development targets
.PHONY: clean debug fmt perf profile run test 
clean:
	@$(ZIG) build uninstall $(ZIG_COMMON_FLAGS)
	@$(GO) clean -cache -v
	@rm -rf $(BUILD_OUT) $(BUILD_CACHE)

debug:
	$(ZIG) build $(ZIG_DEBUG_OPTS) lsmctl -- \
		--mode $(MODE) \
		--data_dir $(DATA_DIR)

fmt:
	@$(ZIG) build $(ZIG_COMMON_FLAGS) fmt

perf:
	perf record -F 200 -g $(ZIG) build $(ZIG_DEBUG_OPTS) xlsm -- \
		--input data/measurements.txt \
		--data_dir $(DATA_DIR) \
		--sst_capacity $(SST_CAPACITY)
	perf script --input=perf.data -F +pid > perf.processed.data

profile:
	$(ZIG) build $(ZIG_RELEASE_OPTS) xlsm -- \
		--mode $(MODE) \
		--input data/measurements.txt \
		--data_dir $(DATA_DIR) \
		--sst_capacity $(SST_CAPACITY)

run:
	$(ZIG) build $(ZIG_RELEASE_OPTS) lsmctl -- \
		--data_dir $(DATA_DIR) \
		--sst_capacity $(SST_CAPACITY)

bench:
	rm -rf .tmp/data/*.dat
	$(ZIG) build $(ZIG_RELEASE_OPTS) lsmctl -- \
		--bench \
		--data_dir $(DATA_DIR) \
		--sst_capacity $(SST_CAPACITY)
test:
	$(ZIG) build test $(ZIG_COMMON_FLAGS)

# Debug notes:
# gdb --tui zig-out/bin/lsm
# b src/tablemap.zig:76
# r
# ipcrm -q <tab>

