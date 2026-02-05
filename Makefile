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

EXEC := $(BUILD_OUT)/bin/lsmctl

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
	@echo "make scan START END - Run scan operation with custom start and end keys"
	@echo "make perf           - Run optimized profiling for Hotspot"
	@echo "make perf-detailed  - Run comprehensive profiling with more events"
	@echo "make perf-memory    - Run memory-focused profiling"
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
$(TARGET): build.zig build.zig.zon $(SOURCES)
	$(ZIG) build $(ZIG_RELEASE_OPTS) $(ZIG_COMMON_FLAGS)

# Language-specific builds
.PHONY: rust go
rust: $(SOURCES)
	$(ZIG) build $(ZIG_RELEASE_OPTS) $(ZIG_COMMON_FLAGS) rust

go: $(SOURCES)
	$(ZIG) build $(ZIG_RELEASE_OPTS) $(ZIG_COMMON_FLAGS) go

# Development targets
.PHONY: clean setup debug fmt perf write read scan test 
clean:
	@$(ZIG) build uninstall $(ZIG_COMMON_FLAGS)
	@$(GO) clean -cache -v
	@rm -rf $(BUILD_OUT) $(BUILD_CACHE)

setup:
	rm -rf .tmp/data/*

debug:
	$(ZIG) build $(ZIG_DEBUG_OPTS) lsmctl -- \
		--write \
		--input data/measurements.txt \
		--data_dir $(DATA_DIR) \
		--sst_capacity $(SST_CAPACITY)

fmt:
	@$(ZIG) build $(ZIG_COMMON_FLAGS) fmt

perf: setup
	# Optimized perf recording for Hotspot visualization
	perf record \
		--call-graph dwarf,65528 \
		--freq 997 \
		--event cycles:u,instructions:u,cache-misses:u,branch-misses:u \
		--sample-cpu \
		--timestamp \
		--running-time \
		--switch-events \
		--mmap-pages 512 \
		--output perf-lsm.data \
		$(ZIG) build $(ZIG_RELEASE_OPTS) lsmctl -- \
			--perf \
			--input data/measurements.txt \
			--data_dir $(DATA_DIR) \
			--sst_capacity $(SST_CAPACITY)
	@echo "Perf data saved to perf-lsm.data - open with: hotspot perf-lsm.data"

# Alternative comprehensive profiling with more events
perf-detailed: setup
	perf record \
		--call-graph dwarf,65528 \
		--freq 1997 \
		--event cycles:u,instructions:u,cache-references:u,cache-misses:u,branch-instructions:u,branch-misses:u,page-faults:u,context-switches:u \
		--sample-cpu \
		--timestamp \
		--running-time \
		--switch-events \
		--mmap-pages 1024 \
		--buildid-all \
		--output perf-lsm-detailed.data \
		$(ZIG) build $(ZIG_RELEASE_OPTS) lsmctl -- \
			--perf \
			--input data/measurements.txt \
			--data_dir $(DATA_DIR) \
			--sst_capacity $(SST_CAPACITY)
	@echo "Detailed perf data saved to perf-lsm-detailed.data"

# Memory-focused profiling for allocation analysis
perf-memory:
	perf record \
		--call-graph dwarf,65528 \
		--freq 997 \
		--event cycles:u,cache-misses:u,dTLB-load-misses:u,dTLB-store-misses:u \
		--sample-cpu \
		--timestamp \
		--mmap-pages 512 \
		--output perf-lsm-memory.data \
		$(ZIG) build $(ZIG_RELEASE_OPTS) lsmctl -- \
			--perf \
			--input data/measurements.txt \
			--data_dir $(DATA_DIR) \
			--sst_capacity $(SST_CAPACITY)
	@echo "Memory-focused perf data saved to perf-lsm-memory.data"

scan:
	$(ZIG) build $(ZIG_DEBUG_OPTS) lsmctl -- \
		--scan \
		--scan_start "$(word 2,$(MAKECMDGOALS))" \
		--scan_end "$(word 3,$(MAKECMDGOALS))" \
		--input data/measurements.txt \
		--data_dir $(DATA_DIR) \
		--sst_capacity $(SST_CAPACITY)

# Prevent make from interpreting the scan arguments as targets
%:
	@:
read:
	$(ZIG) build $(ZIG_RELEASE_OPTS) lsmctl -- \
		--read \
		--input data/measurements.txt \
		--data_dir $(DATA_DIR) \
		--sst_capacity $(SST_CAPACITY)

write: setup
	$(ZIG) build $(ZIG_RELEASE_OPTS) lsmctl -- \
		--write \
		--input data/measurements.txt \
		--data_dir $(DATA_DIR) \
		--sst_capacity $(SST_CAPACITY)

bench: setup
	$(ZIG) build $(ZIG_RELEASE_OPTS) lsmctl -- \
		--bench \
		--input data/measurements.txt \
		--data_dir $(DATA_DIR) \
		--sst_capacity $(SST_CAPACITY)
test:
	$(ZIG) build test $(ZIG_COMMON_FLAGS)

coverage:
	$(ZIG) build cover $(ZIG_COMMON_FLAGS)

poop: build 
	rm -rf .tmp/data/data1/* .tmp/data/data2/*
	./bin/poop \
		'./$(EXEC) --data_dir .tmp/data/data1 --bench --input data/measurements.txt --sst_capacity 1_000_000' \
		'./$(EXEC) --data_dir .tmp/data/data2 --write --input data/measurements.txt --sst_capacity 1_000_000'

massif.o: setup $(EXEC)
	# ms_print
	valgrind --tool=massif --time-unit=B --massif-out-file=$@ \
		./$(EXEC) --bench --data_dir $(DATA_DIR) --input data/measurements.txt

callgrind.o: setup $(EXEC)
	# kcachegrind
	valgrind --tool=callgrind --callgrind-out-file=$@ \
		./$(EXEC) --bench --data_dir $(DATA_DIR) --input data/measurements.txt
# Debug notes:
# gdb --tui zig-out/bin/lsm
# b src/tablemap.zig:76
# r
# ipcrm -q <tab>

