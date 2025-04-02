SOURCES := $(wildcard ./src/*)
GO := /home/blong14/sdk/go1.22/bin/go
GO_MAIN := src/main.go
ZIG := bin/zig-linux-x86_64-0.13.0/zig

# Executable
BUILD_CACHE := .zig-cache
BUILD_OUT := zig-out
BUILD_OPTS := -Dcpu=x86_64 -Doptimize=ReleaseFast
BUILD_FLAGS := --summary all --verbose
DEBUG_BUILD_OPTS := -Dcpu=x86_64 -Doptimize=Debug

# LSM options
DATA_DIR := /home/blong14/Developer/git/lib_lsm/.tmp/data
MODE := singlethreaded
# MODE := multithreaded

all: build 

clean:
	@rm -rf $(BUILD_OUT)/bin/* $(BUILD_OUT)/lib/* $(BUILD_OUT)/include/* $(BUILD_CACHE) 
	@$(ZIG) build uninstall $(BUILD_FLAGS) 
	@$(GO) clean -cache -v

fmt: $(SOURCES)
	@$(ZIG) build $(BUILD_FLAGS) fmt 

build: 
	$(ZIG) build $(BUILD_OPTS) $(BUILD_FLAGS) 

go: 
	@$(ZIG) build $(BUILD_OPTS) go $(BUILD_FLAGS)
	$(GO) run $(GO_MAIN) 

test: 
	$(ZIG) build test $(BUILD_FLAGS) 

run: 
	$(ZIG) build $(BUILD_OPTS) lsmctl -- --data_dir $(DATA_DIR) --sst_capacity 1_000_000 

profile: 
	$(ZIG) build $(BUILD_OPTS) xlsm -- --mode $(MODE) --input data/measurements.txt --data_dir $(DATA_DIR) --sst_capacity 1_000_000 

debug: 
# gdb --tui zig-out/bin/lsm
# b src/tablemap.zig:76
# r
# ipcrm -q <tab>
	$(ZIG) build $(DEBUG_BUILD_OPTS) xlsm -- --mode $(MODE) --data_dir $(DATA_DIR) 

