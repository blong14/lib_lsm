# Executable
BIN := zig-out/bin
BUILD_OPTS := -Dcpu=x86_64 -Doptimize=ReleaseFast
DEBUG_BUILD_OPTS := -Dcpu=x86_64 -Doptimize=Debug
DATA_DIR := /home/blong14/Developer/git/lib_lsm/.tmp/data
EXEC := zig-out/bin/xlsm
MODE := singlethreaded
# MODE := multithreaded
SOURCES := $(wildcard ./src/*)
GO := /home/blong14/sdk/go1.22/bin/go
ZIG := bin/zig-linux-x86_64-0.13.0/zig
GOEXEC := zig-out/bin/gopg 

# 3rd party deps
# sudo apt install linux-tools-common
# sudo apt install linux-tools-generic
# sudo apt install libzmq5-dev
# sudo apt install -y libjemalloc-dev
# zig fetch --global-cache-dir zig-cache --save=jemalloc https://github.com/jiacai2050/zig-jemalloc/archive/1b893cdfccee2c1f4cc76158561b1a0ef54ef622.tar.gz
# zig fetch --global-cache-dir zig-cache --save=zzmq 'https://github.com/nine-lives-later/zzmq/archive/refs/tags/v0.2.2-zig0.12.tar.gz'
# zig fetch --global-cache-dir zig-cache --save 'https://github.com/Hejsil/zig-clap/archive/refs/tags/0.9.1.tar.gz'
# zig fetch --global-cache-dir zig-cache --save 'https://github.com/zigcc/zig-msgpack/archive/refs/tags/0.0.5.tar.gz'
# zig fetch --global-cache-dir zig-cache --save https://github.com/almmiko/btree.c-zig/archive/fc0d08558b5104991ae43c04d7b7c10a4be49aa7.tar.gz
# curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# zig test -lc -I vendor/crossbeam-skiplist/src -L vendor/crossbeam-skiplist/target/release vendor/crossbeam-skiplist/target/release/libconcurrent_skiplist.so src/skiplist.zig

.PHONY: clean test

all: build 

fmt: $(SOURCES)
	$(ZIG) fmt src
	$(GO) fmt ./src/...

# gdb --tui zig-out/bin/lsm
# b src/tablemap.zig:76
# r
# ipcrm -q <tab>
debug-build: clean fmt target/release/libconcurrent_skiplist.so
	$(ZIG) build $(DEBUG_BUILD_OPTS)

perf: debug-build
	perf record -F 200 -g $(EXEC) --mode $(MODE) --data_dir $(DATA_DIR)
	perf script --input=perf.data -F +pid > perf.processed.data

run: fmt target/release/libconcurrent_skiplist.so
	$(ZIG) build lsm -- --data_dir $(DATA_DIR) --sst_capacity 1_000_000 

profile: clean build
	$(EXEC) --mode $(MODE) --input measurements.txt --data_dir $(DATA_DIR) --sst_capacity 1_000_000 

clean:
	rm -rf $(BIN)/* callgrind.o massif.o .zig-cache 
	$(ZIG) build uninstall
	$(GO) clean -cache

test: $(SOURCES)
	$(ZIG) build test --summary all

poop: clean build 
	./bin/poop './$(EXEC) --data_dir ./.tmp/data/data1 --mode singlethreaded --input ./measurements.txt --sst_capacity 1_000_000' \
		'./$(EXEC) --data_dir ./.tmp/data/data2 --mode multithreaded --input ./measurements.txt --sst_capacity 1_000_000' \
		# './$(EXEC) --data_dir ./.tmp/data/data3 --mode multiprocess --input ./measurements.txt --sst_capacity 500_000' \

callgrind.o: $(EXEC)
	# kcachegrind
	valgrind --tool=callgrind --callgrind-out-file=$@ ./$(EXEC) --data_dir $(DATA_DIR)

massif.o: $(EXEC)
	# ms_print
	valgrind --tool=massif --time-unit=B --massif-out-file=$@ ./$(EXEC) --data_dir $(DATA_DIR)

include/skiplist.h: src/lib.rs
	cbindgen --config cbindgen.toml --crate concurrent-skiplist --output include/skiplist.h

target/release/libconcurrent_skiplist.so: include/skiplist.h
	# https://nnethercote.github.io/perf-book/build-configuration.html
	MALLOC_CONF="thp:always,metadata_thp:always" cargo build --release

zig-out/lib/liblib_lsm.a: fmt target/release/libconcurrent_skiplist.so
	$(ZIG) build $(BUILD_OPTS)

build: zig-out/lib/liblib_lsm.a src/main.go
	go build -o zig-out/bin/gopg src/main.go

