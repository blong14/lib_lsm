# Executable
BIN := zig-out/bin
BUILD_OPTS := -Dcpu=x86_64 -Doptimize=ReleaseFast
DATA_DIR := .tmp/data
EXEC := zig-out/bin/lsm
SOURCES := $(wildcard ./src/*.zig)

# 3rd party deps
# sudo apt install libzmq5-dev
# zig fetch --global-cache-dir zig-cache --save=zzmq 'https://github.com/nine-lives-later/zzmq/archive/refs/tags/v0.2.2-zig0.12.tar.gz'
# zig fetch --global-cache-dir zig-cache --save 'https://github.com/Hejsil/zig-clap/archive/refs/tags/0.9.1.tar.gz'
# zig fetch --global-cache-dir zig-cache --save 'https://github.com/zigcc/zig-msgpack/archive/refs/tags/0.0.5.tar.gz'

.PHONY: clean test

all: build callgrind.o massif.o

fmt: $(SOURCES)
	@zig fmt .

# gdb --tui zig-out/bin/lsm
# b src/tablemap.zig:76
# r
# ipcrm -q <tab>
build: fmt
	@zig build $(BUILD_OPTS)

run: clean fmt
	zig build run-lsm -- --data_dir $(DATA_DIR) --mode singlethreaded --sst_capacity 256_000 

profile: clean build
	$(EXEC) --data_dir $(DATA_DIR) --mode multithreaded --input measurements.txt --sst_capacity 500_000 

clean:
	rm -rf $(BIN)/* callgrind.o massif.o $(DATA_DIR)/*.dat $(DATA_DIR)/data*/* 

test: $(SOURCES)
	@zig build test --summary all

poop: clean build 
	./bin/poop './$(EXEC) --data_dir ./.tmp/data/data1 --mode singlethreaded --input ./measurements.txt --sst_capacity 500_000' \
		'./$(EXEC) --data_dir ./.tmp/data/data2 --mode multithreaded --input ./measurements.txt --sst_capacity 500_000' \
		'./$(EXEC) --data_dir ./.tmp/data/data3 --mode multiprocess --input ./measurements.txt --sst_capacity 500_000' \

callgrind.o: $(EXEC)
	# kcachegrind
	valgrind --tool=callgrind --callgrind-out-file=$@ ./$(EXEC) --data_dir $(DATA_DIR)

massif.o: $(EXEC)
	# ms_print
	valgrind --tool=massif --time-unit=B --massif-out-file=$@ ./$(EXEC) --data_dir $(DATA_DIR)
