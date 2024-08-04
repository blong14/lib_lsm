# Executable
BIN := zig-out/bin
BUILD_OPTS := -Dcpu=x86_64 -Doptimize=ReleaseFast
EXEC := zig-out/bin/lsm
SOURCES := $(wildcard ./src/*.zig)

# 3rd party deps
# sudo apt install libzmq5-dev
# zig fetch --global-cache-dir zig-cache --save=zzmq 'https://github.com/nine-lives-later/zzmq/archive/refs/tags/v0.2.2-zig0.12.tar.gz'
# zig fetch --global-cache-dir zig-cache --save 'https://github.com/Hejsil/zig-clap/archive/refs/tags/0.9.1.tar.gz'

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

run: fmt
	@zig build run-lsm -- --input small.csv 

profile: clean build
	$(EXEC) --data_dir data --input trips.txt --sst_capacity 256_000 

clean:
	rm -f $(BIN)/* callgrind.o massif.o data/*.dat

test: $(SOURCES)
	@zig build test --summary all

poop: $(EXEC)
	./bin/poop './$(EXEC) --input ./trips.txt --sst_capacity 128_000' \
		'./$(EXEC) --input ./trips.txt --sst_capacity 256_000' \
		'./$(EXEC) --input ./trips.txt --sst_capacity 512_000' \
		'./$(EXEC) --input ./trips.txt --sst_capacity 1_024_000'

callgrind.o: $(EXEC)
	# kcachegrind
	valgrind --tool=callgrind --callgrind-out-file=$@ ./$(EXEC)

massif.o: $(EXEC)
	# ms_print
	valgrind --tool=massif --time-unit=B --massif-out-file=$@ ./$(EXEC)
