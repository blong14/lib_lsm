# Executable
BIN := zig-out/bin
BUILD_OPTS := -Dcpu=x86_64 -Doptimize=ReleaseFast
EXEC := zig-out/bin/lsm
SOURCES := $(wildcard ./src/*.zig)

# 3rd party deps
# sudo apt install libzmq5-dev
# zig fetch --global-cache-dir zig-cache --save=zzmq 'https://github.com/nine-lives-later/zzmq/archive/refs/tags/v0.2.2-zig0.12.tar.gz'

.PHONY: clean test

all: build callgrind.o massif.o

# gdb --tui zig-out/bin/lsm
# b src/tablemap.zig:76
# r
# ipcrm -q <tab>
build: $(SOURCES)
	@zig fmt .
	@zig build $(BUILD_OPTS)

clean:
	rm -f $(BIN)/* callgrind.o massif.o data/*.dat

test: $(SOURCES)
	@zig build test --summary all

callgrind.o: $(EXEC)
	# kcachegrind
	valgrind --tool=callgrind --callgrind-out-file=$@ ./$(EXEC)

massif.o: $(EXEC)
	# ms_print
	valgrind --tool=massif --time-unit=B --massif-out-file=$@ ./$(EXEC)
