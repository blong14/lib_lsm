# Executable
BUILD_OPTS := -Dcpu=x86_64 -Doptimize=ReleaseFast
EXEC := zig-out/bin/lsm

.PHONY: clean

all: build callgrind.o massif.o

build: $(wildcard ./src/*.zig)
	# gdb --tui zig-out/bin/lsm
	# b src/tablemap.zig:76
	# r
	# ipcrm -q <tab>
	zig build $(BUILD_OPTS) run-lsm

clean:
	rm -f $(EXEC) callgrind.o massif.o

callgrind.o: $(EXEC)
	# kcachegrind
	valgrind --tool=callgrind --callgrind-out-file=$@ ./$(EXEC)

massif.o: $(EXEC)
	# ms_print
	valgrind --tool=massif --time-unit=B --massif-out-file=$@ ./$(EXEC)
