# Executable
BUILD_OPTS = -Dcpu=x86_64 -Doptimize=ReleaseFast
EXEC = zig-out/bin/lsm

.PHONY: clean

build:
	# gdb --tui zig-out/bin/lsm
	# b src/tablemap.zig:76
	# r
	# ipcrm -q <tab>
	zig build $(BUILD_OPTS) run-lsm

clean:
	rm -f $(EXEC) callgrind.out.* massif.out.*

callgrind: $(EXEC)
	# kcachegrind
	valgrind --tool=callgrind ./$(EXEC)

massif: $(EXEC)
	# ms_print
	valgrind --tool=massif --time-unit=B ./$(EXEC)
