# Executable
EXEC = zig-out/bin/lsm

.PHONY: clean

build:
	zig build run-lsm

clean:
	rm -f $(EXEC) callgrind.out.* massif.out.*

callgrind: $(EXEC)
	# kcachegrind
	valgrind --tool=callgrind ./$(EXEC)

massif: $(EXEC)
	# ms_print
	valgrind --tool=massif --time-unit=B ./$(EXEC)
