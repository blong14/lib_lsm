# Executable
EXEC = zig-out/bin/lsm

# Valgrind options
VALGRIND_OPTIONS = --tool=callgrind

.PHONY: clean valgrind

clean:
	rm -f $(EXEC) callgrind.out.*

valgrind: $(EXEC)
	# kcachegrind
	valgrind $(VALGRIND_OPTIONS) ./$(EXEC)
