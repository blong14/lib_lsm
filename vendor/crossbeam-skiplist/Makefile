# curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# zig test -lc -I vendor/crossbeam-skiplist/src -L vendor/crossbeam-skiplist/target/release vendor/crossbeam-skiplist/target/release/libconcurrent_skiplist.so src/skiplist.zig
#
# cbindgen --config cbindgen.toml --crate concurrent-skiplist --output src/skiplist.h
#

src/skiplist.h: src/lib.rs
	cbindgen --config cbindgen.toml --crate concurrent-skiplist --output src/skiplist.h

debug: src/skiplist.h
	cargo build

build: src/skiplist.h
	cargo build --release
