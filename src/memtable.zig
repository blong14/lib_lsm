const std = @import("std");

const sst = @import("sstable.zig");
const log = @import("wal.zig");
const options = @import("opts.zig");
const tbm = @import("tablemap.zig");

const Allocator = std.mem.Allocator;
const Opts = options.Opts;
const SSTable = sst.SSTable;
const TableMap = tbm.TableMap;

pub fn Memtable(comptime K: type, comptime V: type) type {
    return struct {
        alloc: Allocator,
        hash_map: *TableMap,
        wal: *log.WAL,

        const Self = @This();

        const Error = error{
            Full,
        };

        pub fn init(alloc: Allocator, opts: Opts) !*Self {
            const filename = try std.fmt.allocPrint(
            alloc, "{s}/{s}", .{ opts.data_dir, "wal.dat" });
            defer alloc.free(filename);
            var wal = try log.WAL.init(alloc, filename, opts.wal_capacity);
            var map = try TableMap.init(alloc);
            var mtable = try alloc.create(Self);
            mtable.* = .{ .alloc = alloc, .hash_map = map, .wal = wal };
            return mtable;
        }

        pub fn deinit(self: *Self) void {
            self.hash_map.deinit();
            self.alloc.destroy(self.hash_map);
            self.wal.deinit();
            self.alloc.destroy(self.wal);
            self.* = undefined;
        }

        pub fn put(self: *Self, key: K, value: V) !void {
            try self.hash_map.put(key, value);
        }

        pub fn get(self: Self, key: K) ?V {
            return self.hash_map.get(key) catch return null;
        }

        pub fn count(self: Self) usize {
            return self.hash_map.count();
        }

        pub const Iterator = struct {
            hash_iter: TableMap.Iterator,

            const Row = struct {
                key: K,
                value: V,
            };

            pub fn next(it: *Iterator) ?Row {
                if (it.hash_iter.next()) {
                    return Row{
                        .key = it.hash_iter.key(),
                        .value = it.hash_iter.value(),
                    };
                }
                return null;
            }

            /// Reset the iterator to the initial index
            pub fn reset(it: *Iterator) void {
                it.hash_iter.reset();
            }
        };

        pub fn iterator(self: Self) Iterator {
            var iter = self.hash_map.iterator();
            return .{ .hash_iter = iter };
        }

        pub fn flush(self: *Self, sstable: *SSTable) !void {
            var timer = std.time.Timer.start() catch |err| {
                std.debug.print("flush error {s}\n", .{@errorName(err)});
                return;
            };
            const start = timer.read();
            var written: usize = 0;
            var iter = self.iterator();
            while (iter.next()) |row| {
                try sstable.append(row.key, row.value);
                written += 1;
            }
            const end = timer.read();
            std.debug.print("total rows writen {d} in {}ms\n", .{ written, (end - start) / 1_000_000 });
        }
    };
}

test Memtable {
    const testing = std.testing;
    const alloc = testing.allocator;

    // given
    var table = try Memtable(u64, []const u8).init(alloc, options.defaultOpts());
    defer alloc.destroy(table);
    defer table.deinit();

    // when
    try table.put(1, "__value__");
    var actual = table.get(1);

    // then
    try testing.expectEqualStrings("__value__", actual.?);

    // when
    var next_actual: []const u8 = undefined;
    var iter = table.iterator();
    if (iter.next()) |nxt| {
        next_actual = nxt.value[0..];
    }

// then
    try testing.expectEqualStrings("__value__", next_actual);
}
