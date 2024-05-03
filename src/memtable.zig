const std = @import("std");

const log = @import("wal.zig");
const options = @import("opts.zig");
const sst = @import("sstable.zig");
const tbm = @import("tablemap.zig");

const Allocator = std.mem.Allocator;
const Entry = tbm.MapEntry;
const Opts = options.Opts;
const SSTable = sst.SSTable;
const TableMap = tbm.TableMap;

pub fn Memtable(comptime K: type, comptime V: type) type {
    return struct {
        alloc: Allocator,
        hash_map: *TableMap(V),
        id: K,
        wal: *log.WAL,

        const Self = @This();

        const Error = error{
            Full,
        };

        pub fn init(alloc: Allocator, id: K, opts: Opts) !*Self {
            const filename = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ opts.data_dir, "wal.dat" });
            defer alloc.free(filename);
            const wal = try log.WAL.init(alloc, filename, opts.wal_capacity);
            const map = try TableMap(V).init(alloc);
            const mtable = try alloc.create(Self);
            mtable.* = .{
                .alloc = alloc,
                .hash_map = map,
                .id = id,
                .wal = wal,
            };
            return mtable;
        }

        pub fn deinit(self: *Self) void {
            self.hash_map.deinit();
            self.alloc.destroy(self.hash_map);
            self.wal.deinit();
            self.alloc.destroy(self.wal);
            self.* = undefined;
        }

        pub fn getId(self: Self) K {
            return self.id;
        }

        pub fn put(self: *Self, key: K, value: V) !void {
            try self.wal.write(key, value);
            try self.hash_map.put(key, value);
        }

        pub fn get(self: Self, key: K) ?V {
            return self.hash_map.get(key) catch return null;
        }

        pub fn count(self: Self) usize {
            return self.hash_map.count();
        }

        pub fn scan(self: *Self, start: u64, end: u64, out: *std.ArrayList(tbm.MapEntry)) !void {
            var scnr = try self.hash_map.scanner(start, end);
            while (scnr.hasNext()) {
                const entry = try scnr.next();
                try out.append(entry.?);
            }
        }

        pub const Iterator = struct {
            hash_iter: TableMap(V).Iterator,
            k: u64,
            v: V,

            pub fn key(it: Iterator) u64 {
                return it.k;
            }

            pub fn value(it: Iterator) V {
                return it.v;
            }

            pub fn next(it: *Iterator) bool {
                if (it.hash_iter.next()) {
                    it.*.k = it.hash_iter.key();
                    it.*.v = it.hash_iter.value();
                    return true;
                }
                return false;
            }

            /// Reset the iterator to the initial index
            pub fn reset(it: *Iterator) void {
                it.hash_iter.reset();
            }
        };

        pub fn iterator(self: Self) Iterator {
            const iter = self.hash_map.iterator(0);
            return .{ .hash_iter = iter, .k = 0, .v = undefined };
        }

        pub fn flush(self: *Self, sstable: *SSTable) !void {
            var iter = self.iterator();
            while (iter.next()) {
                try sstable.append(iter.key(), iter.value());
            }
        }
    };
}

test Memtable {
    const testing = std.testing;
    const alloc = testing.allocator;

    // given
    var mtable = try Memtable(u64, []const u8).init(alloc, 0, options.defaultOpts());
    defer alloc.destroy(mtable);
    defer mtable.deinit();

    // when
    try mtable.put(1, "__value__");
    const actual = mtable.get(1);

    // then
    try testing.expectEqualStrings("__value__", actual.?);

    // when
    var next_actual: []const u8 = undefined;
    var iter = mtable.iterator();
    if (iter.next()) {
        next_actual = iter.value();
    }

    // then
    try testing.expectEqualStrings("__value__", next_actual);
}
