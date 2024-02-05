const std = @import("std");

const sst = @import("sstable.zig");

const Allocator = std.mem.Allocator;
const SSTable = sst.SSTable;

pub fn Memtable(comptime K: type, comptime V: type) type {
    return struct {
        hash_map: std.AutoArrayHashMap(K, V),

        const Self = @This();

        const Error = error{
            Full,
        };

        pub fn init(alloc: Allocator) !Self {
            var map = std.AutoArrayHashMap(K, V).init(alloc);
            return .{ .hash_map = map };
        }

        pub fn deinit(self: *Self) void {
            self.hash_map.deinit();
            self.* = undefined;
        }

        pub fn put(self: *Self, key: K, value: V) !void {
            try self.hash_map.put(key, value);
        }

        pub fn get(self: Self, key: K) ?V {
            return self.hash_map.get(key);
        }

        pub fn count(self: Self) usize {
            return self.hash_map.count();
        }

        pub const Iterator = struct {
            hash_iter: std.AutoArrayHashMap(K, V).Iterator,

            const Entry = struct {
                key_ptr: *K,
                value_ptr: *V,
            };

            pub fn next(it: *Iterator) ?Entry {
                if (it.hash_iter.next()) |entry| {
                    return Entry{
                        .key_ptr = entry.key_ptr,
                        .value_ptr = entry.value_ptr,
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
            var iter = self.iterator();
            while (iter.next()) |row| {
                try sstable.write(row.key_ptr.*, row.value_ptr.*);
            }
        }
    };
}

test Memtable {
    const testing = std.testing;
    const alloc = testing.allocator;

    // given
    var table = try Memtable(i64, []const u8).init(alloc);
    defer table.deinit();

    // when
    try table.put(1, "__value__");
    var actual = table.get(1);

    // then
    try testing.expectEqualStrings("__value__", actual.?);

    // when
    var iter = table.iterator();
    const entry = iter.next();
    actual = entry.?.value_ptr.*;

    // then
    try testing.expectEqualStrings("__value__", actual.?);
}
