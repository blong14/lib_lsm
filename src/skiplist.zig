const std = @import("std");

const c = @cImport({
    @cInclude("skiplist.h");
});
const kv = @import("kv.zig");

const Allocator = std.mem.Allocator;
const KV = kv.KV;

const print = std.debug.print;

pub fn SkipList(
    comptime V: type,
    comptime decodeFn: fn (v: []const u8) anyerror!V,
    comptime encodeFn: fn (a: Allocator, v: V) anyerror![]const u8,
) type {
    return struct {
        arena: std.heap.ArenaAllocator,
        impl: ?*anyopaque,

        const Self = @This();

        const SkipListError = error{
            BufferTooSmall,
            FailedInsert,
            NotFound,
        };

        pub fn init(alloc: Allocator) !*Self {
            const map = c.skiplist_init();

            const skl = try alloc.create(Self);
            skl.* = .{
                .arena = std.heap.ArenaAllocator.init(alloc),
                .impl = map.?,
            };

            return skl;
        }

        pub fn deinit(self: *Self) void {
            c.skiplist_free(self.impl);
            self.arena.deinit();
            self.* = undefined;
        }

        pub fn get(self: Self, key: []const u8) !V {
            var value_buf: [std.mem.page_size]u8 = undefined;
            var value_len: usize = value_buf.len;

            const result = c.skiplist_get(self.impl, key.ptr, key.len, &value_buf[0], &value_len);
            if (result == -1) {
                return SkipListError.NotFound;
            } else if (result == -2) {
                return SkipListError.BufferTooSmall;
            }

            const value = try decodeFn(value_buf[0..value_len]);

            return value;
        }

        pub fn put(self: *Self, key: []const u8, value: V) !void {
            const v = try encodeFn(self.arena.allocator(), value);

            const result = c.skiplist_insert(self.impl, key.ptr, key.len, v.ptr, v.len);
            if (result != 0) {
                print("not able to insert {d}\n", .{result});
                return SkipListError.FailedInsert;
            }
        }

        pub const Iterator = struct {
            arena: std.heap.ArenaAllocator,
            impl: ?*c.SkipMapIterator,
            k: []const u8,
            v: V,
            state: State,

            const State = enum {
                Open,
                Closed,
            };

            pub fn deinit(it: *Iterator) void {
                c.skiplist_iterator_free(it.impl.?);
                it.arena.deinit();
                it.* = undefined;
            }

            pub fn key(it: Iterator) []const u8 {
                return it.k;
            }

            pub fn value(it: Iterator) V {
                return it.v;
            }

            pub fn next(it: *Iterator) !bool {
                if (it.impl == null) return false;

                var key_buffer: [std.mem.page_size]u8 = undefined;
                var key_len: usize = std.mem.page_size;

                var value_buffer: [std.mem.page_size]u8 = undefined;
                var value_len: usize = std.mem.page_size;

                const result = c.skiplist_iterator_next(
                    it.impl.?,
                    &key_buffer[0],
                    &key_len,
                    &value_buffer[0],
                    &value_len,
                );
                if (result == -1) {
                    // No more elements
                    return false;
                } else if (result != 0) {
                    print("skiplist iterator error: {d}\n", .{result});
                    return false;
                }

                const key_data = try it.arena.allocator().alloc(u8, key_len);
                @memcpy(key_data, key_buffer[0..key_len]);

                const value_data = try it.arena.allocator().alloc(u8, value_len);
                @memcpy(value_data, value_buffer[0..value_len]);

                const val = decodeFn(value_data) catch |err| {
                    print("not able to decode value {s}\n", .{@errorName(err)});
                    return false;
                };

                it.*.k = key_data;
                it.*.v = val;

                return true;
            }
        };

        pub fn iter(self: Self, alloc: Allocator) Iterator {
            return .{
                .arena = std.heap.ArenaAllocator.init(alloc),
                .impl = c.skiplist_iterator_create(self.impl),
                .k = undefined,
                .v = undefined,
                .state = .Open,
            };
        }

        pub fn count(self: Self) usize {
            const cnt = c.skiplist_size(self.impl);
            if (cnt >= 0) return @intCast(cnt) else return 0;
        }
    };
}

test SkipList {
    const testing = std.testing;
    const alloc = testing.allocator;

    var skl = try SkipList([]const u8, kv.decode, kv.encode).init(alloc);
    defer alloc.destroy(skl);
    defer skl.deinit();

    const key: []const u8 = "__key__";

    {
        const expected = KV.init(key, "__value__");

        try skl.put(key, expected.value);

        const actual = try skl.get(key);

        try testing.expectEqualStrings(expected.value, actual);
    }

    {
        const expected = KV.init(key, "__new_value__");

        try skl.put(key, expected.value);

        const actual = try skl.get(key);

        try testing.expectEqual(skl.count(), 1);
        try testing.expectEqualStrings(expected.value, actual);
    }
}

test "SkipList.Iterator" {
    const testing = std.testing;
    const ta = testing.allocator;

    var arena = std.heap.ArenaAllocator.init(ta);
    const alloc = arena.allocator();
    defer arena.deinit();

    var skl = try SkipList([]const u8, kv.decode, kv.encode).init(alloc);
    defer alloc.destroy(skl);
    defer skl.deinit();

    const entries = [_]KV{
        KV.init("b", "b"),
        KV.init("a", "a"),
        KV.init("c", "c"),
    };
    for (entries) |entry| {
        try skl.put(entry.key, entry.value);
    }

    var actual = std.ArrayList([]const u8).init(alloc);
    defer actual.deinit();

    var iter = skl.iter(alloc);
    defer iter.deinit();

    while (try iter.next()) {
        try actual.append(iter.value());
    }

    try testing.expectEqual(3, actual.items.len);
    try testing.expectEqualStrings("a", actual.items[0]);
}
