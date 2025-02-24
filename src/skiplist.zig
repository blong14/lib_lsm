const std = @import("std");

const c = @cImport({
    @cInclude("skiplist.h");
});
const kv = @import("kv.zig");

const Allocator = std.mem.Allocator;
const KV = kv.KV;

const assert = std.debug.assert;
const print = std.debug.print;

pub fn SkipList(
    comptime V: type,
    comptime decodeFn: fn (v: []const u8) anyerror!V,
    comptime encodeFn: fn (a: Allocator, v: V) anyerror![]const u8,
) type {
    return struct {
        alloc: Allocator,
        impl: ?*anyopaque,

        const Self = @This();

        const SkipListError = error{
            BufferTooSmall,
            FailedInsert,
            NotFound,
        };

        pub fn init(alloc: Allocator) !Self {
            const map = c.skiplist_init();

            return .{
                .alloc = alloc,
                .impl = map.?,
            };
        }

        pub fn deinit(self: *Self) void {
            c.skiplist_free(self.impl);
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
            const v = try encodeFn(self.alloc, value);
            // defer self.alloc.free(v);

            const result = c.skiplist_insert(self.impl, key.ptr, key.len, v.ptr, v.len);
            if (result != 0) {
                print("not able to insert {d}\n", .{result});
                return SkipListError.FailedInsert;
            }
        }

        pub const Iterator = struct {
            const Item = struct {
                key: []const u8,
                value: V,
            };

            alloc: Allocator,
            impl: ?*c.SkipMapIterator,
            crnt: usize,
            nxt: usize,
            stack: std.MultiArrayList(Item),
            state: State,

            const State = enum {
                Open,
                Closed,
            };

            pub fn deinit(it: *Iterator) void {
                for (it.stack.items(.value)) |item| {
                    it.alloc.free(item);
                }
                it.stack.deinit(it.alloc);
                c.skiplist_iterator_free(it.impl.?);
                it.* = undefined;
            }

            pub fn key(it: Iterator) []const u8 {
                it.stack.pop();
                return it.stack.items(.key)[it.crnt];
            }

            pub fn value(it: Iterator) V {
                return it.stack.items(.value)[it.crnt];
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

                const nxt_value = try it.alloc.alloc(u8, value_len);
                @memcpy(nxt_value, value_buffer[0..value_len]);

                const val = decodeFn(nxt_value) catch |err| {
                    print("not able to decode value {s}\n", .{@errorName(err)});
                    return false;
                };

                try it.stack.append(it.alloc, Item{ .key = key_buffer[0..key_len], .value = val });
                it.crnt = it.nxt;
                it.nxt += 1;

                return true;
            }
        };

        pub fn iter(self: Self) Iterator {
            return .{
                .alloc = self.alloc,
                .impl = c.skiplist_iterator_create(self.impl),
                .crnt = 0,
                .nxt = 0,
                .stack = std.MultiArrayList(Iterator.Item){},
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
    defer arena.deinit();

    const alloc = arena.allocator();

    var skl = try SkipList([]const u8, kv.decode, kv.encode).init(ta);
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

    var iter = skl.iter();
    defer iter.deinit();

    while (try iter.next()) {
        try actual.append(iter.value());
    }

    try testing.expectEqual(3, actual.items.len);
    try testing.expectEqualStrings("a", actual.items[0]);
}
