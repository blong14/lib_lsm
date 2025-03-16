const std = @import("std");

const c = @cImport({
    @cInclude("skiplist.h");
});
const iter = @import("iterator.zig");
const kv = @import("kv.zig");

const Allocator = std.mem.Allocator;

const Iterator = iter.Iterator;
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

        pub const Item = struct {
            data: []const u8,
            key: []const u8,
            value: V,
        };

        const SkiplistIterator = struct {
            alloc: Allocator,
            impl: *c.SkipMapIterator,
            items: std.ArrayList(Item),

            pub fn init(ctx: *anyopaque, alloc: Allocator) SkiplistIterator {
                const items = std.ArrayList(Item).init(alloc);
                return .{
                    .alloc = alloc,
                    .impl = c.skiplist_iterator_create(ctx).?,
                    .items = items,
                };
            }

            pub fn deinit(ctx: *anyopaque) void {
                const self: *SkiplistIterator = @ptrCast(@alignCast(ctx));
                c.skiplist_iterator_free(self.impl);
                for (self.items.items) |item| {
                    self.alloc.free(item.data);
                }
                self.items.deinit();
                self.alloc.destroy(self);
            }

            pub fn next(ctx: *anyopaque) ?Item {
                const it: *SkiplistIterator = @ptrCast(@alignCast(ctx));

                var key_buffer: [std.mem.page_size]u8 = undefined;
                var key_len: usize = std.mem.page_size;

                var value_buffer: [std.mem.page_size]u8 = undefined;
                var value_len: usize = std.mem.page_size;

                const result = c.skiplist_iterator_next(
                    it.impl,
                    &key_buffer[0],
                    &key_len,
                    &value_buffer[0],
                    &value_len,
                );
                if (result == -1) {
                    // No more elements
                    return null;
                } else if (result != 0) {
                    print("skiplist iterator error: {d}\n", .{result});
                    return null;
                }

                const val = decodeFn(value_buffer[0..value_len]) catch |err| {
                    print("not able to decode value {s}\n", .{@errorName(err)});
                    return null;
                };

                const byts = it.alloc.alloc(u8, key_len + value_len) catch unreachable;
                std.mem.copyForwards(u8, byts[0..key_len], key_buffer[0..key_len]);
                std.mem.copyForwards(u8, byts[key_len..], val);

                const item: Item = .{
                    .data = byts,
                    .key = byts[0..key_len],
                    .value = byts[key_len..],
                };

                it.items.append(item) catch |err| {
                    print("skiplist iterator error: {s}\n", .{@errorName(err)});
                    return null;
                };

                return item;
            }
        };

        pub fn iterator(self: *Self, alloc: Allocator) !Iterator(Item) {
            const it = try alloc.create(SkiplistIterator);
            it.* = SkiplistIterator.init(self.impl.?, alloc);
            return Iterator(Item).init(it, SkiplistIterator.next, SkiplistIterator.deinit);
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

    var it = try skl.iterator(alloc);
    defer it.deinit();

    while (it.next()) |nxt| {
        try actual.append(nxt.value);
    }

    try testing.expectEqual(3, actual.items.len);
    try testing.expectEqualStrings("a", actual.items[0]);
}
