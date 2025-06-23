const std = @import("std");

const c = @cImport({
    @cInclude("skiplist.h");
});
const iter = @import("iterator.zig");
const kv = @import("kv.zig");

const Allocator = std.mem.Allocator;

const Iterator = iter.Iterator;
const KV = kv.KV;

pub fn SkipList(
    comptime V: type,
    comptime decodeFn: fn (v: []const u8) anyerror!V,
    comptime encodeFn: fn (a: Allocator, v: V) anyerror![]const u8,
) type {
    return struct {
        impl: ?*anyopaque,

        const Self = @This();

        const SkipListError = error{
            BufferTooSmall,
            FailedInsert,
            NotFound,
        };

        pub fn init() !Self {
            const map = c.skiplist_init();

            return .{
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

        pub fn put(self: *Self, alloc: Allocator, key: []const u8, value: V) !void {
            const v = try encodeFn(alloc, value);
            defer alloc.free(v);

            try self.putRaw(key, v);
        }

        /// Zero-copy version that accepts pre-encoded value bytes directly.
        /// This is useful when reading from mmapped data to avoid extra allocations.
        pub fn putRaw(self: *Self, key: []const u8, value_bytes: []const u8) !void {
            const result = c.skiplist_insert(self.impl, key.ptr, key.len, value_bytes.ptr, value_bytes.len);
            if (result != 0) {
                std.log.err("not able to insert {d}", .{result});
                return SkipListError.FailedInsert;
            }
        }

        const SkiplistIterator = struct {
            arena: std.heap.ArenaAllocator,
            impl: *c.SkipMapIterator,
            // Keep a buffer for the current KV data to ensure it stays valid
            current_data: ?[]u8 = null,

            pub fn init(ctx: *anyopaque, alloc: Allocator) !SkiplistIterator {
                return .{
                    .arena = std.heap.ArenaAllocator.init(alloc),
                    .impl = c.skiplist_iterator_create(ctx).?,
                };
            }

            pub fn deinit(ctx: *anyopaque) void {
                const self: *SkiplistIterator = @ptrCast(@alignCast(ctx));
                c.skiplist_iterator_free(self.impl);
                self.arena.deinit();
                self.arena.child_allocator.destroy(self);
            }

            pub fn next(ctx: *anyopaque) ?V {
                const it: *SkiplistIterator = @ptrCast(@alignCast(ctx));

                var entry: c.SkipMapEntry = undefined;
                const result = c.skiplist_iterator_xnext(it.impl, &entry);

                if (result == -1) {
                    // No more elements
                    return null;
                } else if (result != 0) {
                    std.log.err("skiplist iterator error: {d}", .{result});
                    return null;
                }

                // Get direct view of the value data without copying
                const value_slice = @as([*]const u8, @ptrCast(entry.value_ptr))[0..entry.value_len];

                return decodeFn(value_slice) catch |err| {
                    std.log.err("value decode failed: {s}", .{@errorName(err)});
                    return null;
                };
            }
        };

        pub fn iterator(self: *Self, alloc: Allocator) !Iterator(KV) {
            const it = try alloc.create(SkiplistIterator);
            it.* = try SkiplistIterator.init(self.impl.?, alloc);
            return Iterator(KV).init(it, SkiplistIterator.next, SkiplistIterator.deinit);
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

    var skl = try SkipList(KV, kv.decode, kv.encode).init();
    defer skl.deinit();

    const key: []const u8 = "__key__";

    {
        const expected = KV.init(key, "__value__");

        try skl.put(alloc, key, expected);

        const actual = try skl.get(key);

        try testing.expectEqualStrings(expected.value, actual.value);
    }

    {
        const expected = KV.init(key, "__new_value__");

        try skl.put(alloc, key, expected);

        const actual = try skl.get(key);

        try testing.expectEqual(skl.count(), 1);
        try testing.expectEqualStrings(expected.value, actual.value);
    }
}

test "SkipList.Iterator" {
    const testing = std.testing;
    const ta = testing.allocator;

    var arena = std.heap.ArenaAllocator.init(ta);
    defer arena.deinit();

    const alloc = arena.allocator();

    var skl = try SkipList(KV, kv.decode, kv.encode).init();
    defer skl.deinit();

    const entries = [_]KV{
        KV.init("b", "b"),
        KV.init("a", "a"),
        KV.init("c", "c"),
    };
    for (entries) |entry| {
        try skl.put(alloc, entry.key, entry);
    }

    var actual = std.ArrayList(KV).init(alloc);
    defer actual.deinit();

    var it = try skl.iterator(alloc);
    defer it.deinit();

    while (it.next()) |nxt| {
        try actual.append(nxt);
    }

    try testing.expectEqual(3, actual.items.len);
    try testing.expectEqualStrings("a", actual.items[0].key);
}
