const std = @import("std");

const c = @cImport({
    @cInclude("skiplist.h");
});
const iter = @import("iterator.zig");
const keyvalue = @import("kv.zig");

const Allocator = std.mem.Allocator;

const Iterator = iter.Iterator;
const KV = keyvalue.KV;

pub fn SkipList(
    comptime V: type,
    comptime decodeFn: fn (v: []const u8) anyerror!V,
) type {
    return struct {
        impl: ?*anyopaque,

        const Self = @This();

        const SkipListError = error{
            FailedInsert,
            NotFound,
            IteratorCreationFailed,
            InitializationFailed,
        };

        pub fn init() !Self {
            const map = c.skiplist_init() orelse return error.InitializationFailed;
            return .{ .impl = map };
        }

        pub fn deinit(self: *Self) void {
            c.skiplist_free(self.impl);
            self.* = undefined;
        }

        pub fn get(self: Self, key: []const u8) !?V {
            var value_ptr: [*c]const u8 = undefined;
            var value_len: usize = undefined;

            const result = c.skiplist_get(
                self.impl,
                key.ptr,
                key.len,
                @ptrCast(&value_ptr),
                &value_len,
            );
            if (result != 0) {
                return null;
            }

            const value_slice = @as(
                [*]const u8,
                @ptrCast(value_ptr),
            )[0..value_len];

            return decodeFn(value_slice) catch return null;
        }

        pub fn put(self: *Self, key: []const u8, value_bytes: []const u8) !void {
            const result = c.skiplist_insert(
                self.impl,
                key.ptr,
                key.len,
                value_bytes.ptr,
                value_bytes.len,
            );
            if (result != 0) {
                return SkipListError.FailedInsert;
            }
        }

        const SkiplistIterator = struct {
            impl: *c.SkipMapIterator,

            pub fn init(ctx: *anyopaque) !SkiplistIterator {
                const iter_ptr = c.skiplist_iterator_create(ctx) orelse return error.IteratorCreationFailed;
                return .{ .impl = iter_ptr };
            }

            pub fn deinit(ctx: *anyopaque) void {
                const self: *SkiplistIterator = @ptrCast(@alignCast(ctx));
                c.skiplist_iterator_free(self.impl);
            }

            pub fn next(ctx: *anyopaque) ?KV {
                const it: *SkiplistIterator = @ptrCast(@alignCast(ctx));

                var entry: c.SkipMapEntry = undefined;

                const result = c.skiplist_iterator_next(it.impl, &entry);
                if (result != 0) {
                    return null;
                }

                const value_slice = @as(
                    [*]const u8,
                    @ptrCast(entry.value_ptr),
                )[0..entry.value_len];

                return decodeFn(value_slice) catch return null;
            }
        };

        pub fn iterator(self: *Self, alloc: Allocator) !Iterator(KV) {
            const it = try alloc.create(SkiplistIterator);
            errdefer alloc.destroy(it);

            it.* = try SkiplistIterator.init(self.impl.?);

            return Iterator(KV).init(it, SkiplistIterator.next, SkiplistIterator.deinit);
        }
    };
}

test SkipList {
    const testing = std.testing;
    const alloc = testing.allocator;

    var skl = try SkipList(KV, keyvalue.decode).init();
    defer skl.deinit();

    const key: []const u8 = "__key__";

    {
        var expected = try KV.initOwned(alloc, key, "__value__");
        defer expected.deinit(alloc);

        try skl.put(key, expected.raw_bytes);

        const actual = try skl.get(key);

        try testing.expectEqualStrings(expected.value, actual.?.value);
    }

    {
        var expected = try KV.initOwned(alloc, key, "__new_value__");
        defer expected.deinit(alloc);

        try skl.put(key, expected.raw_bytes);

        const actual = try skl.get(key);

        try testing.expectEqualStrings(expected.value, actual.?.value);
    }
}

test "SkipList.Iterator" {
    const testing = std.testing;
    const ta = testing.allocator;

    var arena = std.heap.ArenaAllocator.init(ta);
    defer arena.deinit();

    const alloc = arena.allocator();

    var skl = try SkipList(KV, keyvalue.decode).init();
    defer skl.deinit();

    const entries = [_]KV{
        try KV.initOwned(alloc, "b", "b"),
        try KV.initOwned(alloc, "a", "a"),
        try KV.initOwned(alloc, "c", "c"),
    };
    for (entries) |entry| {
        try skl.put(entry.key, entry.raw_bytes);
    }

    var actual = std.ArrayList(KV).init(alloc);
    defer actual.deinit();

    var it = try skl.iterator(alloc);
    defer it.deinit();

    while (it.next()) |nxt| {
        try actual.append(try nxt.clone(alloc));
    }

    try testing.expectEqual(3, actual.items.len);
    try testing.expectEqualStrings("a", actual.items[0].key);
}
