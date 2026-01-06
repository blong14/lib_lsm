const std = @import("std");

const iter = @import("iterator.zig");
const c = @cImport({
    @cInclude("skiplist.h");
});

const Allocator = std.mem.Allocator;
const Iterator = iter.Iterator;

pub fn SkipList(
    comptime V: type,
    comptime decodeFn: fn (v: []const u8) anyerror!V,
) type {
    return struct {
        impl: ?*anyopaque,
        count: u64 = 0,

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
            self.count += 1;
        }

        const SkiplistIterator = struct {
            alloc: Allocator,
            impl: *c.SkipMapIterator,

            pub fn init(ctx: *anyopaque, alloc: Allocator) !*SkiplistIterator {
                const iter_ptr = c.skiplist_iterator_create(ctx) orelse return error.IteratorCreationFailed;

                const it = try alloc.create(SkiplistIterator);
                it.* = .{ .alloc = alloc, .impl = iter_ptr };

                return it;
            }

            pub fn deinit(ctx: *anyopaque) void {
                const self: *SkiplistIterator = @ptrCast(@alignCast(ctx));
                c.skiplist_iterator_free(self.impl);
                self.alloc.destroy(self);
            }

            pub fn next(ctx: *anyopaque) ?V {
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

        pub fn iterator(self: *Self, alloc: Allocator) !Iterator(V) {
            const it = try SkiplistIterator.init(self.impl.?, alloc);

            return Iterator(V).init(it, SkiplistIterator.next, SkiplistIterator.deinit);
        }
    };
}
