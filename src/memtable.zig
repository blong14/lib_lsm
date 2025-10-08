const std = @import("std");

const keyvalue = @import("kv.zig");
const iter = @import("iterator.zig");
const skl = @import("skiplist.zig");

const Allocator = std.mem.Allocator;
const AtomicValue = std.atomic.Value;

const Iterator = iter.Iterator;
const KV = keyvalue.KV;
const SkipList = skl.SkipList;

pub const Memtable = struct {
    id: []const u8,
    index: SkipList(KV, keyvalue.decode),
    byte_count: AtomicValue(usize),
    mutable: AtomicValue(bool),
    is_flushed: AtomicValue(bool),

    const Self = @This();

    pub fn init(alloc: Allocator, id: []const u8) !*Self {
        const id_copy = try alloc.alloc(u8, id.len);
        errdefer alloc.free(id_copy);

        @memcpy(id_copy, id);

        const self = try alloc.create(Self);
        errdefer alloc.destroy(self);

        self.* = .{
            .id = id_copy,
            .index = try SkipList(KV, keyvalue.decode).init(),
            .byte_count = AtomicValue(usize).init(0),
            .mutable = AtomicValue(bool).init(true),
            .is_flushed = AtomicValue(bool).init(false),
        };

        return self;
    }

    pub fn deinit(self: *Self, alloc: Allocator) void {
        self.index.deinit();
        alloc.free(self.id);
        self.* = undefined;
    }

    pub fn getId(self: Self) []const u8 {
        return self.id;
    }

    pub fn put(self: *Self, item: KV) !void {
        if (self.frozen()) return error.MemtableImmutable;
        if (item.key.len == 0) return error.InvalidKey;

        try self.index.put(item.key, item.raw_bytes);

        _ = self.byte_count.fetchAdd(item.len(), .release);
    }

    pub fn get(self: *Self, user_key: []const u8) !?KV {
        if (user_key.len == 0) return null;

        return try self.index.get(user_key);
    }

    pub fn freeze(self: *Self) void {
        self.mutable.store(false, .release);
    }

    pub fn frozen(self: Self) bool {
        return !self.mutable.load(.acquire);
    }

    pub fn flush(self: *Self) void {
        self.freeze();
        self.is_flushed.store(true, .release);
    }

    pub fn flushed(self: Self) bool {
        return self.is_flushed.load(.acquire);
    }

    pub fn size(self: Self) usize {
        return self.byte_count.load(.acquire);
    }

    pub fn iterator(self: *Self, alloc: Allocator) !Iterator(KV) {
        return try self.index.iterator(alloc);
    }
};

test Memtable {
    const testing = std.testing;
    const alloc = testing.allocator;

    // given
    var mtable = try Memtable.init(alloc, "0");
    defer alloc.destroy(mtable);
    defer mtable.deinit(alloc);

    // when
    var kv = try KV.initOwned(alloc, "__key__", "__value__");
    defer kv.deinit(alloc);

    try mtable.put(kv);

    const actual = try mtable.get("__key__");

    // then
    try testing.expectEqualStrings(kv.value, actual.?.value);
    try testing.expectEqual(keyvalue.KVOwnership.borrowed, actual.?.ownership);

    var kv2 = try KV.initOwned(alloc, "__key__", "__updated_value__");
    defer kv2.deinit(alloc);

    try mtable.put(kv2);

    const latest = try mtable.get("__key__");

    try testing.expectEqualStrings("__updated_value__", latest.?.value);
    try testing.expectEqual(keyvalue.KVOwnership.borrowed, latest.?.ownership);
}
