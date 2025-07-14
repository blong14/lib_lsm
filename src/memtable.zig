const std = @import("std");

const keyvalue = @import("kv.zig");
const iter = @import("iterator.zig");
const skl = @import("skiplist.zig");

const Allocator = std.mem.Allocator;
const AtomicValue = std.atomic.Value;

const Iterator = iter.Iterator;
const KV = keyvalue.KV;
const SkipList = skl.SkipList;

const KeyValueSkipList = SkipList(KV, keyvalue.decode);

pub const Memtable = struct {
    init_alloc: Allocator,
    id: []const u8,
    byte_count: AtomicValue(usize),
    data: AtomicValue(*KeyValueSkipList),
    index_mutex: std.Thread.Mutex,
    user_key_index: std.HashMap(
        []const u8,
        []const u8,
        std.hash_map.StringContext,
        std.hash_map.default_max_load_percentage,
    ),
    mutable: AtomicValue(bool),
    isFlushed: AtomicValue(bool),

    const Self = @This();

    pub fn init(alloc: Allocator, id: []const u8) !*Self {
        const id_copy = try alloc.alloc(u8, id.len);
        errdefer alloc.free(id_copy);

        @memcpy(id_copy, id);

        const map = try alloc.create(KeyValueSkipList);
        errdefer alloc.destroy(map);

        map.* = try KeyValueSkipList.init();

        const mtable = try alloc.create(Self);
        mtable.* = .{
            .init_alloc = alloc,
            .id = id_copy,
            .byte_count = AtomicValue(usize).init(0),
            .data = AtomicValue(*KeyValueSkipList).init(map),
            .user_key_index = std.HashMap([]const u8, []const u8, std.hash_map.StringContext, std.hash_map.default_max_load_percentage).init(alloc),
            .index_mutex = std.Thread.Mutex{},
            .mutable = AtomicValue(bool).init(true),
            .isFlushed = AtomicValue(bool).init(false),
        };
        return mtable;
    }

    pub fn deinit(self: *Self) void {
        const data = self.data.load(.acquire);
        data.deinit();
        self.init_alloc.destroy(data);

        // Clean up the user key index
        var it = self.user_key_index.iterator();
        while (it.next()) |entry| {
            self.init_alloc.free(entry.key_ptr.*);
            self.init_alloc.free(entry.value_ptr.*);
        }
        self.user_key_index.deinit();

        self.init_alloc.free(self.id);
        self.* = undefined;
    }

    pub fn getId(self: Self) []const u8 {
        return self.id;
    }

    pub fn put(self: *Self, item: KV) !void {
        if (self.frozen()) return error.MemtableImmutable;

        const data = self.data.load(.acquire);
        try data.put(item.internalKey(), item.raw_bytes);

        // Update the user key index to point to the latest internal key
        self.index_mutex.lock();
        defer self.index_mutex.unlock();

        // Clone the user key and internal key for the index
        const user_key_copy = try self.init_alloc.dupe(u8, item.userKey());
        const internal_key_copy = try self.init_alloc.dupe(u8, item.internalKey());

        // Use getOrPut to handle both new and existing keys
        const result = try self.user_key_index.getOrPut(user_key_copy);
        if (result.found_existing) {
            // Free the old internal key and update with new one
            self.init_alloc.free(result.value_ptr.*);
            result.value_ptr.* = internal_key_copy;
            // Free the duplicate user key since we're using the existing one
            self.init_alloc.free(user_key_copy);
        } else {
            // New entry, set the internal key
            result.value_ptr.* = internal_key_copy;
        }

        _ = self.byte_count.fetchAdd(item.len(), .release);
    }

    pub fn get(self: *Self, key: []const u8) !?KV {
        // First check the user key index for fast lookup
        self.index_mutex.lock();
        const internal_key_opt = self.user_key_index.get(key);
        self.index_mutex.unlock();

        if (internal_key_opt) |internal_key| {
            const data = self.data.load(.acquire);
            return try data.get(internal_key);
        }

        return null;
    }

    pub fn freeze(self: *Self) void {
        self.mutable.store(false, .release);
    }

    pub fn frozen(self: Self) bool {
        return !self.mutable.load(.acquire);
    }

    pub fn flushed(self: Self) bool {
        return self.isFlushed.load(.acquire);
    }

    pub fn size(self: Self) usize {
        return self.byte_count.load(.acquire);
    }

    const MemtableIterator = struct {
        alloc: Allocator,
        iter: Iterator(KV),

        pub fn deinit(ctx: *anyopaque) void {
            const self: *MemtableIterator = @ptrCast(@alignCast(ctx));
            self.iter.deinit();
            self.alloc.destroy(self);
        }

        pub fn next(ctx: *anyopaque) ?KV {
            const self: *MemtableIterator = @ptrCast(@alignCast(ctx));
            if (self.iter.next()) |nxt| {
                return nxt;
            }
            return null;
        }
    };

    pub fn iterator(self: *Self, alloc: Allocator) !Iterator(KV) {
        const data = self.data.load(.acquire);

        const it = try alloc.create(MemtableIterator);
        errdefer alloc.destroy(it);

        it.* = .{ .alloc = alloc, .iter = data.iterator(alloc) catch |err| {
            alloc.destroy(it);
            return err;
        } };

        return Iterator(KV).init(it, MemtableIterator.next, MemtableIterator.deinit);
    }
};

test Memtable {
    const testing = std.testing;
    const alloc = testing.allocator;
    // given
    var mtable = try Memtable.init(alloc, "0");
    defer alloc.destroy(mtable);
    defer mtable.deinit();

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
