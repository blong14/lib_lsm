const std = @import("std");

const keyvalue = @import("kv.zig");
const iter = @import("iterator.zig");
const skl = @import("skiplist.zig");
const tbl = @import("tablemap.zig");
const wal = @import("wal.zig");

const Allocator = std.mem.Allocator;
const AtomicValue = std.atomic.Value;

const Iterator = iter.Iterator;
const KV = keyvalue.KV;
const SkipList = skl.SkipList;

pub fn decodeInternalKey(a: []const u8) anyerror![]const u8 {
    return a;
}

const KeyValueSkipList = SkipList(KV, keyvalue.decode);
const KeyValueSkipListIndex = SkipList([]const u8, decodeInternalKey);
const WAL = wal.WAL;

pub const Memtable = struct {
    init_alloc: Allocator,
    id: []const u8,
    byte_count: AtomicValue(usize),

    data: *KeyValueSkipList,
    user_key_index: KeyValueSkipListIndex,
    wal_log: ?*WAL,

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
        errdefer alloc.destroy(mtable);

        mtable.* = .{
            .init_alloc = alloc,
            .id = id_copy,
            .data = map,
            .user_key_index = try KeyValueSkipListIndex.init(),
            .wal_log = null,
            .byte_count = AtomicValue(usize).init(0),
            .mutable = AtomicValue(bool).init(true),
            .isFlushed = AtomicValue(bool).init(false),
        };

        return mtable;
    }

    pub fn initWithWAL(alloc: Allocator, id: []const u8, wal_path: []const u8) !*Self {
        const mtable = try init(alloc, id);
        errdefer {
            mtable.deinit();
            alloc.destroy(mtable);
        }

        const wal_log = try WAL.init(alloc, wal_path, std.mem.page_size);
        mtable.wal_log = wal_log;

        return mtable;
    }

    pub fn deinit(self: *Self) void {
        if (self.wal_log) |wal_log| {
            wal_log.deinit();
            self.init_alloc.destroy(wal_log);
        }

        self.user_key_index.deinit();

        self.data.deinit();
        self.init_alloc.destroy(self.data);

        self.init_alloc.free(self.id);
        self.* = undefined;
    }

    pub fn getId(self: Self) []const u8 {
        return self.id;
    }

    pub fn put(self: *Self, item: KV) !void {
        if (self.frozen()) return error.MemtableImmutable;

        // Write to WAL first for durability - if this fails, the entire operation fails
        if (self.wal_log) |wal_log| {
            const key_hash = std.hash.Murmur2_64.hash(item.key);
            try wal_log.write(key_hash, item.raw_bytes);
        }

        // Only proceed with memtable writes if WAL write succeeded
        try self.user_key_index.put(item.userKey(), item.internalKey());
        try self.data.put(item.internalKey(), item.raw_bytes);

        _ = self.byte_count.fetchAdd(item.len(), .release);
    }

    pub fn get(self: *Self, user_key: []const u8) !?KV {
        const internal_key_opt = try self.user_key_index.get(user_key);

        if (internal_key_opt) |internal_key| {
            return try self.data.get(internal_key);
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
        const it = try alloc.create(MemtableIterator);
        errdefer alloc.destroy(it);

        it.* = .{
            .alloc = alloc,
            .iter = try self.data.iterator(alloc),
        };

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
