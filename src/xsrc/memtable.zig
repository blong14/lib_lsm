const std = @import("std");

const keyvalue = @import("kv.zig");
const iter = @import("iterator.zig");
const skl = @import("skiplist.zig");

const Allocator = std.mem.Allocator;
const AtomicValue = std.atomic.Value;

const Iterator = iter.Iterator;
const KV = keyvalue.KV;
const SkipList = skl.SkipList;

const MEMTABLE_MAGIC: u32 = 0x4D544142; // "MTAB" in hex
const MEMTABLE_VERSION: u32 = 1;

const MemtableHeader = packed struct {
    magic: u32,
    version: u32,
    id_len: usize,
    byte_count: usize,
    is_frozen: bool,
    is_flushed: bool,
};

pub const Memtable = struct {
    id: []const u8,
    index: *SkipList(KV, keyvalue.decode),
    byte_count: AtomicValue(usize),
    mutable: AtomicValue(bool),
    is_flushed: AtomicValue(bool),

    const Self = @This();

    pub fn init(alloc: Allocator, id: []const u8) !*Self {
        const id_copy = try alloc.alloc(u8, id.len);
        errdefer alloc.free(id_copy);

        @memcpy(id_copy, id);

        const index = try alloc.create(SkipList(KV, keyvalue.decode));
        errdefer alloc.destroy(index);

        index.* = try SkipList(KV, keyvalue.decode).init();

        const self = try alloc.create(Self);
        errdefer alloc.destroy(self);

        self.* = .{
            .id = id_copy,
            .index = index,
            .byte_count = AtomicValue(usize).init(0),
            .mutable = AtomicValue(bool).init(true),
            .is_flushed = AtomicValue(bool).init(false),
        };

        return self;
    }

    pub fn deinit(self: *Self, alloc: Allocator) void {
        self.index.deinit();
        alloc.destroy(self.index);
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

    pub fn serializeToDisk(self: *Self, alloc: Allocator, file_path: []const u8) !void {
        const file = try std.fs.cwd().createFile(file_path, .{});
        defer file.close();

        const header = MemtableHeader{
            .magic = MEMTABLE_MAGIC,
            .version = MEMTABLE_VERSION,
            .id_len = self.id.len,
            .byte_count = self.size(),
            .is_frozen = self.frozen(),
            .is_flushed = self.flushed(),
        };

        try file.writeAll(std.mem.asBytes(&header));
        try file.writeAll(self.id);
        try file.writeAll(std.mem.asBytes(&self.index.count));

        var it = try self.iterator(alloc);
        defer it.deinit();

        while (it.next()) |nxt| {
            try file.writeAll(std.mem.asBytes(&nxt.len()));
            try file.writeAll(nxt.raw_bytes);
        }
    }

    pub fn deserializeFromDisk(alloc: Allocator, file_path: []const u8) !*Self {
        const file = try std.fs.cwd().openFile(file_path, .{});
        defer file.close();

        var header: MemtableHeader = undefined;
        _ = try file.readAll(std.mem.asBytes(&header));

        if (header.magic != MEMTABLE_MAGIC) {
            return error.InvalidMemtableFile;
        }
        if (header.version != MEMTABLE_VERSION) {
            return error.UnsupportedMemtableVersion;
        }

        const id = try alloc.alloc(u8, header.id_len);
        errdefer alloc.free(id);

        _ = try file.readAll(id);

        const self = try Memtable.init(alloc, id);
        errdefer alloc.destroy(self);

        var entry_count: u64 = undefined;
        _ = try file.readAll(std.mem.asBytes(&entry_count));

        var i: u32 = 0;
        while (i < entry_count) : (i += 1) {
            var kv_len: u64 = undefined;
            _ = try file.readAll(std.mem.asBytes(&kv_len));

            const buf = try alloc.alloc(u8, kv_len);
            defer alloc.free(buf);

            _ = try file.readAll(buf);

            var kv: KV = undefined;
            try kv.decode(buf);

            try self.put(kv);
        }

        if (header.is_frozen) {
            self.freeze();
        }
        if (header.is_flushed) {
            self.flush();
        }

        return self;
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
