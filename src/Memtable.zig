const std = @import("std");

const iter = @import("iterator.zig");
const skl = @import("skiplist.zig");

const Allocator = std.mem.Allocator;
const AtomicValue = std.atomic.Value;

const KV = @import("KV.zig");
const Iterator = iter.Iterator;
const SkipList = skl.SkipList;

const MEMTABLE_MAGIC: u32 = 0x4D544142; // "MTAB" in hex
const MEMTABLE_VERSION: u32 = 1;
const Endian = std.builtin.Endian.little;

const Header = packed struct {
    magic: u32,
    version: u32,
    id_len: usize,
    byte_count: usize,
};

fn decode(buf: []const u8) !KV {
    var kv: KV = undefined;
    try kv.decode(buf);
    return kv;
}

id: u64,
index: SkipList(KV, decode),
byte_count: AtomicValue(usize),
flushed: AtomicValue(bool),
mutable: AtomicValue(bool),

pub const Memtable = @This();

pub fn init(id: u64) !Memtable {
    return .{
        .id = id,
        .index = try SkipList(KV, decode).init(),
        .byte_count = AtomicValue(usize).init(0),
        .flushed = AtomicValue(bool).init(false),
        .mutable = AtomicValue(bool).init(true),
    };
}

pub fn deinit(self: *Memtable) void {
    self.index.deinit();
    self.* = undefined;
}

pub fn getId(self: Memtable) u64 {
    return self.id;
}

pub fn put(self: *Memtable, item: KV) !void {
    if (self.frozen()) return error.MemtableImmutable;
    if (item.key.len == 0) return error.InvalidKey;

    try self.index.put(item.key, item.raw_bytes);

    _ = self.byte_count.fetchAdd(item.key.len + item.raw_bytes.len, .release);
}

pub fn get(self: *Memtable, user_key: []const u8) !?KV {
    if (user_key.len == 0) return null;

    return try self.index.get(user_key);
}

pub fn freeze(self: *Memtable) void {
    self.mutable.store(false, .release);
}

pub fn frozen(self: Memtable) bool {
    return !self.mutable.load(.acquire);
}

pub fn flush(self: *Memtable) void {
    self.flushed.store(true, .release);
}

pub fn isFlushed(self: Memtable) bool {
    return self.flushed.load(.acquire);
}

pub fn size(self: Memtable) usize {
    return self.byte_count.load(.acquire);
}

pub fn iterator(self: *Memtable, alloc: Allocator) !Iterator(KV) {
    return try self.index.iterator(alloc);
}

pub fn serialize(self: *Memtable, alloc: Allocator, file_path: []const u8) !void {
    if (self.size() == 0) return;

    const file = try std.fs.cwd().createFile(file_path, .{});
    defer file.close();

    var buffer: [1024]u8 = undefined;
    var writer = file.writer(&buffer).interface;
    defer writer.flush() catch {};

    const header = Header{
        .magic = MEMTABLE_MAGIC,
        .version = MEMTABLE_VERSION,
        .id_len = @sizeOf(u64),
        .byte_count = self.size(),
    };

    try writer.writeAll(std.mem.asBytes(&header));
    try writer.writeSliceEndian(u8, std.mem.asBytes(&self.id), Endian);
    try writer.writeSliceEndian(u8, std.mem.asBytes(&self.index.count), Endian);

    var it = try self.iterator(alloc);
    defer it.deinit();

    while (it.next()) |nxt| {
        try writer.writeSliceEndian(u8, std.mem.asBytes(&nxt.len()), Endian);
        try writer.writeAll(nxt.raw_bytes);
    }
}

pub fn deserialize(self: *Memtable, alloc: Allocator, file_path: []const u8) !void {
    _ = alloc;
    const file = try std.fs.cwd().openFile(file_path, .{});
    defer file.close();

    var buffer: [1024]u8 = undefined;
    var reader = file.reader(&buffer).interface;

    var header: Header = undefined;
    try reader.readSliceAll(std.mem.asBytes(&header));

    if (header.magic != MEMTABLE_MAGIC) {
        return error.InvalidMemtableFile;
    }

    if (header.version != MEMTABLE_VERSION) {
        return error.UnsupportedMemtableVersion;
    }

    try reader.readSliceEndian(u8, std.mem.asBytes(&self.id), Endian);

    self.*.index = try SkipList(KV, decode).init();

    self.mutable.store(true, .seq_cst);

    var entry_count: u64 = undefined;
    try reader.readSliceEndian(u8, std.mem.asBytes(&entry_count), Endian);

    var i: u32 = 0;
    while (i < entry_count) : (i += 1) {
        var kv_len: u64 = undefined;
        try reader.readSliceEndian(u8, std.mem.asBytes(&kv_len), Endian);

        var buf: [4096]u8 = undefined;
        try reader.readSliceAll(&buf);

        var kv: KV = undefined;
        try kv.decode(buf[0..kv_len]);

        try self.put(kv);
    }
}

test Memtable {
    const testing = std.testing;
    const alloc = testing.allocator;

    // given
    var mtable = try Memtable.init(0);
    defer mtable.deinit();

    // when
    var kv = try KV.init(alloc, "__key__", "__value__");
    defer kv.deinit(alloc);

    try mtable.put(kv);

    const actual = try mtable.get("__key__");

    // then
    try testing.expectEqualStrings(kv.value, actual.?.value);

    var kv2 = try KV.init(alloc, "__key__", "__updated_value__");
    defer kv2.deinit(alloc);

    try mtable.put(kv2);

    const latest = try mtable.get("__key__");

    try testing.expectEqualStrings("__updated_value__", latest.?.value);

    var it = try mtable.iterator(alloc);
    defer it.deinit();

    const nxt = it.next();

    try testing.expectEqualStrings("__updated_value__", nxt.?.value);
}
