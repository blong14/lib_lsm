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

    const file = try std.fs.cwd().createFile(file_path, .{ .read = true });
    defer {
        file.sync() catch undefined;
        file.close();
    }

    const header = Header{
        .magic = MEMTABLE_MAGIC,
        .version = MEMTABLE_VERSION,
        .id_len = @sizeOf(u64),
        .byte_count = self.size(),
    };
    _ = try file.write(std.mem.asBytes(&header));

    var id_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &id_buf, self.id, Endian);
    _ = try file.write(&id_buf);

    var count_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &count_buf, self.index.count, Endian);
    _ = try file.write(&count_buf);

    var it = try self.iterator(alloc);
    defer it.deinit();

    while (it.next()) |nxt| {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, nxt.len(), Endian);
        _ = try file.write(&buf);

        _ = try file.write(nxt.raw_bytes);
    }
}

pub fn deserialize(self: *Memtable, alloc: Allocator, file_path: []const u8) !void {
    const file = try std.fs.cwd().openFile(file_path, .{ .mode = .read_write });
    defer file.close();

    const stat = try file.stat();

    std.log.debug("opening {s} stat {d}", .{ file_path, stat.size });

    var header: Header = undefined;
    _ = try file.read(std.mem.asBytes(&header));

    if (header.magic != MEMTABLE_MAGIC) {
        return error.InvalidMemtableFile;
    }

    if (header.version != MEMTABLE_VERSION) {
        return error.UnsupportedMemtableVersion;
    }

    var id_buf: [8]u8 = undefined;
    _ = try file.read(&id_buf);

    self.id = std.mem.readInt(u64, &id_buf, Endian);

    self.*.index = try SkipList(KV, decode).init();

    self.mutable.store(true, .seq_cst);

    var entry_buf: [8]u8 = undefined;
    _ = try file.read(&entry_buf);

    const entry_count = std.mem.readInt(u64, &entry_buf, Endian);

    var kv_len_buf: [8]u8 = undefined;

    var i: u32 = 0;
    while (i < entry_count) : (i += 1) {
        _ = try file.read(&kv_len_buf);

        const kv_len = std.mem.readInt(u64, &kv_len_buf, Endian);

        const buf = try alloc.alloc(u8, kv_len);
        _ = try file.read(buf);

        var kv: KV = undefined;
        kv.decode(buf) catch |err| switch (err) {
            error.InvalidKeyLength => {
                std.log.debug(
                    "invalid key length ({d}), skipping...",
                    .{kv_len},
                );
                continue;
            },
            else => return err,
        };

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
