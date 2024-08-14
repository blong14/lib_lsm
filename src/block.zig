const std = @import("std");

const keyvalue = @import("kv.zig");
const map = @import("mmap.zig");
const options = @import("opts.zig");

const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const FixedBuffer = std.io.FixedBufferStream;
const StringHashMap = std.StringHashMap;

const KV = keyvalue.KV;
const MMap = map.AppendOnlyMMap;
const Opts = options.Opts;

const assert = std.debug.assert;
const print = std.debug.print;

/// [meta,data]
/// sstable -> [meta[offset,kv,kv]block[count,offset1,offset2,kv1,kv2],[meta[offset,kv,kv]block[count,offset1,offset2,kv1,kv2]
const BlockMeta = struct {
    len: u64,
    first_key: []const u8,
    last_key: []const u8,

    const Self = @This();

    pub fn decode(self: *Self, data: []const u8) !void {
        var stream = std.io.fixedBufferStream(data);

        var data_reader = stream.reader();
        self.*.len = try data_reader.readInt(u64, std.builtin.Endian.little);

        const first_key_len = try data_reader.readInt(u64, std.builtin.Endian.little);
        assert(first_key_len > 0);

        self.*.first_key = stream.buffer[stream.pos..][0..first_key_len];

        stream.pos += first_key_len;

        const last_key_len = try data_reader.readInt(u64, std.builtin.Endian.little);
        assert(last_key_len > 0);

        self.*.last_key = stream.buffer[stream.pos..][0..last_key_len];
    }

    pub fn encodeAlloc(self: Self, alloc: Allocator) ![]const u8 {
        const buf = try alloc.alloc(u8, self.len);
        return try self.encode(buf);
    }

    pub fn encode(self: Self, buf: []u8) ![]u8 {
        assert(self.len <= buf.len);
        var stream = std.io.fixedBufferStream(buf);

        var data_writer = stream.writer();
        try data_writer.writeInt(u64, self.len, std.builtin.Endian.little);
        try data_writer.writeInt(u64, self.first_key.len, std.builtin.Endian.little);
        _ = try data_writer.write(self.first_key);
        try data_writer.writeInt(u64, self.last_key.len, std.builtin.Endian.little);
        _ = try data_writer.write(self.last_key);

        return buf[0..self.len];
    }
};

/// block -> [meta[offset,kv,kv]block[count,offset1,offset2,kv1,kv2]
pub const Block = struct {
    alloc: Allocator,
    byte_count: usize,
    count: u16,
    connected: bool,
    meta: ArrayList(u8),
    offset: ArrayList(u8),
    data: ArrayList(u8),
    index: StringHashMap(u64),
    mmap: *MMap,

    const Self = @This();

    const Error = error{
        BlockReadError,
        BlockWriteError,
    };

    pub fn init(allocator: Allocator) !*Self {
        const meta = try ArrayList(u8).initCapacity(allocator, std.mem.page_size);
        const offset = try ArrayList(u8).initCapacity(allocator, std.mem.page_size);
        const block_data = try ArrayList(u8).initCapacity(allocator, std.mem.page_size);
        const index = StringHashMap(u64).init(allocator);

        const block = try allocator.create(Self);
        block.* = .{
            .alloc = allocator,
            .byte_count = 0,
            .count = 0,
            .connected = false,
            .meta = meta,
            .offset = offset,
            .data = block_data,
            .index = index,
            .mmap = undefined,
        };
        return block;
    }

    pub fn deinit(self: *Self) void {
        self.meta.deinit();
        self.offset.deinit();
        self.data.deinit();
        self.index.deinit();
        if (self.connected) {
            self.mmap.deinit();
            self.alloc.destroy(self.mmap);
        }
        self.* = undefined;
    }

    pub fn open(self: *Self, file: std.fs.File, start: u64) !void {
        self.*.mmap = try MMap.init(self.alloc, std.mem.page_size);
        try self.mmap.connect(file, start);
        self.connected = true;
    }

    pub fn initBlockMeta(self: *Self) !void {
        var offset_stream = std.io.fixedBufferStream(self.offset.items);
        var offset_reader = offset_stream.reader();

        try offset_reader.skipBytes((self.count - 1) * @sizeOf(u64), .{});

        const last_key_offset = try offset_reader.readInt(u64, std.builtin.Endian.little);

        var first_kv: KV = undefined;
        try self.read(0, &first_kv);

        var last_kv: KV = undefined;
        try self.read(last_key_offset, &last_kv);

        const block_meta = BlockMeta{
            .len = self.byte_count,
            .first_key = first_kv.key,
            .last_key = last_kv.key,
        };

        const meta = try block_meta.encodeAlloc(self.alloc);
        defer self.alloc.free(meta);

        try self.meta.appendSlice(meta);
    }

    pub fn blockMeta(self: Self) !BlockMeta {
        var meta: BlockMeta = undefined;
        try meta.decode(self.meta.items);
        return meta;
    }

    pub fn read(self: *Self, idx: usize, kv: *KV) !void {
        try kv.decode(self.data.items[idx..]);
    }

    pub fn write(self: *Self, kv: *const KV) !usize {
        // [1 1 1 1 _ _ _ _]
        var offset_writer = self.offset.writer();
        const offset = self.data.items.len;
        try offset_writer.writeInt(u64, offset, std.builtin.Endian.little);

        const encoded_kv = try kv.encodeAlloc(self.alloc);
        defer self.alloc.free(encoded_kv);

        const data_writer = self.data.writer();
        const byte_count = try data_writer.write(encoded_kv);
        assert(byte_count == encoded_kv.len);

        self.*.count += 1;
        self.*.byte_count += @sizeOf(u64) + byte_count;

        return offset;
    }

    inline fn xflush(self: Self, data: *FixedBuffer([]align(std.mem.page_size) u8)) !usize {
        var count: [@divExact(@typeInfo(u64).Int.bits, 8)]u8 = undefined;
        std.mem.writeInt(std.math.ByteAlignedInt(u64), &count, self.count, std.builtin.Endian.little);

        var buf = data.writer();

        var bytes = try buf.write(self.meta.items);
        bytes += try buf.write(&count);
        bytes += try buf.write(self.offset.items);
        bytes += try buf.write(self.data.items);

        return bytes;
    }

    pub fn flush(self: *Self) !usize {
        try self.initBlockMeta();
        return try self.xflush(&self.mmap.buf);
    }
};

test Block {
    const testing = std.testing;
    const alloc = testing.allocator;

    const expected = try KV.init(alloc, "__key__", "__value__");
    defer alloc.destroy(expected);

    // given
    var block = try Block.init(alloc);
    defer alloc.destroy(block);
    defer block.deinit();

    {
        // when
        const idx = try block.write(expected);

        var item: KV = undefined;
        try block.read(idx, &item);

        // then
        try testing.expectEqualStrings(expected.key, item.key);
        try testing.expectEqualStrings(expected.value, item.value);
        try testing.expectEqual(expected.len, item.len);
    }

    {
        // when
        const new_kv = try KV.init(alloc, "__key_2__", "__value_2__");
        defer alloc.destroy(new_kv);

        _ = try block.write(new_kv);
        try block.initBlockMeta();

        // then
        const meta = try block.blockMeta();

        try testing.expectEqualStrings(expected.key, meta.first_key);
        try testing.expectEqualStrings(new_kv.key, meta.last_key);
    }
}
