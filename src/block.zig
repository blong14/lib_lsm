const std = @import("std");

const keyvalue = @import("kv.zig");
const map = @import("mmap.zig");
const options = @import("opts.zig");

const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const FixedBuffer = std.io.FixedBufferStream;

const KV = keyvalue.KV;
const MMap = map.AppendOnlyMMap;
const Opts = options.Opts;

const Endian = std.builtin.Endian.little;
const PageSize = std.mem.page_size;

const assert = std.debug.assert;
const bufferedWriter = std.io.bufferedWriter;
const fixedBufferStream = std.io.fixedBufferStream;
const print = std.debug.print;
const writeInt = std.mem.writeInt;

/// meta_data[len,kvlen1,key,kvlen2,key]
const BlockMeta = struct {
    len: u64,
    first_key: []const u8,
    last_key: []const u8,

    const Self = @This();

    pub fn init(first_key: []const u8, last_key: []const u8) Self {
        return .{
            .len = @sizeOf(u64) + @sizeOf(u64) + first_key.len + @sizeOf(u64) + last_key.len,
            .first_key = first_key,
            .last_key = last_key,
        };
    }

    pub fn decode(self: *Self, data: []const u8) !void {
        var stream = fixedBufferStream(data);
        var data_reader = stream.reader();

        self.*.len = try data_reader.readInt(u64, Endian);

        const first_key_len = try data_reader.readInt(u64, Endian);
        assert(first_key_len > 0);

        self.*.first_key = stream.buffer[stream.pos..][0..first_key_len];

        stream.pos += first_key_len;

        const last_key_len = try data_reader.readInt(u64, Endian);
        assert(last_key_len > 0);

        self.*.last_key = stream.buffer[stream.pos..][0..last_key_len];
    }

    pub fn encodeAlloc(self: Self, alloc: Allocator) ![]const u8 {
        const buf = try alloc.alloc(u8, self.len);
        return try self.encode(buf);
    }

    pub fn encode(self: Self, buf: []u8) ![]u8 {
        assert(self.len <= buf.len);

        var stream = fixedBufferStream(buf);
        var data_writer = stream.writer();

        try data_writer.writeInt(u64, self.len, Endian);
        try data_writer.writeInt(u64, self.first_key.len, Endian);
        _ = try data_writer.write(self.first_key);
        try data_writer.writeInt(u64, self.last_key.len, Endian);
        _ = try data_writer.write(self.last_key);

        return buf[0..self.len];
    }
};

/// block[meta_data, data]
/// meta_data[len,kvlen1,key,kvlen2,key]
/// data[count,offset1,offset2,kv1,kv2]
pub const Block = struct {
    alloc: Allocator,
    byte_count: usize,
    capacity: u64,
    count: u64,
    meta_data: ArrayList(u8),
    offset_data: ArrayList(u8),
    data: ArrayList(u8),

    const Self = @This();

    const Error = error{
        BlockReadError,
        BlockWriteError,
    };

    pub fn init(allocator: Allocator, capacity: u64) !*Self {
        const meta_data = try ArrayList(u8).initCapacity(allocator, capacity);
        const offset_data = try ArrayList(u8).initCapacity(allocator, capacity);
        const block_data = try ArrayList(u8).initCapacity(allocator, capacity);

        const block = try allocator.create(Self);
        block.* = .{
            .alloc = allocator,
            .byte_count = 0,
            .capacity = capacity,
            .count = 0,
            .meta_data = meta_data,
            .offset_data = offset_data,
            .data = block_data,
        };
        return block;
    }

    pub fn deinit(self: *Self) void {
        self.meta_data.deinit();
        self.offset_data.deinit();
        self.data.deinit();
        self.* = undefined;
    }

    pub fn blockMeta(self: Self) !BlockMeta {
        var meta_data: BlockMeta = undefined;
        try meta_data.decode(self.meta_data.items);
        return meta_data;
    }

    pub fn read(self: *Self, idx: usize) !KV {
        var kv: KV = undefined;
        try kv.decode(self.data.items[idx..]);
        return kv;
    }

    pub fn write(self: *Self, kv: *const KV) !usize {
        var offset_data_writer = self.offset_data.writer();

        const offset = self.data.items.len;
        try offset_data_writer.writeInt(u64, offset, Endian);

        const data_writer = self.data.writer();

        const encoded_kv = try kv.encodeAlloc(self.alloc);
        defer self.alloc.free(encoded_kv);

        const byte_count = try data_writer.write(encoded_kv);
        assert(byte_count == encoded_kv.len);

        self.*.count += 1;
        self.*.byte_count += @sizeOf(u64) + byte_count;

        return offset;
    }

    pub fn size(self: Self) u64 {
        const meta_data_count = self.meta_data.items.len;
        const offset_count = self.offset_data.items.len;
        const data_count = self.data.items.len;

        return @sizeOf(u64) + meta_data_count + @sizeOf(u64) + offset_count + data_count;
    }

    pub fn flush(self: *Self, stream: *FixedBuffer([]align(PageSize) u8)) !usize {
        const total_bytes = self.size();
        if (total_bytes > stream.buffer.len) {
            print(
                "flush error: trying to write {d} bytes into {d}\n",
                .{ total_bytes, stream.buffer.len },
            );
            return error.BlockWriteError;
        }

        var block_size: [@divExact(@typeInfo(u64).Int.bits, 8)]u8 = undefined;
        writeInt(std.math.ByteAlignedInt(u64), &block_size, total_bytes, Endian);

        var count: [@divExact(@typeInfo(u64).Int.bits, 8)]u8 = undefined;
        writeInt(std.math.ByteAlignedInt(u64), &count, self.count, Endian);

        var bytes = try stream.write(&block_size);
        bytes += try stream.write(self.meta_data.items);
        bytes += try stream.write(&count);
        bytes += try stream.write(self.offset_data.items);
        bytes += try stream.write(self.data.items);

        return bytes;
    }

    pub fn freeze(self: *Self) !void {
        var offset_data_stream = fixedBufferStream(self.offset_data.items);
        var offset_data_reader = offset_data_stream.reader();

        const first_kv = try self.read(0);

        try offset_data_reader.skipBytes((self.count - 1) * @sizeOf(u64), .{});
        const last_key_offset = try offset_data_reader.readInt(u64, Endian);
        const last_kv = try self.read(last_key_offset);

        const block_meta_data = BlockMeta.init(first_kv.key, last_kv.key);

        const meta_data = try block_meta_data.encodeAlloc(self.alloc);
        defer self.alloc.free(meta_data);

        try self.meta_data.appendSlice(meta_data);
    }

    pub fn decode(self: *Self, stream: *FixedBuffer([]align(PageSize) u8)) !usize {
        var stream_reader = stream.reader();

        try stream_reader.skipBytes(@sizeOf(u64), .{});

        const meta_data_len = try stream_reader.readInt(u64, Endian);
        assert(meta_data_len > 0);

        const meta_data = stream.buffer[stream.pos..][0..meta_data_len];

        try self.meta_data.appendSlice(meta_data);

        stream.pos += meta_data_len;

        self.*.count = try stream_reader.readInt(u64, Endian);

        const offset_len = self.count * @sizeOf(u64);
        const offset_data = stream.buffer[stream.pos..][0..offset_len];

        try self.offset_data.appendSlice(offset_data);

        stream.pos += offset_len;

        try self.data.appendSlice(stream.buffer[stream.pos..]);

        return self.size();
    }
};

test Block {
    const testing = std.testing;
    const alloc = testing.allocator;

    {
        const expected = KV.init("__key__", "__value__");

        // given
        var block = try Block.init(alloc, PageSize);
        defer alloc.destroy(block);
        defer block.deinit();

        // when
        const idx = try block.write(&expected);
        const item = try block.read(idx);

        // then
        try testing.expectEqualStrings(expected.key, item.key);
        try testing.expectEqualStrings(expected.value, item.value);
        try testing.expectEqual(expected.len(), item.len());
    }

    {
        // given
        var block = try Block.init(alloc, PageSize);
        defer alloc.destroy(block);
        defer block.deinit();

        const first_kv = KV.init("__key__", "__value__");
        const last_kv = KV.init("__key_2__", "__value_2__");

        // when
        _ = try block.write(&first_kv);
        _ = try block.write(&last_kv);

        try block.freeze();

        // then
        const meta_data = try block.blockMeta();

        try testing.expectEqualStrings(first_kv.key, meta_data.first_key);
        try testing.expectEqualStrings(last_kv.key, meta_data.last_key);
    }
}
