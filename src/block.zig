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

/// BlockMeta represents metadata for a block containing key-value pairs.
///
/// ## Binary Format Layout
/// The metadata is serialized in the following binary format:
/// ```
/// [total_length][first_key_length][first_key_data][last_key_length][last_key_data]
/// ```
///
/// ## Field Details
/// - `total_length`: u64 - Total size of the entire metadata structure in bytes
/// - `first_key_length`: u64 - Length of the first key in bytes
/// - `first_key_data`: []u8 - Raw bytes of the first key
/// - `last_key_length`: u64 - Length of the last key in bytes
/// - `last_key_data`: []u8 - Raw bytes of the last key
///
/// ## Encoding
/// All integer fields are encoded as little-endian u64 values.
///
/// ## Usage
/// BlockMeta is used to store boundary information for blocks in the LSM tree,
/// enabling efficient range queries and block navigation without reading the
/// entire block contents.
///
/// ## Example
/// ```zig
/// const meta = BlockMeta.init("apple", "zebra");
/// const encoded = try meta.encodeAlloc(allocator);
/// defer allocator.free(encoded);
///
/// var decoded_meta = BlockMeta{};
/// try decoded_meta.decode(encoded);
/// ```
const BlockMeta = struct {
    len: u64,
    first_key: []const u8,
    last_key: []const u8,

    const Error = error{
        InvalidKeyLength,
        BufferTooSmall,
        EndOfStream,
        NoSpaceLeft,
    };

    pub fn init(first_key: []const u8, last_key: []const u8) BlockMeta {
        return .{
            .len = @sizeOf(u64) + @sizeOf(u64) + first_key.len + @sizeOf(u64) + last_key.len,
            .first_key = first_key,
            .last_key = last_key,
        };
    }

    pub fn decode(self: *BlockMeta, data: []const u8) Error!void {
        var stream = fixedBufferStream(data);
        var reader = stream.reader();

        self.len = try reader.readInt(u64, Endian);

        const first_key_len = try reader.readInt(u64, Endian);
        if (first_key_len == 0) return Error.InvalidKeyLength;

        self.first_key = stream.buffer[stream.pos..][0..first_key_len];
        stream.pos += first_key_len;

        const last_key_len = try reader.readInt(u64, Endian);
        if (last_key_len == 0) return Error.InvalidKeyLength;

        self.last_key = stream.buffer[stream.pos..][0..last_key_len];
    }

    pub fn encodeAlloc(self: BlockMeta, alloc: Allocator) ![]const u8 {
        const buf = try alloc.alloc(u8, self.len);
        return try self.encode(buf);
    }

    pub fn encode(self: BlockMeta, buf: []u8) Error![]u8 {
        if (self.len > buf.len) return Error.BufferTooSmall;

        var stream = fixedBufferStream(buf);
        var writer = stream.writer();

        try writer.writeInt(u64, self.len, Endian);
        try writer.writeInt(u64, self.first_key.len, Endian);
        _ = try writer.write(self.first_key);
        try writer.writeInt(u64, self.last_key.len, Endian);
        _ = try writer.write(self.last_key);

        return buf[0..self.len];
    }
};

/// Block represents a storage unit containing key-value pairs with metadata.
///
/// ## Binary Format Layout
/// The block is organized into three sections:
/// ```
/// [meta_data][offset_data][kv_data]
/// ```
///
/// ## Section Details
/// - `meta_data`: Contains block metadata including length, first/last keys, and count
///   - Format: [BlockMeta][count]
///   - BlockMeta: [total_length][first_key_length][first_key_data][last_key_length][last_key_data]
///   - count: u64 - Number of key-value pairs in the block
/// - `offset_data`: Array of u64 offsets pointing to each KV pair in kv_data section
///   - Format: [offset1][offset2]...[offsetN]
/// - `kv_data`: Serialized key-value pairs stored sequentially
///   - Format: [kv1][kv2]...[kvN]
///
/// ## Memory Layout
/// The block uses a fixed-size buffer divided into three sections:
/// - Meta section: 10% of total buffer size
/// - Offset section: 20% of total buffer size
/// - Data section: 70% of total buffer size
///
/// ## Encoding
/// All integer fields are encoded as little-endian u64 values.
///
/// ## Usage
/// Blocks are used as the fundamental storage unit in the LSM tree for organizing
/// and accessing key-value pairs efficiently. They support both reading from
/// existing data and writing new key-value pairs.
///
/// ## Example
/// ```zig
/// var stream = fixedBufferStream(buffer);
/// var block = try Block.init(allocator, &stream);
/// defer block.deinit();
///
/// const kv = try KV.initOwned(allocator, "key1", "value1");
/// const index = try block.write(kv);
/// const retrieved = try block.read(index);
///
/// try block.freeze(); // Finalize the block
/// ```
pub const Block = struct {
    alloc: Allocator,
    stream: *FixedBuffer([]align(PageSize) u8),
    byte_count: usize,
    count: u64,
    first_key: ?[]const u8,
    last_key: ?[]const u8,
    meta_section_start: usize,
    offset_section_start: usize,
    data_section_start: usize,
    meta_section_size: usize,
    offset_section_size: usize,
    data_section_size: usize,

    pub const Error = error{
        ReadError,
        WriteError,
        DataOverflow,
        OffsetOverflow,
        MetadataOverflow,
        IndexOutOfBounds,
    };

    pub fn init(allocator: Allocator, stream: *FixedBuffer([]align(PageSize) u8)) !*Block {
        const total_size = stream.buffer.len;
        const meta_size = total_size / 10;
        const offset_size = total_size / 5;
        const data_start = meta_size + offset_size;
        const data_size = total_size - data_start;

        const block = try allocator.create(Block);
        block.* = .{
            .alloc = allocator,
            .stream = stream,
            .byte_count = 0,
            .count = 0,
            .first_key = null,
            .last_key = null,
            .meta_section_start = 0,
            .offset_section_start = meta_size,
            .data_section_start = data_start,
            .meta_section_size = meta_size,
            .offset_section_size = offset_size,
            .data_section_size = data_size,
        };

        const has_existing_data = stream.buffer.len > 0 and
            @intFromPtr(stream.buffer.ptr) != 0 and
            stream.buffer[0] != 0;

        if (has_existing_data) {
            var meta = BlockMeta{ .len = 0, .first_key = "", .last_key = "" };
            meta.decode(stream.buffer[0..]) catch {
                stream.pos = data_start;
                return block;
            };

            const count_pos = meta.len;
            if (count_pos + @sizeOf(u64) <= stream.buffer.len) {
                const count_bytes = stream.buffer[count_pos .. count_pos + @sizeOf(u64)];
                block.count = std.mem.readInt(u64, count_bytes[0..@sizeOf(u64)], Endian);
                block.first_key = meta.first_key;
                block.last_key = meta.last_key;

                block.byte_count = meta.len + @sizeOf(u64) + (block.count * @sizeOf(u64));

                stream.pos = data_start;

                if (block.count > 0) {
                    const last_offset_pos = block.offset_section_start + ((block.count - 1) * @sizeOf(u64));
                    if (last_offset_pos + @sizeOf(u64) <= stream.buffer.len) {
                        const last_offset_bytes = stream.buffer[last_offset_pos .. last_offset_pos + @sizeOf(u64)];
                        const last_data_offset = std.mem.readInt(u64, last_offset_bytes[0..@sizeOf(u64)], Endian);

                        const last_kv_pos = data_start + last_data_offset;
                        if (last_kv_pos < stream.buffer.len) {
                            const last_kv = keyvalue.decode(stream.buffer[last_kv_pos..]) catch {
                                stream.pos = data_start;
                                return block;
                            };
                            stream.pos = last_kv_pos + last_kv.raw_bytes.len;
                            block.byte_count = stream.pos - block.meta_section_start;
                        }
                    }
                }
            }
        } else {
            stream.pos = block.data_section_start;
        }

        return block;
    }

    pub fn deinit(self: *Block) void {
        self.* = undefined;
    }

    pub fn write(self: *Block, kv: KV) Error!usize {
        const kv_size = kv.raw_bytes.len;
        const offset_size = @sizeOf(u64);
        const current_pos = self.stream.pos;
        const offset_pos = self.offset_section_start + (self.count * offset_size);

        if (current_pos < self.data_section_start) {
            self.stream.pos = self.data_section_start;
        }

        const adjusted_pos = @max(current_pos, self.data_section_start);

        if (adjusted_pos + kv_size > self.data_section_start + self.data_section_size or
            offset_pos + offset_size > self.offset_section_start + self.offset_section_size)
        {
            return Error.DataOverflow;
        }

        const data_offset = adjusted_pos - self.data_section_start;
        self.stream.pos = adjusted_pos;
        const bytes_written = self.stream.write(kv.raw_bytes) catch return Error.WriteError;

        const saved_pos = self.stream.pos;
        self.stream.pos = offset_pos;
        writeInt(u64, self.stream.buffer[offset_pos..][0..offset_size], data_offset, Endian);
        self.stream.pos = saved_pos;

        if (self.first_key == null) self.first_key = kv.key;
        self.last_key = kv.key;
        self.count += 1;
        self.byte_count += bytes_written + offset_size;

        const block_meta = BlockMeta.init(self.first_key.?, self.last_key.?);
        const meta_data = block_meta.encodeAlloc(self.alloc) catch return Error.MetadataOverflow;
        defer self.alloc.free(meta_data);

        if (meta_data.len + offset_size > self.meta_section_size) {
            return Error.MetadataOverflow;
        }

        self.stream.pos = self.meta_section_start;
        _ = self.stream.write(meta_data) catch return Error.WriteError;
        writeInt(u64, self.stream.buffer[self.stream.pos..][0..offset_size], self.count, Endian);
        self.stream.pos = saved_pos;

        return self.count - 1;
    }

    pub fn read(self: *Block, kv_index: usize) Error!KV {
        if (kv_index >= self.count) {
            return Error.IndexOutOfBounds;
        }

        const offset_pos = self.offset_section_start + (kv_index * @sizeOf(u64));
        if (offset_pos + @sizeOf(u64) > self.offset_section_start + self.offset_section_size) {
            return Error.OffsetOverflow;
        }

        const offset_bytes = self.stream.buffer[offset_pos .. offset_pos + @sizeOf(u64)];
        const data_offset = std.mem.readInt(u64, offset_bytes[0..@sizeOf(u64)], Endian);

        const kv_pos = self.data_section_start + data_offset;
        if (kv_pos >= self.data_section_start + self.data_section_size) {
            return Error.ReadError;
        }

        return keyvalue.decode(self.stream.buffer[kv_pos..]) catch Error.ReadError;
    }

    pub fn size(self: Block) u64 {
        return self.byte_count;
    }

    pub fn flush(self: *Block) usize {
        return self.byte_count;
    }


    pub fn decode(self: *Block, stream: *FixedBuffer([]align(PageSize) u8)) Error!usize {
        var meta = BlockMeta{ .len = 0, .first_key = "", .last_key = "" };
        meta.decode(stream.buffer) catch return Error.ReadError;

        const count_pos = meta.len;
        if (count_pos + @sizeOf(u64) > stream.buffer.len) {
            return Error.ReadError;
        }

        const count_bytes = stream.buffer[count_pos .. count_pos + @sizeOf(u64)];
        self.count = std.mem.readInt(u64, count_bytes[0..@sizeOf(u64)], Endian);
        self.first_key = meta.first_key;
        self.last_key = meta.last_key;

        const total_decoded = meta.len + @sizeOf(u64);
        self.byte_count = total_decoded + (self.count * @sizeOf(u64));

        return total_decoded;
    }
};

test Block {
    const testing = std.testing;
    const test_dir = testing.tmpDir(.{});
    const allocator = testing.allocator;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const pathname = try test_dir.dir.realpathAlloc(alloc, ".");
    defer alloc.free(pathname);
    defer test_dir.dir.deleteTree(pathname) catch unreachable;

    const dat_file = try test_dir.dir.createFile("data.dat", .{ .read = true });
    errdefer dat_file.close();

    try dat_file.setEndPos(PageSize);

    const stat = try dat_file.stat();
    const file_size = stat.size;

    var iostream = try MMap.init(alloc, file_size);
    errdefer {
        iostream.deinit();
        alloc.destroy(iostream);
    }

    try iostream.connect(dat_file, 0);
    iostream.buf.reset();

    {
        const expected = try KV.initOwned(alloc, "__key__", "__value__");

        // given
        var block = try Block.init(alloc, &iostream.buf);
        defer alloc.destroy(block);
        defer block.deinit();

        // when
        const idx = try block.write(expected);
        const item = try block.read(idx);

        // then
        try testing.expectEqualStrings(expected.key, item.key);
        try testing.expectEqualStrings(expected.value, item.value);
        try testing.expectEqual(expected.len(), item.len());
    }
}
