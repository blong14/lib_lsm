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

/// block[meta_data, offset_data, kv_data]
/// meta_data[len,kvlen1,key,kvlen2,key,count]
/// offset_data[offset1,offset2,...]
/// kv_data[kv1,kv2,...]
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
        NoKeysStored,
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

        // Check if the stream has existing data (frozen block from disk)
        // Only check if buffer is properly initialized and has data
        const has_existing_data = stream.buffer.len > 0 and
            @intFromPtr(stream.buffer.ptr) != 0 and
            stream.buffer[0] != 0;

        if (has_existing_data) {
            // Try to decode existing metadata
            var meta = BlockMeta{ .len = 0, .first_key = "", .last_key = "" };
            meta.decode(stream.buffer[0..]) catch {
                // If decode fails, treat as empty block
                stream.pos = data_start;
                return block;
            };

            // Read the count from after the metadata
            const count_pos = meta.len;
            if (count_pos + @sizeOf(u64) <= stream.buffer.len) {
                const count_bytes = stream.buffer[count_pos .. count_pos + @sizeOf(u64)];
                block.count = std.mem.readInt(u64, count_bytes[0..@sizeOf(u64)], Endian);
                block.first_key = meta.first_key;
                block.last_key = meta.last_key;

                // Calculate byte count based on existing data
                block.byte_count = meta.len + @sizeOf(u64) + (block.count * @sizeOf(u64));

                // Set stream position to end of data for potential writes
                stream.pos = data_start;

                // Find the actual end of data by reading offsets
                if (block.count > 0) {
                    const last_offset_pos = block.offset_section_start + ((block.count - 1) * @sizeOf(u64));
                    if (last_offset_pos + @sizeOf(u64) <= stream.buffer.len) {
                        const last_offset_bytes = stream.buffer[last_offset_pos .. last_offset_pos + @sizeOf(u64)];
                        const last_data_offset = std.mem.readInt(u64, last_offset_bytes[0..@sizeOf(u64)], Endian);

                        // Read the last KV to determine its size
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
            // For new blocks, set the stream position to the data section start
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

        // Ensure we're writing in the data section
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
        // Make sure we're at the right position before writing
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

    pub fn freeze(self: *Block) Error!void {
        if (self.first_key == null or self.last_key == null) {
            return Error.NoKeysStored;
        }

        const block_meta_data = BlockMeta.init(self.first_key.?, self.last_key.?);
        const meta_data = block_meta_data.encodeAlloc(self.alloc) catch return Error.MetadataOverflow;
        defer self.alloc.free(meta_data);

        if (meta_data.len > self.meta_section_size) {
            return Error.MetadataOverflow;
        }

        const saved_pos = self.stream.pos;
        self.stream.pos = self.meta_section_start;
        _ = self.stream.write(meta_data) catch return Error.WriteError;

        var count_bytes: [@sizeOf(u64)]u8 = undefined;
        writeInt(u64, &count_bytes, self.count, Endian);
        _ = self.stream.write(&count_bytes) catch return Error.WriteError;

        self.stream.pos = saved_pos;
    }

    pub fn decode(self: *Block, stream: *FixedBuffer([]align(PageSize) u8)) Error!usize {
        // Decode metadata from the beginning of the stream
        var meta = BlockMeta{ .len = 0, .first_key = "", .last_key = "" };
        meta.decode(stream.buffer) catch return Error.ReadError;

        // Read count after metadata
        const count_pos = meta.len;
        if (count_pos + @sizeOf(u64) > stream.buffer.len) {
            return Error.ReadError;
        }

        const count_bytes = stream.buffer[count_pos .. count_pos + @sizeOf(u64)];
        self.count = std.mem.readInt(u64, count_bytes[0..@sizeOf(u64)], Endian);
        self.first_key = meta.first_key;
        self.last_key = meta.last_key;

        // Calculate total bytes decoded
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
