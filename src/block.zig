const std = @import("std");

const keyvalue = @import("kv.zig");
const map = @import("mmap.zig");
const options = @import("opts.zig");

const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;

const KV = keyvalue.KV;
const MMap = map.AppendOnlyMMap;
const Opts = options.Opts;

const Endian = std.builtin.Endian.little;
const PageSize = std.mem.page_size;

const assert = std.debug.assert;
const print = std.debug.print;
const copyBackwards = std.mem.copyBackwards;
const fixedBufferStream = std.io.fixedBufferStream;
const writeInt = std.mem.writeInt;
const readInt = std.mem.readInt;

/// BlockMeta represents metadata for a block containing key-value pairs.
const BlockMeta = struct {
    first_key: []const u8,
    last_key: []const u8,
    count: u64,

    const Self = @This();

    pub fn encode(self: Self, writer: anytype) !void {
        try writer.writeInt(u64, self.first_key.len, Endian);
        try writer.writeAll(self.first_key);
        try writer.writeInt(u64, self.last_key.len, Endian);
        try writer.writeAll(self.last_key);
        try writer.writeInt(u64, self.count, Endian);
    }

    pub fn decode(self: *Self, data: []const u8) !void {
        const max_key_size = 1024 * 1024; // 1MB

        var offset: usize = 0;

        const first_key_len = readInt(u64, data[offset..][0..@sizeOf(u64)], Endian);
        if (first_key_len == 0 or first_key_len > max_key_size) {
            std.debug.print("block meta decode key len error\n", .{});
            return error.InvalidKeyLength;
        }

        offset += @sizeOf(u64);
        if (offset + first_key_len > data.len) {
            return error.InvalidData;
        }

        self.*.first_key = data[offset .. offset + first_key_len];

        offset += first_key_len;
        if (offset + @sizeOf(u64) > data.len) {
            return error.InvalidData;
        }

        const last_key_len = readInt(u64, data[offset..][0..@sizeOf(u64)], Endian);
        if (last_key_len == 0 or last_key_len > max_key_size) {
            std.debug.print("block meta decode key len error\n", .{});
            return error.InvalidKeyLength;
        }

        offset += @sizeOf(u64);
        if (offset + @sizeOf(u64) > data.len) {
            return error.InvalidData;
        }

        self.*.last_key = data[offset .. offset + last_key_len];

        offset += last_key_len;
        if (offset + @sizeOf(u64) > data.len) {
            return error.InvalidData;
        }

        self.*.count = readInt(u64, data[offset..][0..@sizeOf(u64)], Endian);
    }

    pub fn encodedSize(self: Self) usize {
        return @sizeOf(u64) * 3 + self.first_key.len + self.last_key.len;
    }
};

/// Block represents a storage unit containing key-value pairs with metadata.
/// Similar to FixedBufferStream but specialized for KV operations.
pub const Block = struct {
    /// Buffer containing the block data
    buffer: []align(PageSize) u8,
    /// Current position for writing
    pos: usize,
    /// Number of KV pairs stored
    count: u64,
    /// First key in the block (for range queries)
    first_key: ?[]const u8,
    /// Last key in the block (for range queries)
    last_key: ?[]const u8,

    pub const ReadError = error{
        IndexOutOfBounds,
        CorruptedData,
        InvalidOffset,
    };

    pub const WriteError = error{
        NoSpaceLeft,
        BufferFull,
    };

    const Self = @This();

    /// Initialize a new block with the given allocator and buffer
    pub fn init(allocator: Allocator, buffer: []align(PageSize) u8) Self {
        _ = allocator; // allocator parameter for API compatibility
        return .{
            .buffer = buffer,
            .pos = 0,
            .count = 0,
            .first_key = null,
            .last_key = null,
        };
    }

    /// Initialize a block from existing data
    pub fn initFromData(alloc: Allocator, buffer: []align(PageSize) u8) !*Self {
        const blck = try alloc.create(Self);
        blck.* = Self.init(alloc, buffer);

        if (buffer.len == 0) return blck;

        var meta: BlockMeta = undefined;
        try meta.decode(buffer);

        blck.*.count = meta.count;
        blck.*.first_key = meta.first_key;
        blck.*.last_key = meta.last_key;

        // Skip to end of data
        blck.*.pos = buffer.len - (blck.count * @sizeOf(u64));

        return blck;
    }

    /// Write a KV pair to the block
    pub fn write(self: *Self, kv: KV) WriteError!usize {
        if (kv.raw_bytes.len == 0) return WriteError.BufferFull;

        const kv_size = kv.raw_bytes.len;
        const offset_size = @sizeOf(u64);
        const meta_size = if (self.count == 0) self.estimateMetaSize(kv.key, kv.key) else 0;

        if (self.pos + kv_size + offset_size + meta_size > self.buffer.len) {
            return WriteError.NoSpaceLeft;
        }

        const kv_offset = self.pos;

        @memcpy(self.buffer[self.pos..][0..kv_size], kv.raw_bytes);
        self.pos += kv_size;

        const offset_pos = self.buffer.len - ((self.count + 1) * offset_size);
        writeInt(u64, self.buffer[offset_pos..][0..offset_size], kv_offset, Endian);

        if (self.first_key == null) self.*.first_key = kv.key;
        self.*.last_key = kv.key;

        const index = self.count;
        self.count += 1;

        return index;
    }

    /// Read a KV pair by index
    pub fn read(self: *const Self, index: usize) ReadError!KV {
        if (index >= self.count) {
            return ReadError.IndexOutOfBounds;
        }

        const offset_size = @sizeOf(u64);
        const offset_pos = self.buffer.len - ((index + 1) * offset_size);

        if (offset_pos < self.pos) {
            return ReadError.InvalidOffset;
        }

        const offset_bytes = self.buffer[offset_pos..][0..offset_size];
        const kv_offset = readInt(u64, offset_bytes, Endian);

        if (kv_offset >= self.pos) {
            return ReadError.InvalidOffset;
        }

        return keyvalue.decode(self.buffer[kv_offset..]) catch ReadError.CorruptedData;
    }

    /// Get the number of KV pairs in the block
    pub fn len(self: Self) u64 {
        return self.count;
    }

    /// Get the current size of data in the block
    pub fn size(self: Self) usize {
        return self.pos + (self.count * @sizeOf(u64));
    }

    /// Check if the block is empty
    pub fn isEmpty(self: Self) bool {
        return self.count == 0;
    }

    /// Reset the block to empty state
    pub fn reset(self: *Self) void {
        self.pos = 0;
        self.count = 0;
        self.first_key = null;
        self.last_key = null;
    }

    /// Flush metadata to the beginning of the buffer
    pub fn flush(self: *Self) WriteError!void {
        if (self.count == 0) return;

        const meta = BlockMeta{
            .first_key = self.first_key.?,
            .last_key = self.last_key.?,
            .count = self.count,
        };

        const meta_size = meta.encodedSize();
        if (meta_size == 0 or (meta_size > self.buffer.len - self.size())) {
            return WriteError.NoSpaceLeft;
        }

        const data_size = self.pos;
        copyBackwards(u8, self.buffer[meta_size..][0..data_size], self.buffer[0..data_size]);

        // Write metadata at the beginning
        var stream = fixedBufferStream(self.buffer[0..meta_size]);

        const writer = stream.writer();
        try meta.encode(writer);

        const offset_size = @sizeOf(u64);
        for (0..self.count) |i| {
            const offset_pos = self.buffer.len - ((i + 1) * offset_size);
            const offset_bytes = self.buffer[offset_pos..][0..offset_size];
            const old_offset = readInt(u64, offset_bytes, Endian);
            writeInt(u64, offset_bytes, old_offset + meta_size, Endian);
        }

        self.pos += meta_size;
    }

    /// Cleanup method for API compatibility with sstable.zig
    /// Block doesn't allocate memory, so this is a no-op
    pub fn deinit(self: *Self) void {
        _ = self; // Block manages a provided buffer, no cleanup needed
    }

    fn estimateMetaSize(self: Self, first_key: []const u8, last_key: []const u8) usize {
        _ = self;
        return @sizeOf(u64) * 3 + first_key.len + last_key.len;
    }
};

/// Create a block with the given buffer
pub fn block(buffer: []align(PageSize) u8) Block {
    return Block.init(std.heap.page_allocator, buffer);
}

test "Block basic operations" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var buffer: [PageSize]u8 align(PageSize) = undefined;
    var test_block = block(&buffer);

    var kv1 = try KV.initOwned(allocator, "key1", "value1");
    defer kv1.deinit(allocator);

    var kv2 = try KV.initOwned(allocator, "key2", "value2");
    defer kv2.deinit(allocator);

    // Test writing
    const idx1 = try test_block.write(kv1);
    const idx2 = try test_block.write(kv2);

    try testing.expectEqual(@as(usize, 0), idx1);
    try testing.expectEqual(@as(usize, 1), idx2);
    try testing.expectEqual(@as(u64, 2), test_block.len());

    // Test reading
    const read_kv1 = try test_block.read(0);
    const read_kv2 = try test_block.read(1);

    try testing.expectEqualStrings(kv1.key, read_kv1.key);
    try testing.expectEqualStrings(kv1.value, read_kv1.value);
    try testing.expectEqualStrings(kv2.key, read_kv2.key);
    try testing.expectEqualStrings(kv2.value, read_kv2.value);

    // Test metadata
    try testing.expectEqualStrings("key1", test_block.first_key.?);
    try testing.expectEqualStrings("key2", test_block.last_key.?);
}

test "Block error conditions" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var small_buffer: [64]u8 align(PageSize) = undefined;
    var test_block = block(&small_buffer);

    // Test index out of bounds
    try testing.expectError(Block.ReadError.IndexOutOfBounds, test_block.read(0));

    // Test buffer full
    var large_kv = try KV.initOwned(
        allocator,
        "very_long_key_that_should_not_fit",
        "very_long_value_that_should_definitely_not_fit_in_small_buffer",
    );
    defer large_kv.deinit(allocator);

    try testing.expectError(Block.WriteError.NoSpaceLeft, test_block.write(large_kv));
}

test "Block reset and reuse" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var buffer: [PageSize]u8 align(PageSize) = undefined;
    var test_block = block(&buffer);

    var kv = try KV.initOwned(allocator, "test_key", "test_value");
    defer kv.deinit(allocator);

    _ = try test_block.write(kv);
    try testing.expectEqual(@as(u64, 1), test_block.len());
    try testing.expectEqual(false, test_block.isEmpty());

    test_block.reset();
    try testing.expectEqual(@as(u64, 0), test_block.len());
    try testing.expectEqual(true, test_block.isEmpty());
    try testing.expectEqual(@as(?[]const u8, null), test_block.first_key);
    try testing.expectEqual(@as(?[]const u8, null), test_block.last_key);
}
