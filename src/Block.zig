const std = @import("std");

const iter = @import("iterator.zig");

const Allocator = std.mem.Allocator;
const Iterator = iter.Iterator;
const KV = @import("KV.zig");

const writeInt = std.mem.writeInt;
const readInt = std.mem.readInt;

pub const Block = @This();

const Endian = std.builtin.Endian.little;

/// Buffers containing the block data
total: []u8,
offsets: []u8,
data: []u8,

/// Current position for writing
pos: usize,

/// Total count of kv pairs
count: u64 = 0,

/// Initialize a new block with the given buffer
pub fn init(
    buffer: []u8,
    opts: struct {
        max_offset_bytes: ?u64 = null,
        max_data_bytes: ?u64 = null,
    },
) Block {
    const max_offset_bytes = opts.max_offset_bytes orelse 4096;
    const max_data_bytes = opts.max_data_bytes orelse buffer.len;
    return .{
        .total = buffer[0..@sizeOf(u64)],
        .offsets = buffer[@sizeOf(u64)..max_offset_bytes],
        .data = buffer[max_offset_bytes..max_data_bytes],
        .pos = max_offset_bytes,
    };
}

/// Read a KV pair by index
pub fn read(self: *const Block, index: usize) !KV {
    if (index >= self.count) {
        return error.IndexOutOfBounds;
    }

    const offset_size = @sizeOf(u64);
    const offset_pos = index * offset_size;

    const offset_bytes = self.offsets[offset_pos..][0..offset_size];
    const kv_offset = readInt(u64, offset_bytes, Endian);

    if (kv_offset >= self.pos) {
        return error.InvalidOffset;
    }

    var kv: KV = undefined;
    try kv.decode(self.data[kv_offset..]);

    return kv;
}

/// Read the first KV pair
pub fn first(self: *const Block) !?KV {
    if (self.count == 0) return null;

    return try self.read(0);
}

/// Read the last KV pair
pub fn last(self: *const Block) !?KV {
    if (self.count == 0) return null;

    return try self.read(self.count - 1);
}

/// Write a KV pair to the block
pub fn write(self: *Block, item: KV) !usize {
    const offset_size = @sizeOf(u64);
    if ((self.count + 1) * offset_size > self.offsets.len) {
        return error.NoOffsetSpaceLeft;
    }

    const kv_size = item.len();
    if (self.pos + kv_size > self.data.len) {
        return error.NoSpaceLeft;
    }

    @memcpy(self.data[self.pos..][0..kv_size], item.raw_bytes);

    const offset_pos = self.count * offset_size;
    writeInt(u64, self.offsets[offset_pos..][0..offset_size], self.pos, Endian);

    // Advance our position for the next write
    self.pos += kv_size;

    const index = self.count;
    self.count += 1;
    writeInt(u64, self.total[0..@sizeOf(u64)], self.count, Endian);

    return index;
}

/// Get the number of KV pairs in the block
pub fn len(self: Block) u64 {
    return self.count;
}

/// Get the current size of data in the block
pub fn size(self: Block) usize {
    return self.pos + (self.count * @sizeOf(u64));
}

const BlockIterator = struct {
    alloc: Allocator,
    block: *const Block = undefined,
    nxt: usize = 0,

    pub fn deinit(ctx: *anyopaque) void {
        const self: *BlockIterator = @ptrCast(@alignCast(ctx));
        self.alloc.destroy(self);
    }

    pub fn next(ctx: *anyopaque) ?KV {
        const self: *BlockIterator = @ptrCast(@alignCast(ctx));

        var nxt: ?KV = null;
        if (self.nxt < self.block.len()) {
            nxt = self.block.read(self.nxt) catch |err| {
                std.log.err("sstable iter error {s}", .{@errorName(err)});
                return null;
            };

            self.*.nxt += 1;
        }

        return nxt;
    }
};

pub fn iterator(self: *Block, alloc: Allocator) !Iterator(KV) {
    const it = try alloc.create(BlockIterator);
    it.* = .{ .alloc = alloc, .block = self };
    return Iterator(KV).init(it, BlockIterator.next, BlockIterator.deinit);
}

test "Block basic operations" {
    const testing = std.testing;
    const allocator = testing.allocator;
    const PageSize = std.heap.pageSize();

    var buffer: [PageSize]u8 align(PageSize) = undefined;
    var test_block: Block = .init(&buffer, .{ .max_offset_bytes = 1000 });

    var kv1 = try KV.init(allocator, "key1", "value1");
    defer kv1.deinit(allocator);

    var kv2 = try KV.init(allocator, "key2", "value2");
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

    // Test first and last functions
    const first_kv = try test_block.first();
    const last_kv = try test_block.last();

    try testing.expectEqualStrings("key1", first_kv.?.key);
    try testing.expectEqualStrings("value1", first_kv.?.value);
    try testing.expectEqualStrings("key2", last_kv.?.key);
    try testing.expectEqualStrings("value2", last_kv.?.value);
}
