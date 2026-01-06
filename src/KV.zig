const std = @import("std");

const Allocator = std.mem.Allocator;

const readInt = std.mem.readInt;
const writeInt = std.mem.writeInt;
const createTimestamp = std.time.nanoTimestamp;

const Endian = std.builtin.Endian.little;

key: []const u8,
value: []const u8,
raw_bytes: []const u8,
timestamp: i128,

const KV = @This();

pub fn init(alloc: Allocator, key: []const u8, value: []const u8) !KV {
    var kv: KV = .{
        .key = key,
        .value = value,
        .raw_bytes = undefined,
        .timestamp = createTimestamp(),
    };

    const raw = try kv.encodeAlloc(alloc);
    errdefer alloc.free(raw);

    const key_start = @sizeOf(u64);
    const value_start = @sizeOf(u64) + key.len + @sizeOf(i128) + @sizeOf(u64);

    return .{
        .key = raw[key_start .. key_start + key.len],
        .value = raw[value_start .. value_start + value.len],
        .raw_bytes = raw,
        .timestamp = kv.timestamp,
    };
}

pub fn deinit(self: *KV, alloc: Allocator) void {
    alloc.free(self.raw_bytes);
    self.* = undefined;
}

pub fn clone(self: KV, alloc: Allocator) !KV {
    // [(keylen)(key)(ts)(valuelen)(value)]
    const raw = try self.encodeAlloc(alloc);
    errdefer alloc.free(raw);

    const key_start = @sizeOf(u64);
    const value_start = @sizeOf(u64) + self.key.len + @sizeOf(i128) + @sizeOf(u64);

    return .{
        .key = raw[key_start .. key_start + self.key.len],
        .value = raw[value_start .. value_start + self.value.len],
        .raw_bytes = raw,
        .timestamp = self.timestamp,
    };
}

pub fn userKey(self: KV) []const u8 {
    return self.key;
}

pub fn userValue(self: KV) []const u8 {
    return self.value;
}

pub fn internalKey(self: KV) []const u8 {
    return self.raw_bytes[0 .. self.key.len + @sizeOf(i128)];
}

pub fn len(self: KV) u64 {
    return @sizeOf(u64) + self.key.len + @sizeOf(i128) + @sizeOf(u64) + self.value.len;
}

pub fn decode(self: *KV, data: []const u8) !void {
    const min_size = @sizeOf(u64) + @sizeOf(i128) + @sizeOf(u64);
    if (data.len < min_size) {
        return error.InvalidData;
    }

    const max_key_size = 1024 * 1024;
    const max_value_size = 16 * 1024 * 1024;

    var offset: usize = 0;

    const key_len = readInt(u64, data[offset..][0..@sizeOf(u64)], Endian);
    offset += @sizeOf(u64);

    if (key_len == 0 or key_len > max_key_size or offset + key_len > data.len) {
        return error.InvalidKeyLength;
    }
    const key = data[offset .. offset + key_len];
    offset += key_len;

    if (offset + @sizeOf(i128) > data.len) {
        return error.InvalidData;
    }
    const timestamp = readInt(i128, data[offset..][0..@sizeOf(i128)], Endian);
    if (timestamp <= 0) {
        return error.InvalidTimestamp;
    }
    offset += @sizeOf(i128);

    if (offset + @sizeOf(u64) > data.len) {
        return error.InvalidData;
    }
    const value_len = readInt(u64, data[offset..][0..@sizeOf(u64)], Endian);
    if (value_len > max_value_size or offset + @sizeOf(u64) + value_len > data.len) {
        return error.InvalidValueLength;
    }
    offset += @sizeOf(u64);
    const value = data[offset .. offset + value_len];

    self.key = key;
    self.value = value;
    self.timestamp = timestamp;
    self.raw_bytes = data;
}

pub fn encodeAlloc(self: KV, alloc: Allocator) ![]const u8 {
    const buf = try alloc.alloc(u8, self.len());
    return try self.encode(buf);
}

pub fn encode(self: KV, buf: []u8) ![]u8 {
    const max_key_size = 1024 * 1024;
    if (self.key.len == 0 or self.key.len > max_key_size) {
        return error.InvalidKeyLength;
    }

    const max_value_size = 16 * 1024 * 1024;
    if (self.value.len > max_value_size) {
        return error.InvalidValueLength;
    }

    if (self.timestamp == 0) {
        return error.InvalidTimestamp;
    }

    const len_ = self.len();
    if (len_ > buf.len) {
        return error.BufferTooSmall;
    }

    var offset: usize = 0;

    writeInt(u64, buf[offset..][0..@sizeOf(u64)], self.key.len, Endian);
    offset += @sizeOf(u64);

    @memcpy(buf[offset .. offset + self.key.len], self.key);
    offset += self.key.len;

    writeInt(i128, buf[offset..][0..@sizeOf(i128)], self.timestamp, Endian);
    offset += @sizeOf(i128);

    writeInt(u64, buf[offset..][0..@sizeOf(u64)], self.value.len, Endian);
    offset += @sizeOf(u64);

    @memcpy(buf[offset .. offset + self.value.len], self.value);
    offset += self.value.len;

    return buf[0..len_];
}

pub fn extractTimestamp(self: KV) i128 {
    const internal_key = self.internalKey();

    const min_size = @sizeOf(u64) + 1 + @sizeOf(i128);
    if (internal_key.len < min_size) {
        return 0; // Invalid internal key format
    }

    // Read key length from the beginning
    const key_len = readInt(u64, internal_key[0..@sizeOf(u64)], Endian);

    // Validate key length bounds
    const expected_total_size = @sizeOf(u64) + key_len + @sizeOf(i128);
    if (expected_total_size > internal_key.len) {
        return 0; // Invalid key length - would overflow buffer
    }

    // Calculate timestamp position: key_len_field + key_data
    const timestamp_offset = @sizeOf(u64) + key_len;

    // Ensure we have enough bytes for the timestamp
    if (timestamp_offset + @sizeOf(i128) > internal_key.len) {
        return 0; // Not enough bytes for timestamp
    }

    // Read timestamp safely
    return readInt(i128, internal_key[timestamp_offset .. timestamp_offset + @sizeOf(i128)], Endian);
}

test "KV encode" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    {
        // given
        var kv = try KV.init(alloc, "__key__", "__value__");
        defer kv.deinit(alloc);

        kv.timestamp = 1;

        // when
        const str = try kv.encodeAlloc(alloc);
        defer alloc.free(str);

        // then
        try std.testing.expectEqualSlices(
            u8,
            "\x07\x00\x00\x00\x00\x00\x00\x00__key__\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x09\x00\x00\x00\x00\x00\x00\x00__value__",
            str,
        );
    }

    {
        // given
        var kv = try KV.init(alloc, "__key__", "__value__");
        defer kv.deinit(alloc);

        kv.timestamp = 1;

        // when
        var buf: [4096]u8 = undefined;
        const str = try kv.encode(&buf);

        // then
        try std.testing.expectEqualSlices(
            u8,
            "\x07\x00\x00\x00\x00\x00\x00\x00__key__\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x09\x00\x00\x00\x00\x00\x00\x00__value__",
            str,
        );
    }
}

test "KV decode" {
    // given
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const original = try KV.init(alloc, "__key__", "__value__");

    const encoded = try original.encodeAlloc(alloc);
    defer alloc.free(encoded);

    // when
    var decoded: KV = undefined;
    try decoded.decode(encoded);

    // then
    try std.testing.expectEqualStrings(original.key, decoded.key);
    try std.testing.expectEqualStrings(original.value, decoded.value);
    try std.testing.expectEqualSlices(u8, encoded, decoded.raw_bytes);
    try std.testing.expectEqual(original.timestamp, decoded.timestamp);
    try std.testing.expectEqual(original.len(), decoded.len());
}
