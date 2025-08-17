const std = @import("std");

const Allocator = std.mem.Allocator;
const ByteAlignedInt = std.math.ByteAlignedInt;
const FormatOptions = std.fmt.FormatOptions;
const Order = std.math.Order;

const assert = std.debug.assert;
const readInt = std.mem.readInt;
const writeInt = std.mem.writeInt;
const createTimestamp = std.time.nanoTimestamp;

const Endian = std.builtin.Endian.little;

pub const KVOwnership = enum {
    // KV owns the memory and should free it
    owned,
    // KV just points to memory owned elsewhere
    borrowed,
};

pub const KV = struct {
    key: []const u8,
    value: []const u8,
    raw_bytes: []const u8,
    timestamp: i128,
    ownership: KVOwnership = .borrowed,

    const Self = @This();

    pub fn initOwned(alloc: Allocator, key: []const u8, value: []const u8) !Self {
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
            .ownership = .owned,
        };
    }

    pub fn deinit(self: *Self, alloc: Allocator) void {
        if (self.ownership == .owned) {
            alloc.free(self.raw_bytes);
        }
        self.* = undefined;
    }

    pub fn clone(self: Self, alloc: Allocator) !KV {
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
            .ownership = .owned,
        };
    }

    pub fn userKey(self: Self) []const u8 {
        return self.key;
    }

    pub fn userValue(self: Self) []const u8 {
        return self.value;
    }

    pub fn internalKey(self: Self) []const u8 {
        return self.raw_bytes[0 .. self.key.len + @sizeOf(i128)];
    }

    pub fn len(self: Self) u64 {
        return @sizeOf(u64) + self.key.len + @sizeOf(i128) + @sizeOf(u64) + self.value.len;
    }

    pub fn decode(self: *Self, data: []const u8) !void {
        // key len key timestamp value len value
        const min_size = @sizeOf(u64) + @sizeOf(i128) + @sizeOf(u64);
        if (data.len < min_size) {
            return error.InvalidData;
        }

        const max_key_size = 1024 * 1024; // 1MB
        const max_value_size = 16 * 1024 * 1024; // 16MB

        var offset: usize = 0;

        const key_len = readInt(u64, data[offset..][0..@sizeOf(u64)], Endian);
        if (key_len == 0 or key_len > max_key_size or key_len > data.len) {
            std.debug.print("kv decode key len error\n", .{});
            return error.InvalidKeyLength;
        }
        offset += @sizeOf(u64);

        if (offset + key_len > data.len) {
            return error.InvalidData;
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
        if (value_len > max_value_size) {
            return error.InvalidValueLength;
        }
        offset += @sizeOf(u64);

        if (offset + value_len > data.len) {
            return error.InvalidData;
        }
        const value = data[offset .. offset + value_len];

        self.key = key;
        self.value = value;
        self.timestamp = timestamp;
        self.ownership = .borrowed;
        self.raw_bytes = data;
    }

    pub fn encodeAlloc(self: Self, alloc: Allocator) ![]const u8 {
        const buf = try alloc.alloc(u8, self.len());
        return try self.encode(buf);
    }

    pub fn encode(self: Self, buf: []u8) ![]u8 {
        const max_key_size = 1024 * 1024; // 1MB
        if (self.key.len == 0 or self.key.len > max_key_size) {
            std.debug.print("kv encode key len error\n", .{});
            return error.InvalidKeyLength;
        }

        const max_value_size = 16 * 1024 * 1024; // 16MB
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

    pub fn format(
        self: Self,
        comptime fmt: []const u8,
        options: FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;

        try writer.print("KV{{ key=\"{s}\" ({d}B), value=\"{s}\" ({d}B), ts={d}, {} }}", .{
            self.key,
            self.key.len,
            self.value,
            self.value.len,
            self.timestamp,
            self.ownership,
        });
    }
};

pub fn compare(a: KV, b: KV) std.math.Order {
    const key_order = std.mem.order(u8, a.key, b.key);
    if (key_order == .eq) {
        return if (a.timestamp < b.timestamp) .lt else .gt;
    }

    return key_order;
}

pub fn userKeyCompare(a: KV, b: KV) std.math.Order {
    return std.mem.order(u8, a.key, b.key);
}

pub fn decode(byts: []const u8) !KV {
    var kv: KV = undefined;
    try kv.decode(byts);
    return kv;
}

pub fn encode(alloc: Allocator, value: KV) ![]const u8 {
    return value.encodeAlloc(alloc);
}

pub fn extractTimestamp(internal_key: []const u8) i128 {
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

pub fn encodeInternalKey(alloc: Allocator, key: []const u8, ts: ?i128) ![]const u8 {
    const len_ = @sizeOf(u64) + key.len + @sizeOf(i128);

    const internal_key_buf = try alloc.alloc(u8, len_);
    errdefer alloc.free(internal_key_buf);

    var offset: usize = 0;

    writeInt(u64, internal_key_buf[offset..][0..@sizeOf(u64)], key.len, Endian);
    offset += @sizeOf(u64);

    @memcpy(internal_key_buf[offset .. offset + key.len], key);
    offset += key.len;

    const timestamp = ts orelse createTimestamp();
    writeInt(i128, internal_key_buf[offset..][0..@sizeOf(i128)], timestamp, Endian);

    return internal_key_buf;
}

test "KV encode" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    {
        // given
        var kv = try KV.initOwned(alloc, "__key__", "__value__");
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
        var kv = try KV.initOwned(alloc, "__key__", "__value__");
        kv.timestamp = 1;

        // when
        var buf: [std.mem.page_size]u8 = undefined;
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

    const original = try KV.initOwned(alloc, "__key__", "__value__");

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
    try std.testing.expectEqual(KVOwnership.borrowed, decoded.ownership);
}

test "KV compare" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    // given
    const kv1 = try KV.initOwned(alloc, "key1", "value1");
    const kv2 = try KV.initOwned(alloc, "key2", "value2");
    var kv3 = try KV.initOwned(alloc, "key1", "different_value");
    kv3.timestamp = kv1.timestamp + 1;

    // then
    try std.testing.expectEqual(Order.lt, compare(kv1, kv2));
    try std.testing.expectEqual(Order.gt, compare(kv2, kv1));
    try std.testing.expectEqual(Order.lt, compare(kv1, kv3));
    try std.testing.expectEqual(Order.lt, compare(kv3, kv2));
    try std.testing.expectEqual(Order.gt, compare(kv3, kv1));

    try std.testing.expectEqual(Order.eq, userKeyCompare(kv1, kv3));
    try std.testing.expect(!std.mem.eql(u8, kv1.value, kv3.value));
}
