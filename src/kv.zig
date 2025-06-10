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

pub const KV = struct {
    key: []const u8,
    value: []const u8,
    timestamp: i128,

    const Self = @This();

    pub fn init(key: []const u8, value: []const u8) Self {
        return .{
            .key = key,
            .value = value,
            .timestamp = createTimestamp(),
        };
    }

    pub fn deinit(self: *Self, alloc: Allocator) void {
        alloc.free(self.key);
        alloc.free(self.value);
        self.* = undefined;
    }

    pub fn clone(self: Self, alloc: Allocator) !KV {
        const key_copy = try alloc.dupe(u8, self.key);
        errdefer alloc.free(key_copy);

        const value_copy = try alloc.dupe(u8, self.value);
        errdefer alloc.free(value_copy);

        return KV{
            .key = key_copy,
            .value = value_copy,
            .timestamp = self.timestamp,
        };
    }

    pub fn userKey(self: Self) []const u8 {
        return self.key;
    }

    pub fn internalKey(self: Self, alloc: Allocator) ![]const u8 {
        return encodeInternalKey(alloc, self.key, self.timestamp);
    }

    pub fn len(self: Self) u64 {
        return @sizeOf(u64) + self.key.len + @sizeOf(u64) + self.value.len + @sizeOf(i128);
    }

    pub fn order(a: []const u8, b: []const u8) Order {
        return std.mem.order(u8, a, b);
    }

    pub fn decode(self: *Self, data: []const u8) !void {
        const min_size = @sizeOf(u64) + 1 + @sizeOf(u64) + 1 + @sizeOf(i128);
        if (data.len < min_size) {
            return error.InvalidData;
        }

        var ptr: usize = 0;
        const max_key_size = 1024 * 1024; // 1MB
        const max_value_size = 16 * 1024 * 1024; // 16MB

        const key_len = readInt(u64, @ptrCast(&data[ptr]), Endian);
        if (key_len == 0 or key_len > max_key_size) {
            return error.InvalidKeyLength;
        }
        ptr += @sizeOf(u64);

        if (ptr + key_len > data.len) {
            return error.InvalidData;
        }

        const key = data[ptr .. ptr + key_len];
        ptr += key_len;

        if (ptr + @sizeOf(u64) > data.len) {
            return error.InvalidData;
        }

        const value_len = readInt(u64, @ptrCast(&data[ptr]), Endian);
        if (value_len == 0 or value_len > max_value_size) {
            return error.InvalidValueLength;
        }
        ptr += @sizeOf(u64);

        if (ptr + value_len > data.len) {
            return error.InvalidData;
        }

        const value = data[ptr .. ptr + value_len];
        ptr += value_len;

        if (ptr + @sizeOf(i128) > data.len) {
            return error.InvalidData;
        }

        const timestamp = readInt(i128, @ptrCast(&data[ptr]), Endian);
        if (timestamp <= 0) {
            return error.InvalidTimestamp;
        }

        self.*.key = key;
        self.*.value = value;
        self.*.timestamp = timestamp;
    }

    pub fn encodeAlloc(self: Self, alloc: Allocator) ![]const u8 {
        const buf = try alloc.alloc(u8, self.len());
        const data = try self.encode(buf);
        return data;
    }

    pub fn encode(self: Self, buf: []u8) ![]u8 {
        const max_key_size = 1024 * 1024; // 1MB
        if (self.key.len == 0 or self.key.len > max_key_size) {
            return error.InvalidKeyLength;
        }

        const max_value_size = 16 * 1024 * 1024; // 16MB
        if (self.value.len == 0 or self.value.len > max_value_size) {
            return error.InvalidValueLength;
        }

        if (self.timestamp <= 0) {
            return error.InvalidTimestamp;
        }

        const len_ = self.len();
        if (len_ > buf.len) {
            return error.BufferTooSmall;
        }

        var ptr = buf.ptr;

        var key_len_bytes: [@divExact(@typeInfo(u64).Int.bits, 8)]u8 = undefined;
        writeInt(ByteAlignedInt(u64), &key_len_bytes, self.key.len, Endian);
        @memcpy(ptr, &key_len_bytes);
        ptr += @sizeOf(u64);

        @memcpy(ptr, self.key);
        ptr += self.key.len;

        var value_len_bytes: [@divExact(@typeInfo(u64).Int.bits, 8)]u8 = undefined;
        writeInt(ByteAlignedInt(u64), &value_len_bytes, self.value.len, Endian);
        @memcpy(ptr, &value_len_bytes);
        ptr += @sizeOf(u64);

        @memcpy(ptr, self.value);
        ptr += self.value.len;

        var timestamp_bytes: [@divExact(@typeInfo(i128).Int.bits, 8)]u8 = undefined;
        writeInt(ByteAlignedInt(i128), &timestamp_bytes, self.timestamp, Endian);
        @memcpy(ptr, &timestamp_bytes);

        ptr += @sizeOf(i128);

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

        try writer.print("{s}\t{d}\t{d}", .{
            self.key,
            self.timestamp,
            self.len(),
        });
    }
};

pub fn compare(a: KV, b: KV) std.math.Order {
    const key_order = std.mem.order(u8, a.key, b.key);
    if (key_order == .eq) {
        return if (a.timestamp > b.timestamp) .gt else .eq;
    }

    return key_order;
}

pub fn decode(byts: []const u8) !KV {
    var kv: KV = undefined;
    try kv.decode(byts);
    return kv;
}

pub fn encode(alloc: Allocator, value: KV) ![]const u8 {
    return value.encodeAlloc(alloc);
}

pub fn encodeInternalKey(alloc: Allocator, key: []const u8, ts: ?i128) ![]const u8 {
    const len_ = @sizeOf(u64) + key.len + @sizeOf(i128);

    const internal_key_buf = try alloc.alloc(u8, len_);
    errdefer alloc.free(internal_key_buf);

    var ptr = internal_key_buf.ptr;

    var key_len_bytes: [@divExact(@typeInfo(u64).Int.bits, 8)]u8 = undefined;
    writeInt(ByteAlignedInt(u64), &key_len_bytes, key.len, Endian);
    @memcpy(ptr, &key_len_bytes);

    ptr += @sizeOf(u64);
    @memcpy(ptr, key);

    ptr += key.len;

    const timestamp = ts orelse createTimestamp();

    var timestamp_bytes: [@divExact(@typeInfo(i128).Int.bits, 8)]u8 = undefined;
    writeInt(ByteAlignedInt(i128), &timestamp_bytes, timestamp, Endian);
    @memcpy(ptr, &timestamp_bytes);

    ptr += @sizeOf(i128);

    return internal_key_buf;
}

test "KV encode" {
    const alloc = std.testing.allocator;

    {
        // given
        const kv = KV.init("__key__", "__value__");

        // when
        const str = try kv.encodeAlloc(alloc);
        defer alloc.free(str);

        // then
        const key_value_part = str[0 .. str.len - 16];

        try std.testing.expectEqualSlices(
            u8,
            "\x07\x00\x00\x00\x00\x00\x00\x00__key__\x09\x00\x00\x00\x00\x00\x00\x00__value__",
            key_value_part,
        );

        const timestamp_bytes = str[str.len - 16 ..];

        var timestamp_array: [@divExact(@typeInfo(i128).Int.bits, 8)]u8 = undefined;
        @memcpy(&timestamp_array, timestamp_bytes);

        const timestamp = readInt(i128, &timestamp_array, Endian);
        try std.testing.expect(timestamp > 0);
    }

    {
        // given
        const kv = KV.init("__key__", "__value__");

        // when
        var buf: [std.mem.page_size]u8 = undefined;
        const str = try kv.encode(&buf);

        // then
        const key_value_part = str[0 .. str.len - 16];

        try std.testing.expectEqualSlices(
            u8,
            "\x07\x00\x00\x00\x00\x00\x00\x00__key__\x09\x00\x00\x00\x00\x00\x00\x00__value__",
            key_value_part,
        );

        const timestamp_bytes = str[str.len - 16 ..];

        var timestamp_array: [@divExact(@typeInfo(i128).Int.bits, 8)]u8 = undefined;
        @memcpy(&timestamp_array, timestamp_bytes);

        const timestamp = readInt(i128, &timestamp_array, Endian);
        try std.testing.expect(timestamp > 0);
    }
}

test "KV decode" {
    // given
    const alloc = std.testing.allocator;
    const original = KV.init("__key__", "__value__");

    const encoded = try original.encodeAlloc(alloc);
    defer alloc.free(encoded);

    // when
    var decoded: KV = undefined;
    try decoded.decode(encoded);

    // then
    try std.testing.expectEqualStrings(original.key, decoded.key);
    try std.testing.expectEqualStrings(original.value, decoded.value);
    try std.testing.expectEqual(original.timestamp, decoded.timestamp);
    try std.testing.expectEqual(original.len(), decoded.len());
}

test "KV compare" {
    // given
    const kv1 = KV.init("key1", "value1");
    const kv2 = KV.init("key2", "value2");
    const kv3 = KV.init("key1", "different_value");

    // then
    try std.testing.expectEqual(Order.lt, compare(kv1, kv2));
    try std.testing.expectEqual(Order.gt, compare(kv2, kv1));
    try std.testing.expectEqual(Order.eq, compare(kv1, kv3));
    try std.testing.expectEqual(Order.lt, compare(kv3, kv2));
    try std.testing.expectEqual(Order.gt, compare(kv3, kv1));

    try std.testing.expectEqual(KV.order(kv1.key, kv3.key), Order.eq);
    try std.testing.expect(!std.mem.eql(u8, kv1.value, kv3.value));
}
