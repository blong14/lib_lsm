const std = @import("std");

const Allocator = std.mem.Allocator;
const ByteAlignedInt = std.math.ByteAlignedInt;
const FormatOptions = std.fmt.FormatOptions;
const Order = std.math.Order;

const assert = std.debug.assert;
const readInt = std.mem.readInt;
const writeInt = std.mem.writeInt;
const milliTimestamp = std.time.milliTimestamp;

const Endian = std.builtin.Endian.little;

pub const KV = struct {
    key: []const u8,
    value: []const u8,
    timestamp: i64,

    const Self = @This();

    pub fn init(key: []const u8, value: []const u8) Self {
        return .{
            .key = key,
            .value = value,
            .timestamp = milliTimestamp(),
        };
    }

    pub fn deinit(self: *Self) void {
        self.* = undefined;
    }

    pub fn internalKey(self: Self, alloc: Allocator) ![]const u8 {
        const internal_key_buf = try alloc.alloc(u8, std.mem.page_size);
        const internal_key = try std.fmt.bufPrint(internal_key_buf, "{s}{d}", .{ self.key, self.timestamp });
        return internal_key;
    }

    pub fn len(self: Self) u64 {
        return @sizeOf(u64) + self.key.len + @sizeOf(u64) + self.value.len + @sizeOf(i64);
    }

    pub fn order(a: []const u8, b: []const u8) Order {
        return std.mem.order(u8, a, b);
    }

    pub fn decode(self: *Self, data: []const u8) !void {
        var ptr: usize = 0;

        const key_len_sz = @sizeOf(u64);
        var key_len_bytes: [@divExact(@typeInfo(u64).Int.bits, 8)]u8 = undefined;
        @memcpy(&key_len_bytes, data[ptr..key_len_sz]);

        const key_len = readInt(u64, &key_len_bytes, Endian);
        assert(key_len > 0);

        ptr += key_len_sz;

        const key = data[ptr..][0..key_len];

        ptr += key_len;

        const value_len_sz = @sizeOf(u64);
        var value_len_bytes: [@divExact(@typeInfo(u64).Int.bits, 8)]u8 = undefined;
        @memcpy(&value_len_bytes, data[ptr..][0..value_len_sz]);

        const value_len = readInt(u64, &value_len_bytes, Endian);
        assert(value_len > 0);

        ptr += value_len_sz;

        const value = data[ptr..][0..value_len];

        ptr += value_len;

        const timestamp_sz = @sizeOf(i64);
        var timestamp_bytes: [@divExact(@typeInfo(i64).Int.bits, 8)]u8 = undefined;
        @memcpy(&timestamp_bytes, data[ptr..][0..timestamp_sz]);

        const timestamp = readInt(i64, &timestamp_bytes, Endian);
        assert(timestamp > 0);

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
        const len_ = self.len();
        assert(len_ <= buf.len);

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

        var timestamp_bytes: [@divExact(@typeInfo(i64).Int.bits, 8)]u8 = undefined;
        writeInt(ByteAlignedInt(i64), &timestamp_bytes, self.timestamp, Endian);
        @memcpy(ptr, &timestamp_bytes);

        ptr += @sizeOf(i64);

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

        try writer.print("{s}{d}\t{d}\t{d}", .{
            self.key,
            self.timestamp,
            self.value,
            self.len(),
        });
    }
};

pub fn compare(a: KV, b: KV) std.math.Order {
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

test "KV encode" {
    const alloc = std.testing.allocator;

    {
        // given
        const kv = KV.init("__key__", "__value__");

        // when
        const str = try kv.encodeAlloc(alloc);
        defer alloc.free(str);

        // then
        const key_value_part = str[0 .. str.len - 8];

        try std.testing.expectEqualSlices(
            u8,
            "\x07\x00\x00\x00\x00\x00\x00\x00__key__\x09\x00\x00\x00\x00\x00\x00\x00__value__",
            key_value_part,
        );

        const timestamp_bytes = str[str.len - 8 ..];

        var timestamp_array: [8]u8 = undefined;
        @memcpy(&timestamp_array, timestamp_bytes);

        const timestamp = readInt(i64, &timestamp_array, Endian);
        try std.testing.expect(timestamp > 0);
    }

    {
        // given
        const kv = KV.init("__key__", "__value__");

        // when
        var buf: [std.mem.page_size]u8 = undefined;
        const str = try kv.encode(&buf);

        // then
        const key_value_part = str[0 .. str.len - 8];

        try std.testing.expectEqualSlices(
            u8,
            "\x07\x00\x00\x00\x00\x00\x00\x00__key__\x09\x00\x00\x00\x00\x00\x00\x00__value__",
            key_value_part,
        );

        const timestamp_bytes = str[str.len - 8 ..];

        var timestamp_array: [8]u8 = undefined;
        @memcpy(&timestamp_array, timestamp_bytes);

        const timestamp = readInt(i64, &timestamp_array, Endian);
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

    try std.testing.expectEqual(KV.order(kv1.key, kv3.key), Order.eq);
    try std.testing.expect(!std.mem.eql(u8, kv1.value, kv3.value));
}
